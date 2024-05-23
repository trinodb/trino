/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.deltalake.metastore;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.deltalake.DeltaLakeColumnMetadata;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.TableParameterLengthLimit;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.storeTableMetadata;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode.NONE;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getColumnMetadata;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.BinaryOperator.maxBy;

public class DeltaLakeTableMetadataScheduler
{
    private static final Logger log = Logger.get(DeltaLakeTableMetadataScheduler.class);

    private static final String TRINO_LAST_TRANSACTION_VERSION = "trino_last_transaction_version";
    private static final String TRINO_METADATA_SCHEMA_STRING = "trino_metadata_schema_string";

    private final HiveMetastoreFactory hiveMetastoreFactory;
    private final TypeManager typeManager;
    private final int tableParameterLengthLimit;
    private final int storeTableMetadataThreads;
    private final Duration storeTableMetadataInterval;
    private final Map<SchemaTableName, UpdateInfo> updateInfos = new ConcurrentHashMap<>();
    private final boolean enabled;

    private ExecutorService executor;
    private ScheduledExecutorService scheduler;

    @Inject
    public DeltaLakeTableMetadataScheduler(
            HiveMetastoreFactory hiveMetastoreFactory,
            NodeManager nodeManager,
            TypeManager typeManager,
            @TableParameterLengthLimit int tableParameterLengthLimit,
            DeltaLakeConfig config)
    {
        this.hiveMetastoreFactory = requireNonNull(hiveMetastoreFactory, "hiveMetastoreFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableParameterLengthLimit = tableParameterLengthLimit;
        this.storeTableMetadataThreads = config.getStoreTableMetadataThreads();
        this.storeTableMetadataInterval = config.getStoreTableMetadataInterval();
        requireNonNull(nodeManager, "nodeManager is null");
        this.enabled = config.isStoreTableMetadataEnabled() && nodeManager.getCurrentNode().isCoordinator();
    }

    public void putAll(Map<SchemaTableName, UpdateInfo> tableParameters)
    {
        updateInfos.putAll(tableParameters);
    }

    @PostConstruct
    public void start()
    {
        if (enabled) {
            executor = storeTableMetadataThreads == 0 ? newDirectExecutorService() : newFixedThreadPool(storeTableMetadataThreads, threadsNamed("store-table-metadata-%s"));
            scheduler = newSingleThreadScheduledExecutor(daemonThreadsNamed("store-table-metadata"));
            scheduler.scheduleWithFixedDelay(this::process, 0, storeTableMetadataInterval.toMillis(), MILLISECONDS);
        }
    }

    @VisibleForTesting
    public void process()
    {
        Map<SchemaTableName, UpdateInfo> updateTables;
        synchronized (this) {
            updateTables = updateInfos.entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue, maxBy(comparing(UpdateInfo::version))));
            updateInfos.clear();
        }

        log.debug("Processing %s table(s): %s", updateTables.size(), updateTables.keySet());
        for (Map.Entry<SchemaTableName, UpdateInfo> entry : updateTables.entrySet()) {
            executor.execute(() -> updateTable(entry.getKey(), entry.getValue()));
        }
    }

    private void updateTable(SchemaTableName schemaTableName, UpdateInfo info)
    {
        log.debug("Updating table: '%s'", schemaTableName);
        HiveMetastore metastore = hiveMetastoreFactory.createMetastore(Optional.of(info.identity));
        try {
            metastore.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName())
                    .ifPresent(table -> {
                        Table updatedTable = setTableMetadata(table, info.version, info.schemaString, info.tableComment);
                        metastore.replaceTable(table.getDatabaseName(), table.getTableName(), updatedTable, buildInitialPrivilegeSet(table.getOwner().orElseThrow()));
                        log.debug("Replaced table: '%s'", schemaTableName);
                    });
        }
        catch (Exception e) {
            log.warn(e, "Failed to store table metadata for '%s'", schemaTableName);
        }
    }

    @VisibleForTesting
    public void clear()
    {
        updateInfos.clear();
    }

    @PreDestroy
    public void stop()
    {
        if (enabled) {
            scheduler.shutdownNow();
            executor.shutdownNow();
        }
    }

    public boolean isSameTransactionVersion(Table table, TableSnapshot snapshot)
    {
        if (!containsLastTransactionVersion(table)) {
            return false;
        }
        return getLastTransactionVersion(table) == snapshot.getVersion();
    }

    public boolean containsLastTransactionVersion(Table table)
    {
        return table.getParameters().containsKey(TRINO_LAST_TRANSACTION_VERSION);
    }

    public long getLastTransactionVersion(Table table)
    {
        return Long.parseLong(table.getParameters().get(TRINO_LAST_TRANSACTION_VERSION));
    }

    public boolean containsSchemaString(Table table)
    {
        return table.getParameters().containsKey(TRINO_METADATA_SCHEMA_STRING);
    }

    public List<ColumnMetadata> getColumnsMetadata(Table table)
    {
        String schemaString = table.getParameters().get(TRINO_METADATA_SCHEMA_STRING);
        // Specify NONE because physical names are unused when listing columns
        return getColumnMetadata(schemaString, typeManager, NONE).stream()
                .map(DeltaLakeColumnMetadata::columnMetadata)
                .collect(toImmutableList());
    }

    public boolean canStoreTableMetadata(ConnectorSession session, String schemaString, Optional<String> tableComment)
    {
        return storeTableMetadata(session) &&
                schemaString.length() <= tableParameterLengthLimit &&
                tableComment.orElse("").length() <= tableParameterLengthLimit;
    }

    private static Table setTableMetadata(Table table, long version, String schemaString, Optional<String> tableComment)
    {
        ImmutableMap.Builder<String, String> parameters = ImmutableMap.builder();
        parameters.putAll(table.getParameters());
        setTableMetadata(parameters, version, schemaString, tableComment);
        return Table.builder(table)
                .setParameters(parameters.buildKeepingLast())
                .build();
    }

    public static void setTableMetadata(
            ImmutableMap.Builder<String, String> builder,
            long version,
            String schemaString,
            Optional<String> tableComment)
    {
        tableComment.ifPresent(comment -> builder.put(TABLE_COMMENT, comment));
        builder.put(TRINO_LAST_TRANSACTION_VERSION, Long.toString(version));
        builder.put(TRINO_METADATA_SCHEMA_STRING, schemaString);
    }

    public record UpdateInfo(ConnectorIdentity identity, long version, String schemaString, Optional<String> tableComment)
    {
        public UpdateInfo
        {
            requireNonNull(identity, "identity is null");
            requireNonNull(schemaString, "schemaString is null");
            requireNonNull(tableComment, "tableComment is null");
        }
    }
}
