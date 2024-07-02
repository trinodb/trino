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
import io.airlift.concurrent.MoreFutures;
import io.airlift.log.Logger;
import io.trino.plugin.deltalake.DeltaLakeColumnMetadata;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.TableParameterLengthLimit;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.storeTableMetadata;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode.NONE;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getColumnMetadata;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
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
    private static final int MAX_FAILED_COUNTS = 10;

    private final DeltaLakeTableOperationsProvider tableOperationsProvider;
    private final TypeManager typeManager;
    private final int tableParameterLengthLimit;
    private final int storeTableMetadataThreads;
    private final Map<SchemaTableName, UpdateInfo> updateInfos = new ConcurrentHashMap<>();
    private final boolean enabled;

    private ExecutorService executor;
    private ScheduledExecutorService scheduler;
    private final AtomicInteger failedCounts = new AtomicInteger();

    @Inject
    public DeltaLakeTableMetadataScheduler(
            DeltaLakeTableOperationsProvider tableOperationsProvider,
            NodeManager nodeManager,
            TypeManager typeManager,
            @TableParameterLengthLimit int tableParameterLengthLimit,
            DeltaLakeConfig config)
    {
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableParameterLengthLimit is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableParameterLengthLimit = tableParameterLengthLimit;
        this.storeTableMetadataThreads = config.getStoreTableMetadataThreads();
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

            scheduler.scheduleWithFixedDelay(() -> {
                try {
                    process();
                }
                catch (Throwable e) {
                    log.warn(e, "Error storing table metadata");
                }
                try {
                    checkFailedTasks();
                }
                catch (Throwable e) {
                    log.warn(e, "Error canceling metadata update tasks");
                }
            }, 200, 1000, MILLISECONDS);
        }
    }

    @VisibleForTesting
    public void process()
    {
        List<Callable<Void>> tasks = new ArrayList<>();
        synchronized (this) {
            Map<SchemaTableName, UpdateInfo> updateTables = updateInfos.entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue, maxBy(comparing(UpdateInfo::version))));

            log.debug("Processing %s table(s): %s", updateTables.size(), updateTables.keySet());
            for (Map.Entry<SchemaTableName, UpdateInfo> entry : updateTables.entrySet()) {
                tasks.add(() -> {
                    updateTable(entry.getKey(), entry.getValue());
                    return null;
                });
            }

            this.updateInfos.clear();
        }

        try {
            executor.invokeAll(tasks).forEach(MoreFutures::getDone);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void updateTable(SchemaTableName schemaTableName, UpdateInfo info)
    {
        log.debug("Updating table: '%s'", schemaTableName);
        try {
            tableOperationsProvider.createTableOperations(info.session, schemaTableName)
                    .commitToExistingTable(info.version, info.schemaString, info.tableComment);
            log.debug("Replaced table: '%s'", schemaTableName);
        }
        catch (TableNotFoundException e) {
            // Don't increment failedCounts. The table might have been dropped concurrently.
            log.debug("Table disappeared during metadata updating operation: '%s'", schemaTableName);
        }
        catch (Exception e) {
            log.warn(e, "Failed to store table metadata for '%s'", schemaTableName);
            // TODO Consider increment only when the exception is permission issue
            failedCounts.incrementAndGet();
        }
    }

    private void checkFailedTasks()
    {
        if (failedCounts.get() > MAX_FAILED_COUNTS) {
            log.warn("Too many failed tasks, stopping the scheduler");
            stop();
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

    public static boolean isSameTransactionVersion(Table table, TableSnapshot snapshot)
    {
        return getLastTransactionVersion(table)
                .map(version -> version == snapshot.getVersion())
                .orElse(false);
    }

    public static Optional<Long> getLastTransactionVersion(Table table)
    {
        String version = table.getParameters().get(TRINO_LAST_TRANSACTION_VERSION);
        return version == null ? Optional.empty() : Optional.of(Long.parseLong(version));
    }

    public static boolean containsSchemaString(Table table)
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

    public static Map<String, String> deltaParameters(long version, String schemaString, Optional<String> tableComment)
    {
        ImmutableMap.Builder<String, String> parameters = ImmutableMap.builder();
        tableComment.ifPresent(comment -> parameters.put(TABLE_COMMENT, comment));
        parameters.put(TRINO_LAST_TRANSACTION_VERSION, Long.toString(version));
        parameters.put(TRINO_METADATA_SCHEMA_STRING, schemaString);
        return parameters.buildOrThrow();
    }

    public record UpdateInfo(ConnectorSession session, long version, String schemaString, Optional<String> tableComment)
    {
        public UpdateInfo
        {
            requireNonNull(session, "session is null");
            requireNonNull(schemaString, "schemaString is null");
            requireNonNull(tableComment, "tableComment is null");
        }
    }
}
