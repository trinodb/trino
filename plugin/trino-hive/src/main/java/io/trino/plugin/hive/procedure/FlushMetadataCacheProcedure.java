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
package io.trino.plugin.hive.procedure;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Table;
import io.trino.plugin.hive.HiveErrorCode;
import io.trino.plugin.hive.fs.DirectoryLister;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.metastore.glue.GlueCache;
import io.trino.plugin.hive.metastore.glue.PartitionName;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.ArrayType;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.util.HiveUtil.makePartName;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class FlushMetadataCacheProcedure
        implements Provider<Procedure>
{
    private static final String PROCEDURE_NAME = "flush_metadata_cache";

    private static final String PARAM_SCHEMA_NAME = "SCHEMA_NAME";
    private static final String PARAM_TABLE_NAME = "TABLE_NAME";
    private static final String PARAM_PARTITION_COLUMNS = "PARTITION_COLUMNS";
    private static final String PARAM_PARTITION_VALUES = "PARTITION_VALUES";

    private static final String PROCEDURE_USAGE_EXAMPLES = format(
            "Valid usages:%n" +
                    " - '%1$s()'%n" +
                    " - %1$s(%2$s => ..., %3$s => ...)" +
                    " - %1$s(%2$s => ..., %3$s => ..., %4$s => ARRAY['...'], %5$s => ARRAY['...'])",
            PROCEDURE_NAME,
            // Use lowercase parameter names per convention. In the usage example the names are not delimited.
            PARAM_SCHEMA_NAME.toLowerCase(ENGLISH),
            PARAM_TABLE_NAME.toLowerCase(ENGLISH),
            PARAM_PARTITION_COLUMNS.toLowerCase(ENGLISH),
            PARAM_PARTITION_VALUES.toLowerCase(ENGLISH));

    private static final MethodHandle FLUSH_HIVE_METASTORE_CACHE;

    static {
        try {
            FLUSH_HIVE_METASTORE_CACHE = lookup().unreflect(FlushMetadataCacheProcedure.class.getMethod(
                    "flushMetadataCache", ConnectorSession.class, String.class, String.class, List.class, List.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final HiveMetastoreFactory hiveMetadataFactory;
    private final Optional<DirectoryLister> directoryLister;
    private final Optional<CachingHiveMetastore> cachingHiveMetastore;
    private final Optional<GlueCache> glueCache;

    @Inject
    public FlushMetadataCacheProcedure(
            HiveMetastoreFactory hiveMetadataFactory,
            Optional<DirectoryLister> directoryLister,
            Optional<CachingHiveMetastore> cachingHiveMetastore,
            Optional<GlueCache> glueCache)
    {
        this.hiveMetadataFactory = requireNonNull(hiveMetadataFactory, "hiveMetadataFactory is null");
        this.directoryLister = requireNonNull(directoryLister, "directoryLister is null");
        this.cachingHiveMetastore = requireNonNull(cachingHiveMetastore, "cachingHiveMetastore is null");
        this.glueCache = requireNonNull(glueCache, "glueCache is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                PROCEDURE_NAME,
                ImmutableList.of(
                        new Procedure.Argument(PARAM_SCHEMA_NAME, VARCHAR, false, null),
                        new Procedure.Argument(PARAM_TABLE_NAME, VARCHAR, false, null),
                        new Procedure.Argument(PARAM_PARTITION_COLUMNS, new ArrayType(VARCHAR), false, null),
                        new Procedure.Argument(PARAM_PARTITION_VALUES, new ArrayType(VARCHAR), false, null)),
                FLUSH_HIVE_METASTORE_CACHE.bindTo(this),
                true);
    }

    public void flushMetadataCache(
            ConnectorSession session,
            String schemaName,
            String tableName,
            List<String> partitionColumns,
            List<String> partitionValues)
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doFlushMetadataCache(
                    session,
                    Optional.ofNullable(schemaName),
                    Optional.ofNullable(tableName),
                    Optional.ofNullable(partitionColumns).orElse(ImmutableList.of()),
                    Optional.ofNullable(partitionValues).orElse(ImmutableList.of()));
        }
    }

    private void doFlushMetadataCache(ConnectorSession session, Optional<String> schemaName, Optional<String> tableName, List<String> partitionColumns, List<String> partitionValues)
    {
        if (cachingHiveMetastore.isEmpty() && glueCache.isEmpty()) {
            // TODO this currently does not work. CachingHiveMetastore is always bound for metastores other than Glue, even when caching is disabled,
            //  so for consistency we do not discern between GlueCache NOOP and real.
            throw new TrinoException(HiveErrorCode.HIVE_METASTORE_ERROR, "Cannot flush, metastore cache is not enabled");
        }

        checkState(
                partitionColumns.size() == partitionValues.size(),
                "Parameters partition_column and partition_value should have same length");

        if (schemaName.isEmpty() && tableName.isEmpty() && partitionColumns.isEmpty()) {
            cachingHiveMetastore.ifPresent(CachingHiveMetastore::flushCache);
            glueCache.ifPresent(GlueCache::flushCache);
            directoryLister.ifPresent(DirectoryLister::invalidateAll);
        }
        else if (schemaName.isPresent() && tableName.isPresent()) {
            HiveMetastore metastore = hiveMetadataFactory.createMetastore(Optional.of(session.getIdentity()));
            Table table = metastore.getTable(schemaName.get(), tableName.get())
                    .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(schemaName.get(), tableName.get())));
            List<String> partitions;

            if (!partitionColumns.isEmpty()) {
                cachingHiveMetastore.ifPresent(cachingHiveMetastore -> cachingHiveMetastore.flushPartitionCache(schemaName.get(), tableName.get(), partitionColumns, partitionValues));
                glueCache.ifPresent(glueCache -> glueCache.invalidatePartition(schemaName.get(), tableName.get(), new PartitionName(partitionValues)));

                partitions = ImmutableList.of(makePartName(partitionColumns, partitionValues));
            }
            else {
                cachingHiveMetastore.ifPresent(cachingHiveMetastore -> cachingHiveMetastore.invalidateTable(schemaName.get(), tableName.get()));
                glueCache.ifPresent(glueCache -> glueCache.invalidateTable(schemaName.get(), tableName.get(), true));

                List<String> partitionColumnNames = table.getPartitionColumns().stream()
                        .map(Column::getName)
                        .collect(toImmutableList());
                partitions = metastore.getPartitionNamesByFilter(schemaName.get(), tableName.get(), partitionColumnNames, TupleDomain.all())
                        .orElse(ImmutableList.of());
            }

            if (directoryLister.isPresent()) {
                if (partitions.isEmpty()) {
                    directoryLister.get().invalidate(table);
                }
                else {
                    metastore.getPartitionsByNames(table, partitions).values().stream()
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .forEach(partition -> directoryLister.get().invalidate(partition));
                }
            }
        }
        else {
            throw new TrinoException(StandardErrorCode.INVALID_PROCEDURE_ARGUMENT, "Illegal parameter set passed. " + PROCEDURE_USAGE_EXAMPLES);
        }
    }
}
