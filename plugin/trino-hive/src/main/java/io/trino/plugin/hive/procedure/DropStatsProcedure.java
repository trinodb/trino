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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.PartitionStatistics;
import io.trino.plugin.base.util.UncheckedCloseable;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.TransactionalMetadata;
import io.trino.plugin.hive.TransactionalMetadataFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.procedure.Procedure.Argument;
import io.trino.spi.type.ArrayType;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metastore.Partitions.makePartName;
import static io.trino.metastore.StatisticsUpdateMode.CLEAR_ALL;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

/**
 * A procedure that drops statistics.  It can be invoked for a subset of partitions (e.g.
 * {@code CALL system.drop_stats('system', 'some_table', ARRAY[ARRAY['x', '7']])}) or
 * for the entire table ({@code CALL system.drop_stats('system', 'some_table')})).
 */
public class DropStatsProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle DROP_STATS;

    static {
        try {
            DROP_STATS = lookup().unreflect(DropStatsProcedure.class.getMethod("dropStats", ConnectorSession.class, ConnectorAccessControl.class, String.class, String.class, List.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final TransactionalMetadataFactory hiveMetadataFactory;

    @Inject
    public DropStatsProcedure(TransactionalMetadataFactory hiveMetadataFactory)
    {
        this.hiveMetadataFactory = requireNonNull(hiveMetadataFactory, "hiveMetadataFactory is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "drop_stats",
                ImmutableList.of(
                        new Argument("SCHEMA_NAME", VARCHAR),
                        new Argument("TABLE_NAME", VARCHAR),
                        new Argument("PARTITION_VALUES", new ArrayType(new ArrayType(VARCHAR)), false, null)),
                DROP_STATS.bindTo(this));
    }

    public void dropStats(ConnectorSession session, ConnectorAccessControl accessControl, String schema, String table, List<?> partitionValues)
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doDropStats(session, accessControl, schema, table, partitionValues);
        }
    }

    private void doDropStats(ConnectorSession session, ConnectorAccessControl accessControl, String schema, String table, List<?> partitionValues)
    {
        checkProcedureArgument(schema != null, "schema_name cannot be null");
        checkProcedureArgument(table != null, "table_name cannot be null");

        TransactionalMetadata hiveMetadata = hiveMetadataFactory.create(session.getIdentity(), true);
        hiveMetadata.beginQuery(session);
        try (UncheckedCloseable ignore = () -> hiveMetadata.cleanupQuery(session)) {
            SchemaTableName schemaTableName = new SchemaTableName(schema, table);
            ConnectorTableHandle handle = hiveMetadata.getTableHandle(session, schemaTableName, Optional.empty(), Optional.empty());
            if (handle == null) {
                throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, format("Table '%s' does not exist", schemaTableName));
            }

            accessControl.checkCanInsertIntoTable(null, schemaTableName);

            Map<String, ColumnHandle> columns = hiveMetadata.getColumnHandles(session, handle);
            List<String> partitionColumns = columns.values().stream()
                    .map(HiveColumnHandle.class::cast)
                    .filter(HiveColumnHandle::isPartitionKey)
                    .map(HiveColumnHandle::getName)
                    .collect(toImmutableList());

            HiveMetastore metastore = hiveMetadata.getMetastore().unsafeGetRawHiveMetastore();
            if (partitionValues != null) {
                // drop stats for specified partitions
                List<List<String>> partitionStringValues = partitionValues.stream()
                        .map(DropStatsProcedure::validateParameterType)
                        .collect(toImmutableList());
                validatePartitions(partitionStringValues, partitionColumns);

                partitionStringValues.forEach(values -> metastore.updatePartitionStatistics(
                        metastore.getTable(schema, table)
                                .orElseThrow(() -> new TableNotFoundException(schemaTableName)),
                        CLEAR_ALL,
                        ImmutableMap.of(
                                makePartName(partitionColumns, values),
                                PartitionStatistics.empty())));
            }
            else {
                // no partition specified, so drop stats for the entire table
                if (partitionColumns.isEmpty()) {
                    // for non-partitioned tables, just wipe table stats
                    metastore.updateTableStatistics(
                            schema,
                            table,
                            OptionalLong.empty(),
                            CLEAR_ALL,
                            PartitionStatistics.empty());
                }
                else {
                    // the table is partitioned; remove stats for every partition
                    hiveMetadata.getMetastore().getPartitionNamesByFilter(schemaTableName.getSchemaName(), schemaTableName.getTableName(), partitionColumns, TupleDomain.all())
                            .ifPresent(partitions -> partitions.forEach(partitionName -> metastore.updatePartitionStatistics(
                                    metastore.getTable(schema, table)
                                            .orElseThrow(() -> new TableNotFoundException(schemaTableName)),
                                    CLEAR_ALL,
                                    ImmutableMap.of(
                                            partitionName,
                                            PartitionStatistics.empty()))));
                }
            }

            hiveMetadata.commit();
        }
    }

    private static List<String> validateParameterType(Object param)
    {
        if (param == null) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Null partition value");
        }

        if (param instanceof List) {
            return ((List<?>) param)
                    .stream()
                    .map(String.class::cast)
                    .collect(toImmutableList());
        }

        throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Partition value must be an array");
    }

    private static void validatePartitions(List<List<String>> partitionValues, List<String> partitionColumns)
    {
        if (partitionValues.isEmpty()) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "No partitions provided");
        }

        if (partitionColumns.isEmpty()) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Cannot specify partition values for an unpartitioned table");
        }

        partitionValues.forEach(value -> {
            if (value.size() != partitionColumns.size()) {
                throw new TrinoException(
                        INVALID_PROCEDURE_ARGUMENT,
                        format("Partition values %s don't match the number of partition columns (%s)", value, partitionColumns.size()));
            }
        });
    }
}
