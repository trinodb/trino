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
package io.trino.plugin.hive;

import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Streams.stream;
import static io.trino.plugin.hive.HiveSessionProperties.getTimestampPrecision;
import static io.trino.plugin.hive.SystemTableHandler.PARTITIONS;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getProtectMode;
import static io.trino.plugin.hive.metastore.MetastoreUtil.verifyOnline;
import static io.trino.plugin.hive.util.HiveBucketing.getHiveBucketHandle;
import static io.trino.plugin.hive.util.HiveUtil.getPartitionKeyColumnHandles;
import static io.trino.plugin.hive.util.HiveUtil.getRegularColumnHandles;
import static io.trino.plugin.hive.util.HiveUtil.isDeltaLakeTable;
import static io.trino.plugin.hive.util.HiveUtil.isIcebergTable;
import static io.trino.plugin.hive.util.SystemTables.createSystemTable;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;

public class PartitionsSystemTableProvider
        implements SystemTableProvider
{
    private final HivePartitionManager partitionManager;
    private final TypeManager typeManager;

    @Inject
    public PartitionsSystemTableProvider(HivePartitionManager partitionManager, TypeManager typeManager)
    {
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public Optional<SchemaTableName> getSourceTableName(SchemaTableName tableName)
    {
        if (!PARTITIONS.matches(tableName)) {
            return Optional.empty();
        }

        return Optional.of(PARTITIONS.getSourceTableName(tableName));
    }

    @Override
    public Optional<SystemTable> getSystemTable(HiveMetadata metadata, ConnectorSession session, SchemaTableName tableName)
    {
        if (!PARTITIONS.matches(tableName)) {
            return Optional.empty();
        }

        SchemaTableName sourceTableName = PARTITIONS.getSourceTableName(tableName);
        Table sourceTable = metadata.getMetastore()
                .getTable(new HiveIdentity(session), sourceTableName.getSchemaName(), sourceTableName.getTableName())
                .orElse(null);
        if (sourceTable == null || isDeltaLakeTable(sourceTable) || isIcebergTable(sourceTable)) {
            return Optional.empty();
        }
        verifyOnline(sourceTableName, Optional.empty(), getProtectMode(sourceTable), sourceTable.getParameters());
        HiveTableHandle sourceTableHandle = new HiveTableHandle(
                sourceTableName.getSchemaName(),
                sourceTableName.getTableName(),
                sourceTable.getParameters(),
                getPartitionKeyColumnHandles(sourceTable, typeManager),
                getRegularColumnHandles(sourceTable, typeManager, getTimestampPrecision(session)),
                getHiveBucketHandle(session, sourceTable, typeManager));

        List<HiveColumnHandle> partitionColumns = sourceTableHandle.getPartitionColumns();
        if (partitionColumns.isEmpty()) {
            return Optional.empty();
        }

        List<Type> partitionColumnTypes = partitionColumns.stream()
                .map(HiveColumnHandle::getType)
                .collect(toImmutableList());

        List<ColumnMetadata> partitionSystemTableColumns = partitionColumns.stream()
                .map(column -> ColumnMetadata.builder()
                        .setName(column.getName())
                        .setType(column.getType())
                        .setComment(column.getComment())
                        .setHidden(column.isHidden())
                        .build())
                .collect(toImmutableList());

        Map<Integer, HiveColumnHandle> fieldIdToColumnHandle =
                IntStream.range(0, partitionColumns.size())
                        .boxed()
                        .collect(toImmutableMap(identity(), partitionColumns::get));

        return Optional.of(createSystemTable(
                new ConnectorTableMetadata(tableName, partitionSystemTableColumns),
                constraint -> {
                    Constraint targetConstraint = new Constraint(constraint.transformKeys(fieldIdToColumnHandle::get));
                    Iterable<List<Object>> records = () ->
                            stream(partitionManager.getPartitions(metadata.getMetastore(), new HiveIdentity(session), sourceTableHandle, targetConstraint).getPartitions())
                                    .map(hivePartition ->
                                            IntStream.range(0, partitionColumns.size())
                                                    .mapToObj(fieldIdToColumnHandle::get)
                                                    .map(columnHandle -> hivePartition.getKeys().get(columnHandle).getValue())
                                                    .collect(toList())) // nullable
                                    .iterator();

                    return new InMemoryRecordSet(partitionColumnTypes, records).cursor();
                }));
    }
}
