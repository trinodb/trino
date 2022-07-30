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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.iceberg.util.PageListBuilder;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import org.apache.iceberg.ManifestFile.PartitionFieldSummary;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.iceberg.ColumnIdentity.createColumnIdentity;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergUtil.createColumnHandle;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ManifestsTable
        implements IcebergSystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final Table icebergTable;
    private final Map<String, ColumnHandle> columnHandles;
    private final Optional<Long> snapshotId;

    public ManifestsTable(SchemaTableName tableName, Table icebergTable, Optional<Long> snapshotId)
    {
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
        List<IcebergColumnHandle> columnHandlesList = ImmutableList.<IcebergColumnHandle>builder()
                .add(createColumnHandle(createColumnIdentity(Types.NestedField.required(1, "path", Types.StringType.get())), VARCHAR))
                .add(createColumnHandle(createColumnIdentity(Types.NestedField.required(2, "length", Types.LongType.get())), BIGINT))
                .add(createColumnHandle(createColumnIdentity(Types.NestedField.required(3, "partition_spec_id", Types.IntegerType.get())), INTEGER))
                .add(createColumnHandle(createColumnIdentity(Types.NestedField.required(4, "added_snapshot_id", Types.LongType.get())), BIGINT))
                .add(createColumnHandle(createColumnIdentity(Types.NestedField.required(5, "added_data_files_count", Types.IntegerType.get())), INTEGER))
                .add(createColumnHandle(createColumnIdentity(Types.NestedField.required(6, "added_rows_count", Types.LongType.get())), BIGINT))
                .add(createColumnHandle(createColumnIdentity(Types.NestedField.required(7, "existing_data_files_count", Types.IntegerType.get())), INTEGER))
                .add(createColumnHandle(createColumnIdentity(Types.NestedField.required(8, "existing_rows_count", Types.LongType.get())), BIGINT))
                .add(createColumnHandle(createColumnIdentity(Types.NestedField.required(9, "deleted_data_files_count", Types.IntegerType.get())), INTEGER))
                .add(createColumnHandle(createColumnIdentity(Types.NestedField.required(10, "deleted_rows_count", Types.LongType.get())), BIGINT))
                .add(createColumnHandle(
                        createColumnIdentity(Types.NestedField.required(11, "partitions", Types.ListType.ofRequired(9, Types.StructType.of(
                                Types.NestedField.required(12, "contains_null", Types.BooleanType.get()),
                                Types.NestedField.optional(13, "contains_nan", Types.BooleanType.get()),
                                Types.NestedField.optional(14, "lower_bound", Types.StringType.get()),
                                Types.NestedField.optional(15, "upper_bound", Types.StringType.get()))))),
                        new ArrayType(RowType.rowType(
                                RowType.field("contains_null", BOOLEAN),
                                RowType.field("contains_nan", BOOLEAN),
                                RowType.field("lower_bound", VARCHAR),
                                RowType.field("upper_bound", VARCHAR)))))
                .build();
        columnHandles = columnHandlesList.stream()
                .collect(toImmutableMap(IcebergColumnHandle::getName, Function.identity()));
        tableMetadata = new ConnectorTableMetadata(
                tableName,
                columnHandlesList.stream()
                        .map(icebergColumnHandle -> new ColumnMetadata(icebergColumnHandle.getName(), icebergColumnHandle.getType()))
                        .collect(Collectors.toUnmodifiableList()));
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        if (snapshotId.isEmpty()) {
            return new FixedPageSource(ImmutableList.of());
        }
        return new FixedPageSource(buildPages(tableMetadata, icebergTable, snapshotId.get()));
    }

    private static List<Page> buildPages(ConnectorTableMetadata tableMetadata, Table icebergTable, long snapshotId)
    {
        PageListBuilder pagesBuilder = PageListBuilder.forTable(tableMetadata);

        Snapshot snapshot = icebergTable.snapshot(snapshotId);
        if (snapshot == null) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, format("Snapshot ID [%s] does not exist for table: %s", snapshotId, icebergTable));
        }

        Map<Integer, PartitionSpec> partitionSpecsById = icebergTable.specs();

        snapshot.allManifests(icebergTable.io()).forEach(file -> {
            pagesBuilder.beginRow();
            pagesBuilder.appendVarchar(file.path());
            pagesBuilder.appendBigint(file.length());
            pagesBuilder.appendInteger(file.partitionSpecId());
            pagesBuilder.appendBigint(file.snapshotId());
            pagesBuilder.appendInteger(file.addedFilesCount());
            pagesBuilder.appendBigint(file.addedRowsCount());
            pagesBuilder.appendInteger(file.existingFilesCount());
            pagesBuilder.appendBigint(file.existingRowsCount());
            pagesBuilder.appendInteger(file.deletedFilesCount());
            pagesBuilder.appendBigint(file.deletedRowsCount());
            writePartitionSummaries(pagesBuilder.nextColumn(), file.partitions(), partitionSpecsById.get(file.partitionSpecId()));
            pagesBuilder.endRow();
        });

        return pagesBuilder.build();
    }

    private static void writePartitionSummaries(BlockBuilder arrayBlockBuilder, List<PartitionFieldSummary> summaries, PartitionSpec partitionSpec)
    {
        BlockBuilder singleArrayWriter = arrayBlockBuilder.beginBlockEntry();
        for (int i = 0; i < summaries.size(); i++) {
            PartitionFieldSummary summary = summaries.get(i);
            PartitionField field = partitionSpec.fields().get(i);
            Type nestedType = partitionSpec.partitionType().fields().get(i).type();

            BlockBuilder rowBuilder = singleArrayWriter.beginBlockEntry();
            BOOLEAN.writeBoolean(rowBuilder, summary.containsNull());
            Boolean containsNan = summary.containsNaN();
            if (containsNan == null) {
                rowBuilder.appendNull();
            }
            else {
                BOOLEAN.writeBoolean(rowBuilder, containsNan);
            }
            VARCHAR.writeString(rowBuilder, field.transform().toHumanString(
                    Conversions.fromByteBuffer(nestedType, summary.lowerBound())));
            VARCHAR.writeString(rowBuilder, field.transform().toHumanString(
                    Conversions.fromByteBuffer(nestedType, summary.upperBound())));
            singleArrayWriter.closeEntry();
        }
        arrayBlockBuilder.closeEntry();
    }
}
