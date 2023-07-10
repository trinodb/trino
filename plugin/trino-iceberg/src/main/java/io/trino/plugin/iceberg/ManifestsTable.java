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
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ManifestsTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final Table icebergTable;
    private final Optional<Long> snapshotId;

    public ManifestsTable(SchemaTableName tableName, Table icebergTable, Optional<Long> snapshotId)
    {
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");

        tableMetadata = new ConnectorTableMetadata(
                tableName,
                ImmutableList.<ColumnMetadata>builder()
                        .add(new ColumnMetadata("path", VARCHAR))
                        .add(new ColumnMetadata("length", BIGINT))
                        .add(new ColumnMetadata("partition_spec_id", INTEGER))
                        .add(new ColumnMetadata("added_snapshot_id", BIGINT))
                        .add(new ColumnMetadata("added_data_files_count", INTEGER))
                        .add(new ColumnMetadata("added_rows_count", BIGINT))
                        .add(new ColumnMetadata("existing_data_files_count", INTEGER))
                        .add(new ColumnMetadata("existing_rows_count", BIGINT))
                        .add(new ColumnMetadata("deleted_data_files_count", INTEGER))
                        .add(new ColumnMetadata("deleted_rows_count", BIGINT))
                        .add(new ColumnMetadata("partitions", new ArrayType(RowType.rowType(
                                RowType.field("contains_null", BOOLEAN),
                                RowType.field("contains_nan", BOOLEAN),
                                RowType.field("lower_bound", VARCHAR),
                                RowType.field("upper_bound", VARCHAR)))))
                        .build());
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
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
        ((ArrayBlockBuilder) arrayBlockBuilder).buildEntry(elementBuilder -> {
            for (int i = 0; i < summaries.size(); i++) {
                PartitionFieldSummary summary = summaries.get(i);
                PartitionField field = partitionSpec.fields().get(i);
                Type nestedType = partitionSpec.partitionType().fields().get(i).type();

                ((RowBlockBuilder) elementBuilder).buildEntry(fieldBuilders -> {
                    BOOLEAN.writeBoolean(fieldBuilders.get(0), summary.containsNull());
                    Boolean containsNan = summary.containsNaN();
                    if (containsNan == null) {
                        fieldBuilders.get(1).appendNull();
                    }
                    else {
                        BOOLEAN.writeBoolean(fieldBuilders.get(1), containsNan);
                    }
                    VARCHAR.writeString(fieldBuilders.get(2), field.transform().toHumanString(
                            nestedType, Conversions.fromByteBuffer(nestedType, summary.lowerBound())));
                    VARCHAR.writeString(fieldBuilders.get(3), field.transform().toHumanString(
                            nestedType, Conversions.fromByteBuffer(nestedType, summary.upperBound())));
                });
            }
        });
    }
}
