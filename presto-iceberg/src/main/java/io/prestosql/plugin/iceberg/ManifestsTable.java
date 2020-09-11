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
package io.prestosql.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.iceberg.util.PageListBuilder;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedPageSource;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.RowType;
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

import static io.prestosql.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.prestosql.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_SNAPSHOT_ID;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
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
                        .add(new ColumnMetadata("existing_data_files_count", INTEGER))
                        .add(new ColumnMetadata("deleted_data_files_count", INTEGER))
                        .add(new ColumnMetadata("partitions", new ArrayType(RowType.rowType(
                                RowType.field("contains_null", BOOLEAN),
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
        return new FixedPageSource(buildPages(tableMetadata, icebergTable, snapshotId));
    }

    private static List<Page> buildPages(ConnectorTableMetadata tableMetadata, Table icebergTable, Optional<Long> snapshotId)
    {
        PageListBuilder pagesBuilder = PageListBuilder.forTable(tableMetadata);

        Snapshot snapshot = snapshotId.map(icebergTable::snapshot)
                .orElseGet(icebergTable::currentSnapshot);
        if (snapshot == null) {
            if (snapshotId.isPresent()) {
                throw new PrestoException(ICEBERG_INVALID_SNAPSHOT_ID, "Invalid snapshot ID: " + snapshotId.get());
            }
            throw new PrestoException(ICEBERG_INVALID_METADATA, "There's no snapshot associated with table " + tableMetadata.getTable().toString());
        }
        Map<Integer, PartitionSpec> partitionSpecsById = icebergTable.specs();

        snapshot.allManifests().forEach(file -> {
            pagesBuilder.beginRow();
            pagesBuilder.appendVarchar(file.path());
            pagesBuilder.appendBigint(file.length());
            pagesBuilder.appendInteger(file.partitionSpecId());
            pagesBuilder.appendBigint(file.snapshotId());
            pagesBuilder.appendInteger(file.addedFilesCount());
            pagesBuilder.appendInteger(file.existingFilesCount());
            pagesBuilder.appendInteger(file.deletedFilesCount());
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
            VARCHAR.writeString(rowBuilder, field.transform().toHumanString(
                    Conversions.fromByteBuffer(nestedType, summary.lowerBound())));
            VARCHAR.writeString(rowBuilder, field.transform().toHumanString(
                    Conversions.fromByteBuffer(nestedType, summary.upperBound())));
            singleArrayWriter.closeEntry();
        }
        arrayBlockBuilder.closeEntry();
    }
}
