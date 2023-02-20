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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFile.PartitionFieldSummary;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.rowType;
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
                        .add(new ColumnMetadata("partitions", new ArrayType(rowType(
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
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        List<io.trino.spi.type.Type> types = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());
        if (snapshotId.isEmpty()) {
            return InMemoryRecordSet.builder(types).build().cursor();
        }
        Snapshot snapshot = icebergTable.snapshot(snapshotId.get());
        if (snapshot == null) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, format("Snapshot ID [%s] does not exist for table: %s", snapshotId, icebergTable));
        }

        ManifestFilesIterable manifestFilesIterable = new ManifestFilesIterable(snapshot.allManifests(icebergTable.io()), types);
        return manifestFilesIterable.cursor();
    }

    private class ManifestFilesIterable
            implements Iterable<List<Object>>
    {
        private final List<ManifestFile> manifestFiles;
        private final List<io.trino.spi.type.Type> types;
        private final io.trino.spi.type.Type partitionsColumnType;
        private final Map<Integer, PartitionSpec> partitionSpecsById;

        public ManifestFilesIterable(List<ManifestFile> manifestFiles, List<io.trino.spi.type.Type> types)
        {
            this.manifestFiles = requireNonNull(manifestFiles, "manifestFiles is null");
            this.types = requireNonNull(types, "types is null");
            this.partitionsColumnType = tableMetadata.getColumns().stream()
                    .filter(column -> column.getName().equals("partitions"))
                    .findAny()
                    .orElseThrow(() -> new VerifyException("partitions field cannot be found"))
                    .getType();
            this.partitionSpecsById = icebergTable.specs();
        }

        public RecordCursor cursor()
        {
            return new InMemoryRecordSet.InMemoryRecordCursor(types, this.iterator());
        }

        @Override
        public Iterator<List<Object>> iterator()
        {
            Iterator<ManifestFile> manifestFileIterator = manifestFiles.iterator();

            return new Iterator<>() {
                @Override
                public boolean hasNext()
                {
                    return manifestFileIterator.hasNext();
                }

                @Override
                public List<Object> next()
                {
                    return getRecord(manifestFileIterator.next());
                }
            };
        }

        private List<Object> getRecord(ManifestFile file)
        {
            List<Object> columns = new ArrayList<>();
            columns.add(file.path());
            columns.add(file.length());
            columns.add(file.partitionSpecId());
            columns.add(file.snapshotId());
            columns.add(file.addedFilesCount());
            columns.add(file.addedRowsCount());
            columns.add(file.existingFilesCount());
            columns.add(file.existingRowsCount());
            columns.add(file.deletedFilesCount());
            columns.add(file.deletedRowsCount());
            columns.add(getPartitionSummaries(file.partitions(), partitionSpecsById.get(file.partitionSpecId())));
            return columns;
        }

        private Object getPartitionSummaries(List<PartitionFieldSummary> summaries, PartitionSpec partitionSpec)
        {
            BlockBuilder blockBuilder = partitionsColumnType.createBlockBuilder(null, 1);
            BlockBuilder singleArrayWriter = blockBuilder.beginBlockEntry();
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
                        nestedType, Conversions.fromByteBuffer(nestedType, summary.lowerBound())));
                VARCHAR.writeString(rowBuilder, field.transform().toHumanString(
                        nestedType, Conversions.fromByteBuffer(nestedType, summary.upperBound())));
                singleArrayWriter.closeEntry();
            }
            blockBuilder.closeEntry();
            return partitionsColumnType.getObject(blockBuilder.build(), 0);
        }
    }
}
