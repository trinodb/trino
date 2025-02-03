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
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeZoneKey;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataTableType.ALL_MANIFESTS;

public class AllManifestsTable
        extends BaseSystemTable
{
    public AllManifestsTable(SchemaTableName tableName, Table icebergTable, ExecutorService executor)
    {
        super(requireNonNull(icebergTable, "icebergTable is null"),
                new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"), ImmutableList.<ColumnMetadata>builder()
                        .add(new ColumnMetadata("path", VARCHAR))
                        .add(new ColumnMetadata("length", BIGINT))
                        .add(new ColumnMetadata("partition_spec_id", INTEGER))
                        .add(new ColumnMetadata("added_snapshot_id", BIGINT))
                        .add(new ColumnMetadata("added_data_files_count", INTEGER))
                        .add(new ColumnMetadata("existing_data_files_count", INTEGER))
                        .add(new ColumnMetadata("deleted_data_files_count", INTEGER))
                        .add(new ColumnMetadata("partition_summaries", new ArrayType(RowType.rowType(
                                RowType.field("contains_null", BOOLEAN),
                                RowType.field("contains_nan", BOOLEAN),
                                RowType.field("lower_bound", VARCHAR),
                                RowType.field("upper_bound", VARCHAR)))))
                        .build()),
                ALL_MANIFESTS,
                executor);
    }

    @Override
    protected void addRow(PageListBuilder pagesBuilder, Row row, TimeZoneKey timeZoneKey)
    {
        pagesBuilder.beginRow();
        pagesBuilder.appendVarchar(row.get("path", String.class));
        pagesBuilder.appendBigint(row.get("length", Long.class));
        pagesBuilder.appendInteger(row.get("partition_spec_id", Integer.class));
        pagesBuilder.appendBigint(row.get("added_snapshot_id", Long.class));
        pagesBuilder.appendInteger(row.get("added_data_files_count", Integer.class));
        pagesBuilder.appendInteger(row.get("existing_data_files_count", Integer.class));
        pagesBuilder.appendInteger(row.get("deleted_data_files_count", Integer.class));
        //noinspection unchecked
        appendPartitionSummaries((ArrayBlockBuilder) pagesBuilder.nextColumn(), row.get("partition_summaries", List.class));
        pagesBuilder.endRow();
    }

    private static void appendPartitionSummaries(ArrayBlockBuilder arrayBuilder, List<StructLike> partitionSummaries)
    {
        arrayBuilder.buildEntry(elementBuilder -> {
            for (StructLike partitionSummary : partitionSummaries) {
                ((RowBlockBuilder) elementBuilder).buildEntry(fieldBuilders -> {
                    BOOLEAN.writeBoolean(fieldBuilders.get(0), partitionSummary.get(0, Boolean.class)); // required contains_null
                    Boolean containsNan = partitionSummary.get(1, Boolean.class);
                    if (containsNan == null) {
                        // This usually occurs when reading from V1 table, where contains_nan is not populated.
                        fieldBuilders.get(1).appendNull();
                    }
                    else {
                        BOOLEAN.writeBoolean(fieldBuilders.get(1), containsNan);
                    }
                    VARCHAR.writeString(fieldBuilders.get(2), partitionSummary.get(2, String.class)); // optional lower_bound (human-readable)
                    VARCHAR.writeString(fieldBuilders.get(3), partitionSummary.get(3, String.class)); // optional upper_bound (human-readable)
                });
            }
        });
    }
}
