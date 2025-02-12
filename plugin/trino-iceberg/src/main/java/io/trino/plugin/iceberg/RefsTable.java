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
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TimeZoneKey;
import org.apache.iceberg.Table;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataTableType.REFS;

public class RefsTable
        extends BaseSystemTable
{
    private static final List<ColumnMetadata> COLUMNS = ImmutableList.<ColumnMetadata>builder()
            .add(new ColumnMetadata("name", VARCHAR))
            .add(new ColumnMetadata("type", VARCHAR))
            .add(new ColumnMetadata("snapshot_id", BIGINT))
            .add(new ColumnMetadata("max_reference_age_in_ms", BIGINT))
            .add(new ColumnMetadata("min_snapshots_to_keep", INTEGER))
            .add(new ColumnMetadata("max_snapshot_age_in_ms", BIGINT))
            .build();

    public RefsTable(SchemaTableName tableName, Table icebergTable, ExecutorService executor)
    {
        super(
                requireNonNull(icebergTable, "icebergTable is null"),
                new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"), COLUMNS),
                REFS,
                executor);
    }

    @Override
    protected void addRow(PageListBuilder pagesBuilder, Row row, TimeZoneKey timeZoneKey)
    {
        pagesBuilder.beginRow();
        pagesBuilder.appendVarchar(row.get("name", String.class));
        pagesBuilder.appendVarchar(row.get("type", String.class));
        pagesBuilder.appendBigint(row.get("snapshot_id", Long.class));
        pagesBuilder.appendBigint(row.get("max_reference_age_in_ms", Long.class));
        pagesBuilder.appendInteger(row.get("min_snapshots_to_keep", Integer.class));
        pagesBuilder.appendBigint(row.get("max_snapshot_age_in_ms", Long.class));
        pagesBuilder.endRow();
    }
}
