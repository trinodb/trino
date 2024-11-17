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
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataTableType.HISTORY;

public class HistoryTable
        extends BaseSystemTable
{
    private static final List<ColumnMetadata> COLUMNS = ImmutableList.<ColumnMetadata>builder()
            .add(new ColumnMetadata("made_current_at", TIMESTAMP_TZ_MILLIS))
            .add(new ColumnMetadata("snapshot_id", BIGINT))
            .add(new ColumnMetadata("parent_id", BIGINT))
            .add(new ColumnMetadata("is_current_ancestor", BOOLEAN))
            .build();

    public HistoryTable(SchemaTableName tableName, Table icebergTable)
    {
        super(
                requireNonNull(icebergTable, "icebergTable is null"),
                new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"), COLUMNS),
                HISTORY);
    }

    @Override
    protected void addRow(PageListBuilder pagesBuilder, StructLike structLike, TimeZoneKey timeZoneKey, Map<String, Integer> columnNameToPositionInSchema)
    {
        pagesBuilder.beginRow();
        pagesBuilder.appendTimestampTzMillis(structLike.get(columnNameToPositionInSchema.get("made_current_at"), Long.class) / MICROSECONDS_PER_MILLISECOND, timeZoneKey);
        pagesBuilder.appendBigint(structLike.get(columnNameToPositionInSchema.get("snapshot_id"), Long.class));
        pagesBuilder.appendBigint(structLike.get(columnNameToPositionInSchema.get("parent_id"), Long.class));
        pagesBuilder.appendBoolean(structLike.get(columnNameToPositionInSchema.get("is_current_ancestor"), Boolean.class));
        pagesBuilder.endRow();
    }
}
