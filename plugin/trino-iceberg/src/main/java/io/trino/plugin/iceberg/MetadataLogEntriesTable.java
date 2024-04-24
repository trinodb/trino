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

import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataTableType.METADATA_LOG_ENTRIES;

public class MetadataLogEntriesTable
        extends BaseSystemTable
{
    private static final String TIMESTAMP_COLUMN_NAME = "timestamp";
    private static final String FILE_COLUMN_NAME = "file";
    private static final String LATEST_SNAPSHOT_ID_COLUMN_NAME = "latest_snapshot_id";
    private static final String LATEST_SCHEMA_ID_COLUMN_NAME = "latest_schema_id";
    private static final String LATEST_SEQUENCE_NUMBER_COLUMN_NAME = "latest_sequence_number";

    public MetadataLogEntriesTable(SchemaTableName tableName, Table icebergTable)
    {
        super(
                requireNonNull(icebergTable, "icebergTable is null"),
                createConnectorTableMetadata(requireNonNull(tableName, "tableName is null")),
                METADATA_LOG_ENTRIES);
    }

    private static ConnectorTableMetadata createConnectorTableMetadata(SchemaTableName tableName)
    {
        return new ConnectorTableMetadata(
                tableName,
                ImmutableList.<ColumnMetadata>builder()
                        .add(new ColumnMetadata(TIMESTAMP_COLUMN_NAME, TIMESTAMP_TZ_MILLIS))
                        .add(new ColumnMetadata(FILE_COLUMN_NAME, VARCHAR))
                        .add(new ColumnMetadata(LATEST_SNAPSHOT_ID_COLUMN_NAME, BIGINT))
                        .add(new ColumnMetadata(LATEST_SCHEMA_ID_COLUMN_NAME, INTEGER))
                        .add(new ColumnMetadata(LATEST_SEQUENCE_NUMBER_COLUMN_NAME, BIGINT))
                        .build());
    }

    @Override
    protected void addRow(PageListBuilder pagesBuilder, StructLike structLike, TimeZoneKey timeZoneKey, Map<String, Integer> columnNameToPositionInSchema)
    {
        pagesBuilder.beginRow();

        pagesBuilder.appendTimestampTzMillis(
                structLike.get(columnNameToPositionInSchema.get(TIMESTAMP_COLUMN_NAME), Long.class) / MICROSECONDS_PER_MILLISECOND,
                timeZoneKey);
        pagesBuilder.appendVarchar(structLike.get(columnNameToPositionInSchema.get(FILE_COLUMN_NAME), String.class));
        pagesBuilder.appendBigint(structLike.get(columnNameToPositionInSchema.get(LATEST_SNAPSHOT_ID_COLUMN_NAME), Long.class));
        pagesBuilder.appendInteger(structLike.get(columnNameToPositionInSchema.get(LATEST_SCHEMA_ID_COLUMN_NAME), Integer.class));
        pagesBuilder.appendBigint(structLike.get(columnNameToPositionInSchema.get(LATEST_SEQUENCE_NUMBER_COLUMN_NAME), Long.class));
        pagesBuilder.endRow();
    }
}
