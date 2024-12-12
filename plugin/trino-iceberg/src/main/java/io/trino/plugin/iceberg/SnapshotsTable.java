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
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import org.apache.iceberg.Table;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataTableType.SNAPSHOTS;

public class SnapshotsTable
        extends BaseSystemTable
{
    private static final String COMMITTED_AT_COLUMN_NAME = "committed_at";
    private static final String SNAPSHOT_ID_COLUMN_NAME = "snapshot_id";
    private static final String PARENT_ID_COLUMN_NAME = "parent_id";
    private static final String OPERATION_COLUMN_NAME = "operation";
    private static final String MANIFEST_LIST_COLUMN_NAME = "manifest_list";
    private static final String SUMMARY_COLUMN_NAME = "summary";

    public SnapshotsTable(SchemaTableName tableName, TypeManager typeManager, Table icebergTable, ExecutorService executor)
    {
        super(
                requireNonNull(icebergTable, "icebergTable is null"),
                createConnectorTableMetadata(
                        requireNonNull(tableName, "tableName is null"),
                        requireNonNull(typeManager, "typeManager is null")),
                SNAPSHOTS,
                executor);
    }

    private static ConnectorTableMetadata createConnectorTableMetadata(SchemaTableName tableName, TypeManager typeManager)
    {
        return new ConnectorTableMetadata(
                tableName,
                ImmutableList.<ColumnMetadata>builder()
                        .add(new ColumnMetadata(COMMITTED_AT_COLUMN_NAME, TIMESTAMP_TZ_MILLIS))
                        .add(new ColumnMetadata(SNAPSHOT_ID_COLUMN_NAME, BIGINT))
                        .add(new ColumnMetadata(PARENT_ID_COLUMN_NAME, BIGINT))
                        .add(new ColumnMetadata(OPERATION_COLUMN_NAME, VARCHAR))
                        .add(new ColumnMetadata(MANIFEST_LIST_COLUMN_NAME, VARCHAR))
                        .add(new ColumnMetadata(SUMMARY_COLUMN_NAME, typeManager.getType(TypeSignature.mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()))))
                        .build());
    }

    @Override
    protected void addRow(PageListBuilder pagesBuilder, Row row, TimeZoneKey timeZoneKey)
    {
        pagesBuilder.beginRow();
        pagesBuilder.appendTimestampTzMillis(row.get(COMMITTED_AT_COLUMN_NAME, Long.class) / MICROSECONDS_PER_MILLISECOND, timeZoneKey);
        pagesBuilder.appendBigint(row.get(SNAPSHOT_ID_COLUMN_NAME, Long.class));
        pagesBuilder.appendBigint(row.get(PARENT_ID_COLUMN_NAME, Long.class));
        pagesBuilder.appendVarchar(row.get(OPERATION_COLUMN_NAME, String.class));
        pagesBuilder.appendVarchar(row.get(MANIFEST_LIST_COLUMN_NAME, String.class));
        //noinspection unchecked
        pagesBuilder.appendVarcharVarcharMap(row.get(SUMMARY_COLUMN_NAME, Map.class));
        pagesBuilder.endRow();
    }
}
