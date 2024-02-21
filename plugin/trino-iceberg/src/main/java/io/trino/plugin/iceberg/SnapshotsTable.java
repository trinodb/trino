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
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;

import java.util.Map;

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

    public SnapshotsTable(SchemaTableName tableName, TypeManager typeManager, Table icebergTable)
    {
        super(
                requireNonNull(icebergTable, "icebergTable is null"),
                createConnectorTableMetadata(
                        requireNonNull(tableName, "tableName is null"),
                        requireNonNull(typeManager, "typeManager is null")),
                SNAPSHOTS);
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
    protected void addRow(PageListBuilder pagesBuilder, StructLike structLike, TimeZoneKey timeZoneKey, Map<String, Integer> columnNameToPositionInSchema)
    {
        pagesBuilder.beginRow();

        pagesBuilder.appendTimestampTzMillis(
                structLike.get(columnNameToPositionInSchema.get(COMMITTED_AT_COLUMN_NAME), Long.class) / MICROSECONDS_PER_MILLISECOND,
                timeZoneKey);
        pagesBuilder.appendBigint(structLike.get(columnNameToPositionInSchema.get(SNAPSHOT_ID_COLUMN_NAME), Long.class));

        Long parentId = structLike.get(columnNameToPositionInSchema.get(PARENT_ID_COLUMN_NAME), Long.class);
        pagesBuilder.appendBigint(parentId != null ? parentId.longValue() : null);

        pagesBuilder.appendVarchar(structLike.get(columnNameToPositionInSchema.get(OPERATION_COLUMN_NAME), String.class));
        pagesBuilder.appendVarchar(structLike.get(columnNameToPositionInSchema.get(MANIFEST_LIST_COLUMN_NAME), String.class));
        pagesBuilder.appendVarcharVarcharMap(structLike.get(columnNameToPositionInSchema.get(SUMMARY_COLUMN_NAME), Map.class));
        pagesBuilder.endRow();
    }
}
