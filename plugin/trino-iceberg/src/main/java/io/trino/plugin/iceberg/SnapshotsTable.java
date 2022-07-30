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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.iceberg.ColumnIdentity.createColumnIdentity;
import static io.trino.plugin.iceberg.IcebergUtil.createColumnHandle;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class SnapshotsTable
        implements IcebergSystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final Map<String, ColumnHandle> columnHandles;
    private final Table icebergTable;

    public SnapshotsTable(SchemaTableName tableName, TypeManager typeManager, Table icebergTable)
    {
        requireNonNull(typeManager, "typeManager is null");

        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
        List<IcebergColumnHandle> columnHandlesList = ImmutableList.<IcebergColumnHandle>builder()
                .add(createColumnHandle(createColumnIdentity(Types.NestedField.required(1, "committed_at", Types.TimestampType.withZone())), TIMESTAMP_TZ_MILLIS))
                .add(createColumnHandle(createColumnIdentity(Types.NestedField.required(2, "snapshot_id", Types.LongType.get())), BIGINT))
                .add(createColumnHandle(createColumnIdentity(Types.NestedField.optional(3, "parent_id", Types.LongType.get())), BIGINT))
                .add(createColumnHandle(createColumnIdentity(Types.NestedField.optional(4, "operation", Types.StringType.get())), VARCHAR))
                .add(createColumnHandle(createColumnIdentity(Types.NestedField.optional(5, "manifest_list", Types.StringType.get())), VARCHAR))
                .add(createColumnHandle(
                        createColumnIdentity(Types.NestedField.optional(
                                6,
                                "summary",
                                Types.MapType.ofRequired(7, 8, Types.StringType.get(), Types.StringType.get()))),
                        typeManager.getType(TypeSignature.mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()))))
                .build();
        columnHandles = columnHandlesList.stream()
                .collect(toImmutableMap(IcebergColumnHandle::getName, Function.identity()));
        tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"),
                columnHandlesList.stream()
                        .map(icebergColumnHandle -> new ColumnMetadata(icebergColumnHandle.getName(), icebergColumnHandle.getType()))
                        .collect(Collectors.toUnmodifiableList()));
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
        return new FixedPageSource(buildPages(tableMetadata, session, icebergTable));
    }

    private static List<Page> buildPages(ConnectorTableMetadata tableMetadata, ConnectorSession session, Table icebergTable)
    {
        PageListBuilder pagesBuilder = PageListBuilder.forTable(tableMetadata);

        TimeZoneKey timeZoneKey = session.getTimeZoneKey();
        icebergTable.snapshots().forEach(snapshot -> {
            pagesBuilder.beginRow();
            pagesBuilder.appendTimestampTzMillis(snapshot.timestampMillis(), timeZoneKey);
            pagesBuilder.appendBigint(snapshot.snapshotId());
            if (checkNonNull(snapshot.parentId(), pagesBuilder)) {
                pagesBuilder.appendBigint(snapshot.parentId());
            }
            if (checkNonNull(snapshot.operation(), pagesBuilder)) {
                pagesBuilder.appendVarchar(snapshot.operation());
            }
            if (checkNonNull(snapshot.manifestListLocation(), pagesBuilder)) {
                pagesBuilder.appendVarchar(snapshot.manifestListLocation());
            }
            if (checkNonNull(snapshot.summary(), pagesBuilder)) {
                pagesBuilder.appendVarcharVarcharMap(snapshot.summary());
            }
            pagesBuilder.endRow();
        });

        return pagesBuilder.build();
    }

    private static boolean checkNonNull(Object object, PageListBuilder pagesBuilder)
    {
        if (object == null) {
            pagesBuilder.appendNull();
            return false;
        }
        return true;
    }
}
