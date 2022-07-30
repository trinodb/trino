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
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TimeZoneKey;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.iceberg.ColumnIdentity.createColumnIdentity;
import static io.trino.plugin.iceberg.IcebergUtil.createColumnHandle;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static java.util.Objects.requireNonNull;

public class HistoryTable
        implements IcebergSystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final Table icebergTable;

    private static final List<IcebergColumnHandle> COLUMNS_HANDLES_LIST = ImmutableList.<IcebergColumnHandle>builder()
            .add(createColumnHandle(createColumnIdentity(Types.NestedField.required(1, "made_current_at", Types.TimestampType.withZone())), TIMESTAMP_TZ_MILLIS))
            .add(createColumnHandle(createColumnIdentity(Types.NestedField.required(2, "snapshot_id", Types.LongType.get())), BIGINT))
            .add(createColumnHandle(createColumnIdentity(Types.NestedField.optional(3, "parent_id", Types.LongType.get())), BIGINT))
            .add(createColumnHandle(createColumnIdentity(Types.NestedField.required(4, "is_current_ancestor", Types.BooleanType.get())), BOOLEAN))
            .build();

    private static final List<ColumnMetadata> COLUMNS = COLUMNS_HANDLES_LIST.stream()
            .map(icebergColumnHandle -> new ColumnMetadata(icebergColumnHandle.getName(), icebergColumnHandle.getType()))
            .collect(Collectors.toUnmodifiableList());

    private static final Map<String, ColumnHandle> COLUMN_HANDLES_MAP = COLUMNS_HANDLES_LIST.stream()
                .collect(toImmutableMap(IcebergColumnHandle::getName, Function.identity()));

    public HistoryTable(SchemaTableName tableName, Table icebergTable)
    {
        tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"), COLUMNS);
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles()
    {
        return COLUMN_HANDLES_MAP;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(COLUMNS);

        Set<Long> ancestorIds = ImmutableSet.copyOf(SnapshotUtil.currentAncestorIds(icebergTable));
        TimeZoneKey timeZoneKey = session.getTimeZoneKey();
        for (HistoryEntry historyEntry : icebergTable.history()) {
            long snapshotId = historyEntry.snapshotId();
            Snapshot snapshot = icebergTable.snapshot(snapshotId);

            table.addRow(
                    packDateTimeWithZone(historyEntry.timestampMillis(), timeZoneKey),
                    snapshotId,
                    snapshot != null ? snapshot.parentId() : null,
                    ancestorIds.contains(snapshotId));
        }

        return table.build().cursor();
    }
}
