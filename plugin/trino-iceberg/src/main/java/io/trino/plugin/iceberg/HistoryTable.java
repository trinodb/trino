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
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TimeZoneKey;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.SnapshotUtil;

import java.util.List;
import java.util.Set;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static java.util.Objects.requireNonNull;

public class HistoryTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final Table icebergTable;

    private static final List<ColumnMetadata> COLUMNS = ImmutableList.<ColumnMetadata>builder()
            .add(new ColumnMetadata("made_current_at", TIMESTAMP_TZ_MILLIS))
            .add(new ColumnMetadata("snapshot_id", BIGINT))
            .add(new ColumnMetadata("parent_id", BIGINT))
            .add(new ColumnMetadata("is_current_ancestor", BOOLEAN))
            .build();

    public HistoryTable(SchemaTableName tableName, Table icebergTable)
    {
        tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"), COLUMNS);
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
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
        InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(COLUMNS);

        Set<Long> ancestorIds = ImmutableSet.copyOf(SnapshotUtil.currentAncestorIds(icebergTable));
        TimeZoneKey timeZoneKey = session.getTimeZoneKey();
        for (Snapshot snapshot : icebergTable.snapshots()) {
            long snapshotId = snapshot.snapshotId();

            table.addRow(
                    packDateTimeWithZone(snapshot.timestampMillis(), timeZoneKey),
                    snapshotId,
                    snapshot.parentId(),
                    ancestorIds.contains(snapshotId));
        }

        return table.build().cursor();
    }
}
