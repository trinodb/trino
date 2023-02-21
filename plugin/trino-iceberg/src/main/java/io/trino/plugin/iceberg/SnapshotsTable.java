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
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class SnapshotsTable
        implements SystemTable
{
    private final TypeManager typeManager;
    private final ConnectorTableMetadata tableMetadata;
    private final Table icebergTable;

    public SnapshotsTable(SchemaTableName tableName, TypeManager typeManager, Table icebergTable)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
        tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"),
                ImmutableList.<ColumnMetadata>builder()
                        .add(new ColumnMetadata("committed_at", TIMESTAMP_TZ_MILLIS))
                        .add(new ColumnMetadata("snapshot_id", BIGINT))
                        .add(new ColumnMetadata("parent_id", BIGINT))
                        .add(new ColumnMetadata("operation", VARCHAR))
                        .add(new ColumnMetadata("manifest_list", VARCHAR))
                        .add(new ColumnMetadata("summary", typeManager.getType(mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()))))
                        .build());
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

        SnapshotsIterable snapshotsIterable = new SnapshotsIterable(icebergTable.snapshots(), types, session.getTimeZoneKey());
        return snapshotsIterable.cursor();
    }

    private class SnapshotsIterable
            implements Iterable<List<Object>>
    {
        private final Iterable<Snapshot> snapshots;
        private final List<Type> types;
        private final TimeZoneKey timeZoneKey;
        private final Type summaryMapType;

        public SnapshotsIterable(Iterable<Snapshot> snapshots, List<Type> types, TimeZoneKey timeZoneKey)
        {
            this.snapshots = requireNonNull(snapshots, "snapshots is null");
            this.types = requireNonNull(types, "types is null");
            this.timeZoneKey = requireNonNull(timeZoneKey, "timeZoneKey is null");
            this.summaryMapType = typeManager.getType(mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()));
        }

        public RecordCursor cursor()
        {
            return new InMemoryRecordSet.InMemoryRecordCursor(types, this.iterator());
        }

        @Override
        public Iterator<List<Object>> iterator()
        {
            Iterator<Snapshot> snapshotsIterator = snapshots.iterator();

            return new Iterator<>() {
                @Override
                public boolean hasNext()
                {
                    return snapshotsIterator.hasNext();
                }

                @Override
                public List<Object> next()
                {
                    return getRecord(snapshotsIterator.next());
                }
            };
        }

        private List<Object> getRecord(Snapshot snapshot)
        {
            List<Object> columns = new ArrayList<>();
            columns.add(packDateTimeWithZone(snapshot.timestampMillis(), timeZoneKey));
            columns.add(snapshot.snapshotId());
            columns.add(snapshot.parentId());
            columns.add(snapshot.operation());
            columns.add(snapshot.manifestListLocation());
            columns.add(getSummaryMap(snapshot.summary()));
            return columns;
        }

        private Object getSummaryMap(Map<String, String> values)
        {
            BlockBuilder blockBuilder = summaryMapType.createBlockBuilder(null, 1);
            BlockBuilder singleMapBlockBuilder = blockBuilder.beginBlockEntry();
            values.forEach((key, value) -> {
                VARCHAR.writeString(singleMapBlockBuilder, key);
                VARCHAR.writeString(singleMapBlockBuilder, value);
            });
            blockBuilder.closeEntry();
            return summaryMapType.getObject(blockBuilder, 0);
        }
    }
}
