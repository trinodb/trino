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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.trino.plugin.deltalake.transactionlog.CommitInfoEntry;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;

public class DeltaLakeHistoryTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final List<Type> types;
    private final Iterable<CommitInfoEntry> commitInfoEntries;
    private final Type varcharToVarcharMapType;

    public DeltaLakeHistoryTable(SchemaTableName tableName, List<CommitInfoEntry> commitInfoEntries, TypeManager typeManager)
    {
        requireNonNull(typeManager, "typeManager is null");
        this.commitInfoEntries = ImmutableList.copyOf(requireNonNull(commitInfoEntries, "commitInfoEntries is null")).stream()
                .sorted(comparingLong(CommitInfoEntry::getVersion).reversed())
                .collect(toImmutableList());

        tableMetadata = new ConnectorTableMetadata(
                requireNonNull(tableName, "tableName is null"),
                ImmutableList.<ColumnMetadata>builder()
                        .add(new ColumnMetadata("version", BIGINT))
                        .add(new ColumnMetadata("timestamp", TIMESTAMP_TZ_MILLIS))
                        .add(new ColumnMetadata("user_id", VARCHAR))
                        .add(new ColumnMetadata("user_name", VARCHAR))
                        .add(new ColumnMetadata("operation", VARCHAR))
                        .add(new ColumnMetadata("operation_parameters", typeManager.getType(mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()))))
                        .add(new ColumnMetadata("cluster_id", VARCHAR))
                        .add(new ColumnMetadata("read_version", BIGINT))
                        .add(new ColumnMetadata("isolation_level", VARCHAR))
                        .add(new ColumnMetadata("is_blind_append", BOOLEAN))
                        //TODO add support for operationMetrics, userMetadata, engineInfo
                        .build());
        types = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());
        varcharToVarcharMapType = typeManager.getType(mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()));
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
        TimeZoneKey timeZoneKey = session.getTimeZoneKey();
        Iterable<List<Object>> records = Iterables.transform(commitInfoEntries, commitInfoEntry -> getRecord(commitInfoEntry, timeZoneKey));

        return new InMemoryRecordSet(types, records).cursor();
    }

    private List<Object> getRecord(CommitInfoEntry commitInfoEntry, TimeZoneKey timeZoneKey)
    {
        List<Object> columns = new ArrayList<>();
        columns.add(commitInfoEntry.getVersion());
        columns.add(packDateTimeWithZone(commitInfoEntry.getTimestamp(), timeZoneKey));
        columns.add(commitInfoEntry.getUserId());
        columns.add(commitInfoEntry.getUserName());
        columns.add(commitInfoEntry.getOperation());
        if (commitInfoEntry.getOperationParameters() == null) {
            columns.add(null);
        }
        else {
            columns.add(toVarcharVarcharMapBlock(commitInfoEntry.getOperationParameters()));
        }
        columns.add(commitInfoEntry.getClusterId());
        columns.add(commitInfoEntry.getReadVersion());
        columns.add(commitInfoEntry.getIsolationLevel());
        columns.add(commitInfoEntry.isBlindAppend().orElse(null));

        return columns;
    }

    private Object toVarcharVarcharMapBlock(Map<String, String> values)
    {
        BlockBuilder blockBuilder = varcharToVarcharMapType.createBlockBuilder(null, 1);
        BlockBuilder singleMapBlockBuilder = blockBuilder.beginBlockEntry();
        values.forEach((key, value) -> {
            VARCHAR.writeString(singleMapBlockBuilder, key);
            VARCHAR.writeString(singleMapBlockBuilder, value);
        });
        blockBuilder.closeEntry();
        return varcharToVarcharMapType.getObject(blockBuilder, 0);
    }
}
