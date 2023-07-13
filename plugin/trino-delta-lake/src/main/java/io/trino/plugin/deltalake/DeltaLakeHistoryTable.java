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
import io.trino.plugin.deltalake.transactionlog.CommitInfoEntry;
import io.trino.plugin.deltalake.util.PageListBuilder;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TypeManager;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;

public class DeltaLakeHistoryTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final List<CommitInfoEntry> commitInfoEntries;

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
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        if (commitInfoEntries.isEmpty()) {
            return new FixedPageSource(ImmutableList.of());
        }
        return new FixedPageSource(buildPages(session));
    }

    private List<Page> buildPages(ConnectorSession session)
    {
        PageListBuilder pagesBuilder = PageListBuilder.forTable(tableMetadata);
        TimeZoneKey timeZoneKey = session.getTimeZoneKey();

        commitInfoEntries.forEach(commitInfoEntry -> {
            pagesBuilder.beginRow();

            pagesBuilder.appendBigint(commitInfoEntry.getVersion());
            pagesBuilder.appendTimestampTzMillis(commitInfoEntry.getTimestamp(), timeZoneKey);
            write(commitInfoEntry.getUserId(), pagesBuilder);
            write(commitInfoEntry.getUserName(), pagesBuilder);
            write(commitInfoEntry.getOperation(), pagesBuilder);
            if (commitInfoEntry.getOperationParameters() == null) {
                pagesBuilder.appendNull();
            }
            else {
                pagesBuilder.appendVarcharVarcharMap(commitInfoEntry.getOperationParameters());
            }
            write(commitInfoEntry.getClusterId(), pagesBuilder);
            pagesBuilder.appendBigint(commitInfoEntry.getReadVersion());
            write(commitInfoEntry.getIsolationLevel(), pagesBuilder);
            commitInfoEntry.isBlindAppend().ifPresentOrElse(pagesBuilder::appendBoolean, pagesBuilder::appendNull);

            pagesBuilder.endRow();
        });

        return pagesBuilder.build();
    }

    private static void write(String value, PageListBuilder pagesBuilder)
    {
        if (value == null) {
            pagesBuilder.appendNull();
        }
        else {
            pagesBuilder.appendVarchar(value);
        }
    }
}
