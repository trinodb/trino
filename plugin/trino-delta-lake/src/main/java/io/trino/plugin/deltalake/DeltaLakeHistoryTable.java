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
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.transactionlog.CommitInfoEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.Transaction;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.util.PageListBuilder;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class DeltaLakeHistoryTable
        extends BaseTransactionsTable
{
    public DeltaLakeHistoryTable(
            SchemaTableName tableName,
            String tableLocation,
            TrinoFileSystemFactory fileSystemFactory,
            TransactionLogAccess transactionLogAccess,
            TypeManager typeManager)
    {
        super(
                tableName,
                tableLocation,
                fileSystemFactory,
                transactionLogAccess,
                typeManager,
                new ConnectorTableMetadata(
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
                                .add(new ColumnMetadata("operation_metrics", typeManager.getType(mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()))))
                                //TODO add support for userMetadata, engineInfo
                                .build()));
    }

    @Override
    protected List<Page> buildPages(ConnectorSession session, PageListBuilder pagesBuilder, List<Transaction> transactions, TrinoFileSystem fileSystem)
    {
        List<CommitInfoEntry> commitInfoEntries;
        try (Stream<CommitInfoEntry> commitStream = transactions.stream()
                .flatMap(transaction -> transaction.transactionEntries().getEntries(fileSystem))
                .map(DeltaLakeTransactionLogEntry::getCommitInfo)
                .filter(Objects::nonNull)) {
            commitInfoEntries = commitStream.collect(toImmutableList());
        }

        TimeZoneKey timeZoneKey = session.getTimeZoneKey();

        commitInfoEntries.forEach(commitInfoEntry -> {
            pagesBuilder.beginRow();

            pagesBuilder.appendBigint(commitInfoEntry.version());
            commitInfoEntry.inCommitTimestamp().ifPresentOrElse(
                    // use `inCommitTimestamp` if table In-Commit timestamps enabled, otherwise read the `timestamp` field
                    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#recommendations-for-readers-of-tables-with-in-commit-timestamps
                    inCommitTimestamp -> pagesBuilder.appendTimestampTzMillis(inCommitTimestamp, timeZoneKey),
                    () -> pagesBuilder.appendTimestampTzMillis(commitInfoEntry.timestamp(), timeZoneKey));
            write(commitInfoEntry.userId(), pagesBuilder);
            write(commitInfoEntry.userName(), pagesBuilder);
            write(commitInfoEntry.operation(), pagesBuilder);
            if (commitInfoEntry.operationParameters() == null) {
                pagesBuilder.appendNull();
            }
            else {
                pagesBuilder.appendVarcharVarcharMap(commitInfoEntry.operationParameters());
            }
            write(commitInfoEntry.clusterId(), pagesBuilder);
            pagesBuilder.appendBigint(commitInfoEntry.readVersion());
            write(commitInfoEntry.isolationLevel(), pagesBuilder);
            commitInfoEntry.isBlindAppend().ifPresentOrElse(pagesBuilder::appendBoolean, pagesBuilder::appendNull);
            if (commitInfoEntry.operationMetrics() == null) {
                pagesBuilder.appendNull();
            }
            else {
                pagesBuilder.appendVarcharVarcharMap(commitInfoEntry.operationMetrics());
            }
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
