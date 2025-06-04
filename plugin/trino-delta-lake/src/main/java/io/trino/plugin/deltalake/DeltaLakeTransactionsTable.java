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
import io.airlift.json.JsonCodec;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.Transaction;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.util.PageListBuilder;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;

import java.util.List;

import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.StandardTypes.JSON;
import static java.util.Objects.requireNonNull;

public class DeltaLakeTransactionsTable
        extends BaseTransactionsTable
{
    private static final JsonCodec<List<DeltaLakeTransactionLogEntry>> TRANSACTION_LOG_ENTRIES_CODEC = listJsonCodec(DeltaLakeTransactionLogEntry.class);

    public DeltaLakeTransactionsTable(
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
                                .add(new ColumnMetadata("transaction", typeManager.getType(new TypeSignature(JSON))))
                                .build()));
    }

    @Override
    protected List<Page> buildPages(ConnectorSession session, PageListBuilder pagesBuilder, List<Transaction> transactions, TrinoFileSystem fileSystem)
    {
        for (Transaction transaction : transactions) {
            pagesBuilder.beginRow();
            pagesBuilder.appendBigint(transaction.transactionId());
            pagesBuilder.appendVarchar(TRANSACTION_LOG_ENTRIES_CODEC.toJson(
                    transaction.transactionEntries().getEntriesList(fileSystem)));
            pagesBuilder.endRow();
        }
        return pagesBuilder.build();
    }
}
