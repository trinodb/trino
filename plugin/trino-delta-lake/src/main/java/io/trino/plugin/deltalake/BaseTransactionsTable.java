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

import io.airlift.units.DataSize;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.deltalake.metastore.DeltaMetastoreTable;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.Transaction;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.util.PageListBuilder;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail.loadNewTail;
import static java.util.Objects.requireNonNull;

public abstract class BaseTransactionsTable
        implements SystemTable
{
    private final DeltaMetastoreTable table;
    private final DeltaLakeFileSystemFactory fileSystemFactory;
    private final TransactionLogAccess transactionLogAccess;
    private final ConnectorTableMetadata tableMetadata;

    public BaseTransactionsTable(
            DeltaMetastoreTable table,
            DeltaLakeFileSystemFactory fileSystemFactory,
            TransactionLogAccess transactionLogAccess,
            TypeManager typeManager,
            ConnectorTableMetadata tableMetadata)
    {
        requireNonNull(typeManager, "typeManager is null");
        this.table = requireNonNull(table, "table is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogAccess is null");
        this.tableMetadata = requireNonNull(tableMetadata, "tableMetadata is null");
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
        TrinoFileSystem fileSystem = fileSystemFactory.create(session, table);
        long snapshotVersion;
        try {
            // Verify the transaction log is readable
            TableSnapshot tableSnapshot = transactionLogAccess.loadSnapshot(session, table, Optional.empty());
            snapshotVersion = tableSnapshot.getVersion();
            transactionLogAccess.getMetadataEntry(session, fileSystem, tableSnapshot);
        }
        catch (IOException e) {
            throw new TrinoException(DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA, "Unable to load table metadata from location: " + table.location(), e);
        }

        int versionColumnIndex = IntStream.range(0, tableMetadata.getColumns().size())
                .filter(i -> tableMetadata.getColumns().get(i).getName().equals("version"))
                .boxed()
                .collect(onlyElement());

        Optional<Long> startVersionExclusive = Optional.empty();
        Optional<Long> endVersionInclusive = Optional.empty();

        if (constraint.getDomains().isPresent()) {
            Map<Integer, Domain> domains = constraint.getDomains().get();
            if (domains.containsKey(versionColumnIndex)) {
                Domain versionDomain = domains.get(versionColumnIndex); // The zero value here relies on the column ordering defined in the constructor
                Range range = versionDomain.getValues().getRanges().getSpan();
                if (range.isSingleValue()) {
                    long value = (long) range.getSingleValue();
                    startVersionExclusive = Optional.of(value - 1);
                    endVersionInclusive = Optional.of(value);
                }
                else {
                    Optional<Long> lowValue = range.getLowValue().map(Long.class::cast);
                    if (lowValue.isPresent()) {
                        startVersionExclusive = Optional.of(lowValue.get() - (range.isLowInclusive() ? 1 : 0));
                    }

                    Optional<Long> highValue = range.getHighValue().map(Long.class::cast);
                    if (highValue.isPresent()) {
                        endVersionInclusive = Optional.of(highValue.get() - (range.isHighInclusive() ? 0 : 1));
                    }
                }
            }
        }

        if (startVersionExclusive.isPresent() && endVersionInclusive.isPresent() && startVersionExclusive.get() >= endVersionInclusive.get()) {
            return new EmptyPageSource();
        }

        if (endVersionInclusive.isEmpty()) {
            endVersionInclusive = Optional.of(snapshotVersion);
        }

        PageListBuilder pagesBuilder = PageListBuilder.forTable(tableMetadata);
        try {
            checkArgument(endVersionInclusive.isPresent(), "endVersionInclusive must be present");
            List<Transaction> transactions = loadNewTail(fileSystem, table.location(), startVersionExclusive, endVersionInclusive, DataSize.ofBytes(0)).getTransactions();
            return new FixedPageSource(buildPages(session, pagesBuilder, transactions, fileSystem));
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (IOException | RuntimeException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Error getting commit info entries from " + table.location(), e);
        }
    }

    protected abstract List<Page> buildPages(ConnectorSession session, PageListBuilder pagesBuilder, List<Transaction> transactions, TrinoFileSystem fileSystem)
            throws IOException;
}
