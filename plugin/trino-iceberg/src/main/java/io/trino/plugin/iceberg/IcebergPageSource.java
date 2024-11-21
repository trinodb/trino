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

import io.trino.plugin.hive.ReaderProjectionsAdapter;
import io.trino.plugin.iceberg.delete.RowPredicate;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static java.util.Objects.requireNonNull;

public class IcebergPageSource
        implements ConnectorPageSource
{
    private final int[] expectedColumnIndexes;
    private final ConnectorPageSource delegate;
    private final Optional<ReaderProjectionsAdapter> projectionsAdapter;
    private final Supplier<Optional<RowPredicate>> deletePredicate;
    private final Function<Block, RowBlock> rowIdBlockFactory;
    // The $row_id's index in 'expectedColumns', or -1 if there isn't one
    // this column with contain row position populated in the source, and must be wrapped with constant data for full row id
    private int rowIdColumnIndex = -1;
    // Maps the Iceberg field ids of unmodified columns to their indexes in updateRowIdChildColumnIndexes

    public IcebergPageSource(
            List<IcebergColumnHandle> expectedColumns,
            List<IcebergColumnHandle> requiredColumns,
            ConnectorPageSource delegate,
            Optional<ReaderProjectionsAdapter> projectionsAdapter,
            Supplier<Optional<RowPredicate>> deletePredicate,
            Function<Block, RowBlock> rowIdBlockFactory)
    {
        // expectedColumns should contain columns which should be in the final Page
        // requiredColumns should include all expectedColumns as well as any columns needed by the DeleteFilter
        requireNonNull(expectedColumns, "expectedColumns is null");
        requireNonNull(requiredColumns, "requiredColumns is null");
        this.expectedColumnIndexes = new int[expectedColumns.size()];
        for (int i = 0; i < expectedColumns.size(); i++) {
            IcebergColumnHandle expectedColumn = expectedColumns.get(i);
            checkArgument(expectedColumn.equals(requiredColumns.get(i)), "Expected columns must be a prefix of required columns");
            expectedColumnIndexes[i] = i;

            if (expectedColumn.isMergeRowIdColumn()) {
                this.rowIdColumnIndex = i;
            }
        }

        this.delegate = requireNonNull(delegate, "delegate is null");
        this.projectionsAdapter = requireNonNull(projectionsAdapter, "projectionsAdapter is null");
        this.deletePredicate = requireNonNull(deletePredicate, "deletePredicate is null");
        this.rowIdBlockFactory = requireNonNull(rowIdBlockFactory, "rowIdBlockFactory is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return delegate.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return delegate.isBlocked();
    }

    @Override
    public Page getNextPage()
    {
        try {
            Page dataPage = delegate.getNextPage();
            if (dataPage == null) {
                return null;
            }

            Optional<RowPredicate> deleteFilterPredicate = deletePredicate.get();
            if (deleteFilterPredicate.isPresent()) {
                dataPage = deleteFilterPredicate.get().filterPage(dataPage);
            }

            if (projectionsAdapter.isPresent()) {
                dataPage = projectionsAdapter.get().adaptPage(dataPage);
            }

            dataPage = withRowIdBlock(dataPage);
            dataPage = dataPage.getColumns(expectedColumnIndexes);

            return dataPage;
        }
        catch (RuntimeException e) {
            closeWithSuppression(e);
            throwIfInstanceOf(e, TrinoException.class);
            throw new TrinoException(ICEBERG_BAD_DATA, e);
        }
    }

    /**
     * The $row_id column used for updates is a composite column of at least one other column in the Page.
     * The indexes of the columns needed for the $row_id are in the updateRowIdChildColumnIndexes array.
     *
     * @param page The raw Page from the Parquet/ORC reader.
     * @return A Page where the $row_id channel has been populated.
     */
    private Page withRowIdBlock(Page page)
    {
        if (rowIdColumnIndex == -1) {
            return page;
        }

        RowBlock rowIdBlock = rowIdBlockFactory.apply(page.getBlock(rowIdColumnIndex));
        Block[] fullPage = new Block[page.getChannelCount()];
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            fullPage[channel] = channel == rowIdColumnIndex ? rowIdBlock : page.getBlock(channel);
        }
        return new Page(page.getPositionCount(), fullPage);
    }

    @Override
    public void close()
    {
        try {
            delegate.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString()
    {
        return delegate.toString();
    }

    @Override
    public long getMemoryUsage()
    {
        return delegate.getMemoryUsage();
    }

    @Override
    public Metrics getMetrics()
    {
        return delegate.getMetrics();
    }

    protected void closeWithSuppression(Throwable throwable)
    {
        closeAllSuppress(throwable, this);
    }
}
