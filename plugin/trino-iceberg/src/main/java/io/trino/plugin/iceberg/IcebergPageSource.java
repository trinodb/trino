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
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.plugin.hive.ReaderProjectionsAdapter;
import io.trino.plugin.iceberg.delete.IcebergPositionDeletePageSink;
import io.trino.plugin.iceberg.delete.RowPredicate;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.UpdatablePageSource;
import io.trino.spi.metrics.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class IcebergPageSource
        implements UpdatablePageSource
{
    private final Schema schema;
    private final int[] expectedColumnIndexes;
    private final ConnectorPageSource delegate;
    private final Optional<ReaderProjectionsAdapter> projectionsAdapter;
    private final Optional<RowPredicate> deletePredicate;
    private final Supplier<IcebergPositionDeletePageSink> positionDeleteSinkSupplier;
    private final Supplier<IcebergPageSink> updatedRowPageSinkSupplier;
    // An array with one element per field in the $row_id column. The value in the array points to the
    // channel where the data can be read from.
    private int[] updateRowIdChildColumnIndexes = new int[0];
    // The $row_id's index in 'expectedColumns', or -1 if there isn't one
    private int updateRowIdColumnIndex = -1;
    // Maps the Iceberg field ids of unmodified columns to their indexes in updateRowIdChildColumnIndexes
    private Map<Integer, Integer> icebergIdToRowIdColumnIndex = ImmutableMap.of();
    // Maps the Iceberg field ids of modified columns to their indexes in the updateColumns columnValueAndRowIdChannels array
    private Map<Integer, Integer> icebergIdToUpdatedColumnIndex = ImmutableMap.of();

    @Nullable
    private IcebergPositionDeletePageSink positionDeleteSink;
    @Nullable
    private IcebergPageSink updatedRowPageSink;

    public IcebergPageSource(
            Schema schema,
            List<IcebergColumnHandle> expectedColumns,
            List<IcebergColumnHandle> requiredColumns,
            ConnectorPageSource delegate,
            Optional<ReaderProjectionsAdapter> projectionsAdapter,
            Optional<RowPredicate> deletePredicate,
            Supplier<IcebergPositionDeletePageSink> positionDeleteSinkSupplier,
            Supplier<IcebergPageSink> updatedRowPageSinkSupplier,
            List<IcebergColumnHandle> updatedColumns)
    {
        this.schema = requireNonNull(schema, "schema is null");
        // expectedColumns should contain columns which should be in the final Page
        // requiredColumns should include all expectedColumns as well as any columns needed by the DeleteFilter
        requireNonNull(expectedColumns, "expectedColumns is null");
        requireNonNull(requiredColumns, "requiredColumns is null");
        this.expectedColumnIndexes = new int[expectedColumns.size()];
        for (int i = 0; i < expectedColumns.size(); i++) {
            IcebergColumnHandle expectedColumn = expectedColumns.get(i);
            checkArgument(expectedColumn.equals(requiredColumns.get(i)), "Expected columns must be a prefix of required columns");
            expectedColumnIndexes[i] = i;

            if (expectedColumn.isUpdateRowIdColumn()) {
                this.updateRowIdColumnIndex = i;

                Map<Integer, Integer> fieldIdToColumnIndex = mapFieldIdsToIndex(requiredColumns);
                List<ColumnIdentity> rowIdFields = expectedColumn.getColumnIdentity().getChildren();
                ImmutableMap.Builder<Integer, Integer> fieldIdToRowIdIndex = ImmutableMap.builder();
                this.updateRowIdChildColumnIndexes = new int[rowIdFields.size()];
                for (int columnIndex = 0; columnIndex < rowIdFields.size(); columnIndex++) {
                    int fieldId = rowIdFields.get(columnIndex).getId();
                    updateRowIdChildColumnIndexes[columnIndex] = requireNonNull(fieldIdToColumnIndex.get(fieldId), () -> format("Column %s not found in requiredColumns", fieldId));
                    fieldIdToRowIdIndex.put(fieldId, columnIndex);
                }
                this.icebergIdToRowIdColumnIndex = fieldIdToRowIdIndex.buildOrThrow();
            }
        }

        this.delegate = requireNonNull(delegate, "delegate is null");
        this.projectionsAdapter = requireNonNull(projectionsAdapter, "projectionsAdapter is null");
        this.deletePredicate = requireNonNull(deletePredicate, "deletePredicate is null");
        this.positionDeleteSinkSupplier = requireNonNull(positionDeleteSinkSupplier, "positionDeleteSinkSupplier is null");
        this.updatedRowPageSinkSupplier = requireNonNull(updatedRowPageSinkSupplier, "updatedRowPageSinkSupplier is null");
        requireNonNull(updatedColumns, "updatedColumnFieldIds is null");
        if (!updatedColumns.isEmpty()) {
            this.icebergIdToUpdatedColumnIndex = mapFieldIdsToIndex(updatedColumns);
        }
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
    public Page getNextPage()
    {
        try {
            Page dataPage = delegate.getNextPage();
            if (dataPage == null) {
                return null;
            }

            if (deletePredicate.isPresent()) {
                dataPage = deletePredicate.get().filterPage(dataPage);
            }

            if (projectionsAdapter.isPresent()) {
                dataPage = projectionsAdapter.get().adaptPage(dataPage);
            }

            dataPage = setUpdateRowIdBlock(dataPage);
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
     * @param page The raw Page from the Parquet/ORC reader.
     * @return A Page where the $row_id channel has been populated.
     */
    private Page setUpdateRowIdBlock(Page page)
    {
        if (updateRowIdColumnIndex == -1) {
            return page;
        }

        Block[] rowIdFields = new Block[updateRowIdChildColumnIndexes.length];
        for (int childIndex = 0; childIndex < updateRowIdChildColumnIndexes.length; childIndex++) {
            rowIdFields[childIndex] = page.getBlock(updateRowIdChildColumnIndexes[childIndex]);
        }

        Block[] fullPage = new Block[page.getChannelCount()];
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            if (channel == updateRowIdColumnIndex) {
                fullPage[channel] = RowBlock.fromFieldBlocks(page.getPositionCount(), Optional.empty(), rowIdFields);
                continue;
            }

            fullPage[channel] = page.getBlock(channel);
        }

        return new Page(page.getPositionCount(), fullPage);
    }

    @Override
    public void deleteRows(Block rowIds)
    {
        if (positionDeleteSink == null) {
            positionDeleteSink = positionDeleteSinkSupplier.get();
            verify(positionDeleteSink != null);
        }
        positionDeleteSink.appendPage(new Page(rowIds)).join();
    }

    @Override
    public void updateRows(Page page, List<Integer> columnValueAndRowIdChannels)
    {
        int rowIdChannel = columnValueAndRowIdChannels.get(columnValueAndRowIdChannels.size() - 1);
        List<Integer> columnChannelMapping = columnValueAndRowIdChannels.subList(0, columnValueAndRowIdChannels.size() - 1);

        if (positionDeleteSink == null) {
            positionDeleteSink = positionDeleteSinkSupplier.get();
            verify(positionDeleteSink != null);
        }
        if (updatedRowPageSink == null) {
            updatedRowPageSink = updatedRowPageSinkSupplier.get();
            verify(updatedRowPageSink != null);
        }

        ColumnarRow rowIdColumns = ColumnarRow.toColumnarRow(page.getBlock(rowIdChannel));
        positionDeleteSink.appendPage(new Page(rowIdColumns.getField(0))).join();

        List<Types.NestedField> columns = schema.columns();
        Block[] fullPage = new Block[columns.size()];
        Set<Integer> updatedColumnFieldIds = icebergIdToUpdatedColumnIndex.keySet();

        for (int targetChannel = 0; targetChannel < columns.size(); targetChannel++) {
            Types.NestedField column = columns.get(targetChannel);
            if (updatedColumnFieldIds.contains(column.fieldId())) {
                fullPage[targetChannel] = page.getBlock(columnChannelMapping.get(icebergIdToUpdatedColumnIndex.get(column.fieldId())));
            }
            else {
                // Plus one because the first field is the row position column
                fullPage[targetChannel] = rowIdColumns.getField(icebergIdToRowIdColumnIndex.get(column.fieldId()));
            }
        }

        updatedRowPageSink.appendPage(new Page(page.getPositionCount(), fullPage)).join();
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        CompletableFuture<Collection<Slice>> fragments = CompletableFuture.completedFuture(ImmutableList.of());
        BiFunction<Collection<Slice>, Collection<Slice>, Collection<Slice>> combineSliceCollections = (collection1, collection2) ->
                ImmutableList.<Slice>builder().addAll(collection1).addAll(collection2).build();

        if (positionDeleteSink != null) {
            fragments = fragments.thenCombine(positionDeleteSink.finish(), combineSliceCollections);
        }
        if (updatedRowPageSink != null) {
            fragments = fragments.thenCombine(updatedRowPageSink.finish(), combineSliceCollections);
        }
        return fragments;
    }

    @Override
    public void abort()
    {
        if (positionDeleteSink != null) {
            positionDeleteSink.abort();
        }
        if (updatedRowPageSink != null) {
            updatedRowPageSink.abort();
        }
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
        long memoryUsage = delegate.getMemoryUsage();
        if (positionDeleteSink != null) {
            memoryUsage += positionDeleteSink.getMemoryUsage();
        }
        return memoryUsage;
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

    private static Map<Integer, Integer> mapFieldIdsToIndex(List<IcebergColumnHandle> columns)
    {
        ImmutableMap.Builder<Integer, Integer> fieldIdsToIndex = ImmutableMap.builder();
        for (int i = 0; i < columns.size(); i++) {
            fieldIdsToIndex.put(columns.get(i).getId(), i);
        }
        return fieldIdsToIndex.buildOrThrow();
    }
}
