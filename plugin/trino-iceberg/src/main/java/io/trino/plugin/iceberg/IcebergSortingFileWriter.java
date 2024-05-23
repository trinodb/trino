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
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.RollbackAction;
import io.trino.plugin.hive.SortingFileWriter;
import io.trino.spi.Page;
import io.trino.spi.PageSorter;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static java.util.Objects.requireNonNull;

public final class IcebergSortingFileWriter
        implements IcebergFileWriter
{
    private final IcebergFileWriter outputWriter;
    private final SortingFileWriter sortingFileWriter;
    private final int originalChannelCount;
    // Per sort field: index path within the page block hierarchy to the leaf block
    private final List<List<Integer>> sortIndexPaths;

    public IcebergSortingFileWriter(
            TrinoFileSystem fileSystem,
            Location tempFilePrefix,
            IcebergFileWriter outputWriter,
            DataSize maxMemory,
            int maxOpenTempFiles,
            List<Type> types,
            List<List<Integer>> sortIndexPaths,
            List<SortOrder> sortOrders,
            PageSorter pageSorter,
            TypeOperators typeOperators)
    {
        this.outputWriter = requireNonNull(outputWriter, "outputWriter is null");
        this.originalChannelCount = types.size();
        this.sortIndexPaths = ImmutableList.copyOf(sortIndexPaths);

        // Determine leaf type for each sort field by walking the type hierarchy along the index path
        ImmutableList.Builder<Type> expandedTypes = ImmutableList.builder();
        expandedTypes.addAll(types);
        for (List<Integer> indexPath : sortIndexPaths) {
            Type type = types.get(indexPath.getFirst());
            for (int i = 1; i < indexPath.size(); i++) {
                type = type.getTypeParameters().get(indexPath.get(i));
            }
            expandedTypes.add(type);
        }
        List<Type> allTypes = expandedTypes.build();

        // Sort channels are the appended projected columns
        List<Integer> sortChannels = IntStream.range(originalChannelCount, allTypes.size())
                .boxed()
                .collect(toImmutableList());

        this.sortingFileWriter = new SortingFileWriter(
                fileSystem,
                tempFilePrefix,
                new StrippingFileWriter(outputWriter, originalChannelCount),
                maxMemory,
                maxOpenTempFiles,
                allTypes,
                sortChannels,
                sortOrders,
                pageSorter,
                typeOperators);
    }

    @Override
    public FileMetrics getFileMetrics()
    {
        return outputWriter.getFileMetrics();
    }

    @Override
    public long getWrittenBytes()
    {
        return sortingFileWriter.getWrittenBytes();
    }

    @Override
    public long getMemoryUsage()
    {
        return sortingFileWriter.getMemoryUsage();
    }

    @Override
    public void appendRows(Page dataPage)
    {
        sortingFileWriter.appendRows(expandPage(dataPage));
    }

    @Override
    public RollbackAction commit()
    {
        return sortingFileWriter.commit();
    }

    @Override
    public void rollback()
    {
        sortingFileWriter.rollback();
    }

    @Override
    public long getValidationCpuNanos()
    {
        return sortingFileWriter.getValidationCpuNanos();
    }

    private Page expandPage(Page page)
    {
        if (sortIndexPaths.isEmpty()) {
            return page;
        }

        Block[] blocks = new Block[originalChannelCount + sortIndexPaths.size()];
        for (int i = 0; i < originalChannelCount; i++) {
            blocks[i] = page.getBlock(i);
        }
        for (int i = 0; i < sortIndexPaths.size(); i++) {
            List<Integer> indexPath = sortIndexPaths.get(i);
            Block block = page.getBlock(indexPath.getFirst());
            for (int depth = 1; depth < indexPath.size(); depth++) {
                block = getRowFieldsFromBlock(block).get(indexPath.get(depth));
            }
            blocks[originalChannelCount + i] = block;
        }
        return new Page(page.getPositionCount(), blocks);
    }

    private static final class StrippingFileWriter
            implements FileWriter
    {
        private final IcebergFileWriter delegate;
        private final int channelsToKeep;

        private StrippingFileWriter(IcebergFileWriter delegate, int channelsToKeep)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.channelsToKeep = channelsToKeep;
        }

        @Override
        public void appendRows(Page page)
        {
            delegate.appendRows(page.getColumns(IntStream.range(0, channelsToKeep).toArray()));
        }

        @Override
        public RollbackAction commit()
        {
            return delegate.commit();
        }

        @Override
        public void rollback()
        {
            delegate.rollback();
        }

        @Override
        public long getWrittenBytes()
        {
            return delegate.getWrittenBytes();
        }

        @Override
        public long getMemoryUsage()
        {
            return delegate.getMemoryUsage();
        }

        @Override
        public long getValidationCpuNanos()
        {
            return delegate.getValidationCpuNanos();
        }
    }
}
