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
import io.trino.plugin.hive.RollbackAction;
import io.trino.plugin.hive.SortingFileWriter;
import io.trino.plugin.hive.SortingFileWriter.DerivedSortColumn;
import io.trino.plugin.iceberg.PartitionTransforms.ColumnTransform;
import io.trino.spi.Page;
import io.trino.spi.PageSorter;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

import java.util.List;

import static io.trino.plugin.iceberg.PartitionTransforms.getColumnTransform;
import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static java.util.Objects.requireNonNull;

public final class IcebergSortingFileWriter
        implements IcebergFileWriter
{
    private final IcebergFileWriter outputWriter;
    private final SortingFileWriter sortingFileWriter;

    public IcebergSortingFileWriter(
            TrinoFileSystem fileSystem,
            Location tempFilePrefix,
            IcebergFileWriter outputWriter,
            DataSize maxMemory,
            int maxOpenTempFiles,
            List<Type> types,
            List<List<Integer>> sortIndexPaths,
            List<TrinoSortField> sortFields,
            PageSorter pageSorter,
            TypeOperators typeOperators)
    {
        this.outputWriter = requireNonNull(outputWriter, "outputWriter is null");

        ImmutableList.Builder<DerivedSortColumn> derivedSortColumns = ImmutableList.builder();
        ImmutableList.Builder<Integer> sortChannels = ImmutableList.builder();
        ImmutableList.Builder<SortOrder> sortOrders = ImmutableList.builder();
        int nextChannel = types.size();
        for (int i = 0; i < sortIndexPaths.size(); i++) {
            List<Integer> path = sortIndexPaths.get(i);
            TrinoSortField sortField = sortFields.get(i);
            sortOrders.add(sortField.sortOrder());

            boolean identity = sortField.transform().equals("identity");
            if (path.size() == 1 && identity) {
                sortChannels.add(path.getFirst());
                continue;
            }

            ColumnTransform transform = getColumnTransform(sortField.transform(), getNestedType(types, path));
            derivedSortColumns.add(new DerivedSortColumn(transform.type(), page -> transform.blockTransform().apply(getNestedBlock(page, path)), !identity));
            sortChannels.add(nextChannel++);
        }

        this.sortingFileWriter = new SortingFileWriter(
                fileSystem,
                tempFilePrefix,
                outputWriter,
                maxMemory,
                maxOpenTempFiles,
                types,
                sortChannels.build(),
                sortOrders.build(),
                pageSorter,
                typeOperators,
                derivedSortColumns.build());
    }

    private static Type getNestedType(List<Type> types, List<Integer> path)
    {
        Type type = types.get(path.getFirst());
        for (int depth = 1; depth < path.size(); depth++) {
            type = type.getTypeParameters().get(path.get(depth));
        }
        return type;
    }

    private static Block getNestedBlock(Page page, List<Integer> path)
    {
        Block block = page.getBlock(path.getFirst());
        for (int depth = 1; depth < path.size(); depth++) {
            block = getRowFieldsFromBlock(block).get(path.get(depth));
        }
        return block;
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
        sortingFileWriter.appendRows(dataPage);
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
}
