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
package io.trino.lance.file.v2.reader;

import io.trino.lance.file.LanceDataSource;
import io.trino.lance.file.v2.metadata.ColumnMetadata;
import io.trino.lance.file.v2.metadata.Field;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class StructColumnReader
        implements ColumnReader
{
    private final ColumnReader[] childColumnReaders;
    private int nextBatchSize;

    public StructColumnReader(LanceDataSource dataSource, Field field, Map<Integer, ColumnMetadata> columnMetadata, List<Range> ranges, AggregatedMemoryContext memoryContext)
    {
        ColumnReader[] childReaders = new ColumnReader[field.children().size()];
        for (int i = 0; i < childReaders.length; i++) {
            childReaders[i] = ColumnReader.createColumnReader(dataSource, field.children().get(i), columnMetadata, ranges, memoryContext);
        }
        this.childColumnReaders = childReaders;
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        for (ColumnReader childColumnReader : childColumnReaders) {
            childColumnReader.prepareNextRead(batchSize);
        }
        nextBatchSize = batchSize;
    }

    @Override
    public DecodedPage read()
    {
        List<DecodedPage> decodedPages = Arrays.stream(childColumnReaders).map(ColumnReader::read).collect(toImmutableList());

        Block[] fieldBlocks = decodedPages.stream().map(DecodedPage::block).toArray(Block[]::new);
        // repetition/definition levels should be identical across all children
        RepetitionDefinitionUnraveler unraveler = decodedPages.getFirst().unraveler();
        if (unraveler.isAllValid()) {
            unraveler.skipValidity();
            return new DecodedPage(RowBlock.fromFieldBlocks(nextBatchSize, fieldBlocks), unraveler);
        }
        return new DecodedPage(RowBlock.fromNotNullSuppressedFieldBlocks(nextBatchSize, unraveler.calculateNulls(), fieldBlocks), unraveler);
    }
}
