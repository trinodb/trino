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
import io.trino.lance.file.v2.reader.RepetitionDefinitionUnraveler.BlockPositions;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class ListColumnReader
        implements ColumnReader
{
    private final ColumnReader childColumnReader;

    private int nextBatchSize;

    public ListColumnReader(LanceDataSource dataSource,
            Field field,
            Map<Integer, ColumnMetadata> columnMetadata,
            List<Range> ranges,
            AggregatedMemoryContext memoryContext)
    {
        requireNonNull(field, "field is null");
        checkArgument(field.children().size() == 1, "list should have only one child filed");
        this.childColumnReader = ColumnReader.createColumnReader(dataSource, field.children().getFirst(), columnMetadata, ranges, memoryContext);
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        childColumnReader.prepareNextRead(batchSize);
        nextBatchSize = batchSize;
    }

    @Override
    public DecodedPage read()
    {
        DecodedPage decodedChild = childColumnReader.read();
        RepetitionDefinitionUnraveler unraveler = decodedChild.unraveler();
        BlockPositions positions = unraveler.calculateOffsets();
        verify(nextBatchSize == positions.offsets().length - 1);
        Block arrayBlock = ArrayBlock.fromElementBlock(nextBatchSize, positions.nulls(), positions.offsets(), decodedChild.block());
        return new DecodedPage(arrayBlock, unraveler);
    }
}
