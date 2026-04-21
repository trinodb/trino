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
package io.trino.operator.output;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.predicate.Utils;
import io.trino.type.BlockTypeOperators;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.block.BlockAssertions.createRandomBlockForType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPositionsAppenderPageBuilder
{
    @Test
    public void testFullOnPositionCountLimit()
    {
        int maxPageBytes = 1024 * 1024;
        int maxDirectSize = maxPageBytes * 10;
        PositionsAppenderPageBuilder pageBuilder = PositionsAppenderPageBuilder.withMaxPageSize(
                maxPageBytes,
                maxDirectSize,
                List.of(VARCHAR),
                new PositionsAppenderFactory(new BlockTypeOperators()));

        RunLengthEncodedBlock rleBlock = (RunLengthEncodedBlock) RunLengthEncodedBlock.create(VARCHAR, Slices.utf8Slice("test"), 10);
        Page inputPage = new Page(rleBlock);

        IntArrayList positions = IntArrayList.wrap(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        // Append 32760 positions, just less than MAX_POSITION_COUNT
        assertThat(PositionsAppenderPageBuilder.MAX_POSITION_COUNT)
                .as("expected MAX_POSITION_COUNT to be 32768")
                .isEqualTo(32768);
        for (int i = 0; i < 3276; i++) {
            pageBuilder.appendToOutputPartition(inputPage, positions);
        }
        assertThat(pageBuilder.isFull())
                .as("pageBuilder should still not be full")
                .isFalse();
        // Append 10 more positions, crossing the threshold on position count
        pageBuilder.appendToOutputPartition(inputPage, positions);
        assertThat(pageBuilder.isFull())
                .as("pageBuilder should be full")
                .isTrue();
        PositionsAppenderSizeAccumulator sizeAccumulator = pageBuilder.computeAppenderSizes();
        assertThat(sizeAccumulator.getSizeInBytes()).isEqualTo(rleBlock.getValue().getSizeInBytes());
        assertThat(sizeAccumulator.getDirectSizeInBytes() < maxDirectSize)
                .as("direct size should still be below threshold")
                .isTrue();
        assertThat(pageBuilder.getSizeInBytes())
                .as("pageBuilder sizeInBytes must match sizeAccumulator value")
                .isEqualTo(sizeAccumulator.getSizeInBytes());
    }

    @Test
    public void testFullOnDirectSizeInBytes()
    {
        int maxPageBytes = 100;
        int maxDirectSize = 1000;
        PositionsAppenderPageBuilder pageBuilder = PositionsAppenderPageBuilder.withMaxPageSize(
                maxPageBytes,
                maxDirectSize,
                List.of(VARCHAR),
                new PositionsAppenderFactory(new BlockTypeOperators()));

        PositionsAppenderSizeAccumulator sizeAccumulator = pageBuilder.computeAppenderSizes();
        assertThat(sizeAccumulator.getSizeInBytes()).isEqualTo(0L);
        assertThat(sizeAccumulator.getDirectSizeInBytes()).isEqualTo(0L);
        assertThat(pageBuilder.isFull()).isFalse();

        RunLengthEncodedBlock rleBlock = (RunLengthEncodedBlock) RunLengthEncodedBlock.create(VARCHAR, Slices.utf8Slice("test"), 10);
        Page inputPage = new Page(rleBlock);

        IntArrayList positions = IntArrayList.wrap(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        pageBuilder.appendToOutputPartition(inputPage, positions);
        // 10 positions inserted, size in bytes is still the same since we're in RLE mode but direct size is 10x
        sizeAccumulator = pageBuilder.computeAppenderSizes();
        assertThat(sizeAccumulator.getSizeInBytes()).isEqualTo(rleBlock.getValue().getSizeInBytes());
        assertThat(pageBuilder.getSizeInBytes())
                .as("pageBuilder sizeInBytes must match sizeAccumulator value")
                .isEqualTo(sizeAccumulator.getSizeInBytes());
        assertThat(sizeAccumulator.getDirectSizeInBytes()).isEqualTo(rleBlock.getValue().getSizeInBytes() * 10);
        assertThat(pageBuilder.isFull()).isFalse();

        // Keep inserting until the direct size limit is reached
        while (pageBuilder.computeAppenderSizes().getDirectSizeInBytes() < maxDirectSize) {
            pageBuilder.appendToOutputPartition(inputPage, positions);
        }
        // size in bytes is unchanged
        sizeAccumulator = pageBuilder.computeAppenderSizes();
        assertThat(sizeAccumulator.getSizeInBytes())
                .as("sizeInBytes must still report the RLE block size only")
                .isEqualTo(rleBlock.getValue().getSizeInBytes());
        assertThat(pageBuilder.getSizeInBytes())
                .as("pageBuilder sizeInBytes must match sizeAccumulator value")
                .isEqualTo(sizeAccumulator.getSizeInBytes());
        // builder reports full due to maximum size in bytes reached
        assertThat(pageBuilder.isFull()).isTrue();
        Page result = pageBuilder.build();
        assertThat(result.getPositionCount())
                .as("result positions should be below the 8192 maximum")
                .isEqualTo(120);
        assertThat(result.getBlock(0) instanceof RunLengthEncodedBlock)
                .as("result block is RLE encoded")
                .isTrue();
    }

    @Test
    public void testFlushUsefulDictionariesOnRelease()
    {
        int maxPageBytes = 100;
        int maxDirectSize = 1000;
        PositionsAppenderPageBuilder pageBuilder = PositionsAppenderPageBuilder.withMaxPageSize(
                maxPageBytes,
                maxDirectSize,
                List.of(VARCHAR),
                new PositionsAppenderFactory(new BlockTypeOperators()));

        Block valueBlock = Utils.nativeValueToBlock(VARCHAR, Slices.utf8Slice("test"));
        Block dictionaryBlock = DictionaryBlock.create(10, valueBlock, new int[10]);
        Page inputPage = new Page(dictionaryBlock);

        pageBuilder.appendToOutputPartition(inputPage, IntArrayList.wrap(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
        // Dictionary mode appender should report the size of the ID's, but doesn't currently track
        // the per-position size at all because it would be inefficient
        assertThat(pageBuilder.getSizeInBytes()).isEqualTo(Integer.BYTES * 10);
        assertThat(pageBuilder.isFull()).isFalse();

        Optional<Page> flushedPage = pageBuilder.flushOrFlattenBeforeRelease();
        assertThat(flushedPage.isPresent())
                .as("pageBuilder should force flush the dictionary")
                .isTrue();
        assertThat(flushedPage.get().getBlock(0) instanceof DictionaryBlock)
                .as("result should be dictionary encoded")
                .isTrue();
    }

    @Test
    public void testFlattenUnhelpfulDictionariesOnRelease()
    {
        // Create unhelpful dictionary wrapping
        Block valueBlock = createRandomBlockForType(VARCHAR, 10, 0.25f);
        Block dictionaryBlock = DictionaryBlock.create(10, valueBlock, new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        Page inputPage = new Page(dictionaryBlock);

        // Ensure the builder allows the entire value block to be inserted without being full
        int maxPageBytes = toIntExact(valueBlock.getSizeInBytes() * 10);
        int maxDirectSize = maxPageBytes * 10;
        PositionsAppenderPageBuilder pageBuilder = PositionsAppenderPageBuilder.withMaxPageSize(
                maxPageBytes,
                maxDirectSize,
                List.of(VARCHAR),
                new PositionsAppenderFactory(new BlockTypeOperators()));

        pageBuilder.appendToOutputPartition(inputPage, IntArrayList.wrap(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
        assertThat(pageBuilder.getSizeInBytes()).isEqualTo(Integer.BYTES * 10);
        assertThat(pageBuilder.isFull()).isFalse();

        assertThat(pageBuilder.flushOrFlattenBeforeRelease())
                .as("pageBuilder should not force a flush")
                .isEqualTo(Optional.empty());
        assertThat(pageBuilder.isFull()).isFalse();
        assertThat(pageBuilder.getSizeInBytes())
                .as("pageBuilder should have transitioned to direct mode").isEqualTo(valueBlock.getSizeInBytes());

        Page result = pageBuilder.build();
        assertThat(result.getBlock(0) instanceof ValueBlock)
                .as("result should not be a dictionary block")
                .isTrue();
    }
}
