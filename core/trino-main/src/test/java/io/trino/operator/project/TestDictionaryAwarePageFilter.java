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
package io.trino.operator.project;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorSession;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.stream.IntStream;

import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestDictionaryAwarePageFilter
{
    @Test
    public void testDelegateMethods()
    {
        DictionaryAwarePageFilter filter = new DictionaryAwarePageFilter(new TestDictionaryFilter(true));
        assertEquals(filter.isDeterministic(), true);
        assertEquals(filter.getInputChannels().getInputChannels(), ImmutableList.of(3));
    }

    @Test
    public void testSimpleBlock()
    {
        Block block = createLongSequenceBlock(0, 100);
        testFilter(block, LongArrayBlock.class);
    }

    @Test
    public void testRleBlock()
    {
        testRleBlock(true);
        testRleBlock(false);
    }

    private static void testRleBlock(boolean filterRange)
    {
        DictionaryAwarePageFilter filter = createDictionaryAwarePageFilter(filterRange, LongArrayBlock.class);
        RunLengthEncodedBlock match = (RunLengthEncodedBlock) RunLengthEncodedBlock.create(createLongSequenceBlock(4, 5), 100);
        testFilter(filter, match, filterRange);
        RunLengthEncodedBlock noMatch = (RunLengthEncodedBlock) RunLengthEncodedBlock.create(createLongSequenceBlock(0, 1), 100);
        testFilter(filter, noMatch, filterRange);
    }

    @Test
    public void testRleBlockWithFailure()
    {
        DictionaryAwarePageFilter filter = createDictionaryAwarePageFilter(true, LongArrayBlock.class);
        RunLengthEncodedBlock fail = (RunLengthEncodedBlock) RunLengthEncodedBlock.create(createLongSequenceBlock(-10, -9), 100);
        assertThatThrownBy(() -> testFilter(filter, fail, true))
                .isInstanceOf(NegativeValueException.class)
                .hasMessage("value is negative: -10");
    }

    @Test
    public void testDictionaryBlock()
    {
        // match some
        testFilter(createDictionaryBlock(20, 100), LongArrayBlock.class);

        // match none
        testFilter(createDictionaryBlock(20, 0), LongArrayBlock.class);

        // match all
        testFilter(DictionaryBlock.create(100, createLongSequenceBlock(4, 5), new int[100]), LongArrayBlock.class);
    }

    @Test
    public void testDictionaryBlockWithFailure()
    {
        assertThatThrownBy(() -> testFilter(createDictionaryBlockWithFailure(20, 100), LongArrayBlock.class))
                .isInstanceOf(NegativeValueException.class)
                .hasMessage("value is negative: -10");
    }

    @Test
    public void testDictionaryBlockProcessingWithUnusedFailure()
    {
        // match some
        testFilter(createDictionaryBlockWithUnusedEntries(20, 100), DictionaryBlock.class);

        // match none, blockSize must be > 1 to actually create a DictionaryBlock
        testFilter(createDictionaryBlockWithUnusedEntries(20, 2), DictionaryBlock.class);

        // match all
        testFilter(DictionaryBlock.create(100, createLongsBlock(4, 5, -1), new int[100]), DictionaryBlock.class);
    }

    @Test
    public void testDictionaryProcessingEnableDisable()
    {
        TestDictionaryFilter nestedFilter = new TestDictionaryFilter(true);
        DictionaryAwarePageFilter filter = new DictionaryAwarePageFilter(nestedFilter);

        Block ineffectiveBlock = createDictionaryBlock(100, 20);
        Block effectiveBlock = createDictionaryBlock(10, 100);
        Block anotherIneffectiveBlock = createDictionaryBlock(100, 25);

        // function will always process the first dictionary
        nestedFilter.setExpectedType(LongArrayBlock.class);
        testFilter(filter, ineffectiveBlock, true);

        // last dictionary not effective and incoming dictionary is also ineffective, so dictionary processing is disabled
        nestedFilter.setExpectedType(DictionaryBlock.class);
        testFilter(filter, anotherIneffectiveBlock, true);

        for (int i = 0; i < 5; i++) {
            // Increase usage count of large ineffective dictionary with multiple pages of small positions count
            testFilter(filter, anotherIneffectiveBlock, true);
        }
        // last dictionary effective, so dictionary processing is enabled
        nestedFilter.setExpectedType(LongArrayBlock.class);
        testFilter(filter, ineffectiveBlock, true);

        // last dictionary not effective, but incoming dictionary is effective, so dictionary processing stays enabled
        nestedFilter.setExpectedType(LongArrayBlock.class);
        testFilter(filter, effectiveBlock, true);
    }

    private static Block createDictionaryBlock(int dictionarySize, int blockSize)
    {
        Block dictionary = createLongSequenceBlock(0, dictionarySize);
        int[] ids = new int[blockSize];
        Arrays.setAll(ids, index -> index % dictionarySize);
        return DictionaryBlock.create(ids.length, dictionary, ids);
    }

    private static Block createDictionaryBlockWithFailure(int dictionarySize, int blockSize)
    {
        Block dictionary = createLongSequenceBlock(-10, dictionarySize - 10);
        int[] ids = new int[blockSize];
        Arrays.setAll(ids, index -> index % dictionarySize);
        return DictionaryBlock.create(ids.length, dictionary, ids);
    }

    private static Block createDictionaryBlockWithUnusedEntries(int dictionarySize, int blockSize)
    {
        Block dictionary = createLongSequenceBlock(-10, dictionarySize);
        int[] ids = new int[blockSize];
        Arrays.setAll(ids, index -> (index % dictionarySize) + 10);
        return DictionaryBlock.create(ids.length, dictionary, ids);
    }

    private static void testFilter(Block block, Class<? extends Block> expectedType)
    {
        testFilter(block, true, expectedType);
        testFilter(block, false, expectedType);
        testFilter(lazyWrapper(block), true, expectedType);
        testFilter(lazyWrapper(block), false, expectedType);
    }

    private static void testFilter(Block block, boolean filterRange, Class<? extends Block> expectedType)
    {
        DictionaryAwarePageFilter filter = createDictionaryAwarePageFilter(filterRange, expectedType);
        testFilter(filter, block, filterRange);
        // exercise dictionary caching code
        testFilter(filter, block, filterRange);
    }

    private static DictionaryAwarePageFilter createDictionaryAwarePageFilter(boolean filterRange, Class<? extends Block> expectedType)
    {
        return new DictionaryAwarePageFilter(new TestDictionaryFilter(filterRange, expectedType));
    }

    private static void testFilter(DictionaryAwarePageFilter filter, Block block, boolean filterRange)
    {
        IntSet actualSelectedPositions = toSet(filter.filter(null, new Page(block)));

        block = block.getLoadedBlock();

        IntSet expectedSelectedPositions = new IntArraySet(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (isSelected(filterRange, block.getLong(position, 0))) {
                expectedSelectedPositions.add(position);
            }
        }
        assertEquals(actualSelectedPositions, expectedSelectedPositions);
    }

    private static IntSet toSet(SelectedPositions selectedPositions)
    {
        int start = selectedPositions.getOffset();
        int end = start + selectedPositions.size();
        if (selectedPositions.isList()) {
            return new IntArraySet(Arrays.copyOfRange(selectedPositions.getPositions(), start, end));
        }
        return new IntArraySet(IntStream.range(start, end).toArray());
    }

    private static LazyBlock lazyWrapper(Block block)
    {
        return new LazyBlock(block.getPositionCount(), () -> block);
    }

    private static boolean isSelected(boolean filterRange, long value)
    {
        if (value < 0) {
            throw new IllegalArgumentException("value is negative: " + value);
        }

        boolean selected;
        if (filterRange) {
            selected = value > 3 && value < 11;
        }
        else {
            selected = value % 3 == 1;
        }
        return selected;
    }

    /**
     * Filter for the dictionary.  This will fail if the input block is a DictionaryBlock
     */
    private static class TestDictionaryFilter
            implements PageFilter
    {
        private final boolean filterRange;
        private Class<? extends Block> expectedType;

        public TestDictionaryFilter(boolean filterRange)
        {
            this.filterRange = filterRange;
        }

        public TestDictionaryFilter(boolean filterRange, Class<? extends Block> expectedType)
        {
            this.filterRange = filterRange;
            this.expectedType = expectedType;
        }

        public void setExpectedType(Class<? extends Block> expectedType)
        {
            this.expectedType = expectedType;
        }

        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public InputChannels getInputChannels()
        {
            return new InputChannels(3);
        }

        @Override
        public SelectedPositions filter(ConnectorSession session, Page page)
        {
            assertEquals(page.getChannelCount(), 1);
            Block block = page.getBlock(0);

            boolean sequential = true;
            IntArrayList selectedPositions = new IntArrayList();
            for (int position = 0; position < block.getPositionCount(); position++) {
                long value = block.getLong(position, 0);
                verifyPositive(value);

                boolean selected = isSelected(filterRange, value);
                if (selected) {
                    if (sequential && !selectedPositions.isEmpty()) {
                        sequential = (position == selectedPositions.getInt(selectedPositions.size() - 1) + 1);
                    }
                    selectedPositions.add(position);
                }
            }

            // verify the input block is the expected type (this is to assure that
            // dictionary processing enabled and disabled as expected)
            // this check is performed last so that dictionary processing that fails
            // is not checked (only the fall back processing is checked)
            assertInstanceOf(block, expectedType);

            if (selectedPositions.isEmpty()) {
                return SelectedPositions.positionsRange(0, 0);
            }
            if (sequential) {
                return SelectedPositions.positionsRange(selectedPositions.getInt(0), selectedPositions.size());
            }
            // add 3 invalid elements to the head and tail
            for (int i = 0; i < 3; i++) {
                selectedPositions.add(0, -1);
                selectedPositions.add(-1);
            }

            return SelectedPositions.positionsList(selectedPositions.elements(), 3, selectedPositions.size() - 6);
        }

        private static long verifyPositive(long value)
        {
            if (value < 0) {
                throw new NegativeValueException(value);
            }
            return value;
        }
    }

    private static class NegativeValueException
            extends RuntimeException
    {
        public NegativeValueException(long value)
        {
            super("value is negative: " + value);
        }
    }
}
