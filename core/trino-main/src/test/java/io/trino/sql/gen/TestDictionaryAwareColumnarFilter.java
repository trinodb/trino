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
package io.trino.sql.gen;

import com.google.common.collect.ImmutableList;
import io.trino.FullConnectorSession;
import io.trino.operator.project.InputChannels;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.sql.gen.columnar.ColumnarFilter;
import io.trino.sql.gen.columnar.DictionaryAwareColumnarFilter;
import io.trino.testing.TestingSession;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;

import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDictionaryAwareColumnarFilter
{
    private static final FullConnectorSession FULL_CONNECTOR_SESSION = new FullConnectorSession(
            TestingSession.testSessionBuilder().build(),
            ConnectorIdentity.ofUser("test"));

    @Test
    public void testGetInputChannels()
    {
        DictionaryAwareColumnarFilter filter = new DictionaryAwareColumnarFilter(new ColumnarFilter() {
            @Override
            public int filterPositionsRange(ConnectorSession session, int[] outputPositions, int offset, int size, SourcePage loadedPage)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public int filterPositionsList(ConnectorSession session, int[] outputPositions, int[] activePositions, int offset, int size, SourcePage loadedPage)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public InputChannels getInputChannels()
            {
                return new InputChannels(3);
            }
        });
        assertThat(filter.getInputChannels().getInputChannels()).isEqualTo(ImmutableList.of(3));
    }

    @Test
    public void testSimpleBlock()
    {
        Block block = createLongSequenceBlock(0, 100);
        testFilter(block, LongArrayBlock.class);
    }

    @ParameterizedTest
    @MethodSource("io.trino.testing.DataProviders#trueFalse")
    public void testRleBlock(boolean usePositionsList)
    {
        testRleBlock(true, usePositionsList);
        testRleBlock(false, usePositionsList);
    }

    private static void testRleBlock(boolean filterRange, boolean usePositionsList)
    {
        DictionaryAwareColumnarFilter filter = createDictionaryAwareColumnarFilter(filterRange, LongArrayBlock.class);
        RunLengthEncodedBlock match = (RunLengthEncodedBlock) RunLengthEncodedBlock.create(createLongSequenceBlock(4, 5), 100);
        testFilter(filter, match, filterRange, usePositionsList);
        RunLengthEncodedBlock noMatch = (RunLengthEncodedBlock) RunLengthEncodedBlock.create(createLongSequenceBlock(0, 1), 100);
        testFilter(filter, noMatch, filterRange, usePositionsList);
    }

    @ParameterizedTest
    @MethodSource("io.trino.testing.DataProviders#trueFalse")
    public void testRleBlockWithFailure(boolean usePositionsList)
    {
        DictionaryAwareColumnarFilter filter = createDictionaryAwareColumnarFilter(true, LongArrayBlock.class);
        RunLengthEncodedBlock fail = (RunLengthEncodedBlock) RunLengthEncodedBlock.create(createLongSequenceBlock(-10, -9), 100);
        assertThatThrownBy(() -> testFilter(filter, fail, true, usePositionsList))
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
        testFilter(block, true, true, expectedType);
        testFilter(block, true, false, expectedType);
        testFilter(block, false, true, expectedType);
        testFilter(block, false, false, expectedType);
    }

    private static void testFilter(Block block, boolean selectRange, boolean usePositionsList, Class<? extends Block> expectedType)
    {
        DictionaryAwareColumnarFilter filter = createDictionaryAwareColumnarFilter(selectRange, expectedType);
        testFilter(filter, block, selectRange, usePositionsList);
        // exercise dictionary caching code
        testFilter(filter, block, selectRange, usePositionsList);
    }

    private static DictionaryAwareColumnarFilter createDictionaryAwareColumnarFilter(boolean selectRange, Class<? extends Block> expectedType)
    {
        return new DictionaryAwareColumnarFilter(new TestingDictionaryFilter(selectRange, expectedType));
    }

    private static void testFilter(DictionaryAwareColumnarFilter filter, Block block, boolean selectRange, boolean usePositionsList)
    {
        int[] outputPositions = new int[block.getPositionCount()];
        int outputPositionsCount;
        if (usePositionsList) {
            outputPositionsCount = filter.filterPositionsList(FULL_CONNECTOR_SESSION, outputPositions, toPositionsList(0, block.getPositionCount()), 0, block.getPositionCount(), SourcePage.create(block));
        }
        else {
            outputPositionsCount = filter.filterPositionsRange(FULL_CONNECTOR_SESSION, outputPositions, 0, block.getPositionCount(), SourcePage.create(block));
        }
        IntSet actualSelectedPositions = new IntArraySet(Arrays.copyOfRange(outputPositions, 0, outputPositionsCount));
        IntSet expectedSelectedPositions = new IntArraySet(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (isSelected(selectRange, BIGINT.getLong(block, position))) {
                expectedSelectedPositions.add(position);
            }
        }
        assertThat(actualSelectedPositions).isEqualTo(expectedSelectedPositions);
    }

    private static int[] toPositionsList(int offset, int length)
    {
        int[] positions = new int[length];
        for (int index = 0; index < length; index++) {
            positions[index] = offset + index;
        }
        return positions;
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
    private static class TestingDictionaryFilter
            implements ColumnarFilter
    {
        private final boolean selectRange;
        private Class<? extends Block> expectedType;

        public TestingDictionaryFilter(boolean selectRange, Class<? extends Block> expectedType)
        {
            this.selectRange = selectRange;
            this.expectedType = expectedType;
        }

        public void setExpectedType(Class<? extends Block> expectedType)
        {
            this.expectedType = expectedType;
        }

        @Override
        public InputChannels getInputChannels()
        {
            return new InputChannels(3);
        }

        @Override
        public int filterPositionsRange(ConnectorSession session, int[] outputPositions, int offset, int size, SourcePage loadedPage)
        {
            assertThat(loadedPage.getChannelCount()).isEqualTo(1);
            Block block = loadedPage.getBlock(0);

            int outputPositionsCount = 0;
            for (int position = offset; position < offset + size; position++) {
                long value = BIGINT.getLong(block, position);
                verifyPositive(value);

                boolean selected = isSelected(selectRange, value);
                if (selected) {
                    outputPositions[outputPositionsCount] = position;
                    outputPositionsCount++;
                }
            }

            // verify the input block is the expected type (this is to assure that
            // dictionary processing enabled and disabled as expected)
            // this check is performed last so that dictionary processing that fails
            // is not checked (only the fall back processing is checked)
            assertThat(block).isInstanceOf(expectedType);
            return outputPositionsCount;
        }

        @Override
        public int filterPositionsList(ConnectorSession session, int[] outputPositions, int[] activePositions, int offset, int size, SourcePage loadedPage)
        {
            assertThat(loadedPage.getChannelCount()).isEqualTo(1);
            Block block = loadedPage.getBlock(0);

            int outputPositionsCount = 0;
            for (int index = offset; index < offset + size; index++) {
                int position = activePositions[index];
                long value = BIGINT.getLong(block, position);
                verifyPositive(value);

                boolean selected = isSelected(selectRange, value);
                if (selected) {
                    outputPositions[outputPositionsCount] = position;
                    outputPositionsCount++;
                }
            }

            // verify the input block is the expected type (this is to assure that
            // dictionary processing enabled and disabled as expected)
            // this check is performed last so that dictionary processing that fails
            // is not checked (only the fall back processing is checked)
            assertThat(block).isInstanceOf(expectedType);
            return outputPositionsCount;
        }

        private static void verifyPositive(long value)
        {
            if (value < 0) {
                throw new NegativeValueException(value);
            }
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
