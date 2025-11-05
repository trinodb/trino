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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.spi.block.DictionaryId.randomDictionaryId;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestDictionaryAwarePageProjection
{
    private static final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("TestDictionaryAwarePageProjection-%s"));

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testDelegateMethods()
    {
        DictionaryAwarePageProjection projection = createProjection();
        assertThat(projection.isDeterministic()).isEqualTo(true);
        assertThat(projection.getInputChannels().getInputChannels()).isEqualTo(ImmutableList.of(3));
    }

    @Test
    public void testSimpleBlock()
    {
        ValueBlock block = createLongSequenceBlock(0, 100);
        testProject(block, block.getClass());
        testProject(block, block.getClass());
    }

    @Test
    public void testRleBlock()
    {
        Block value = createLongSequenceBlock(42, 43);
        RunLengthEncodedBlock block = (RunLengthEncodedBlock) RunLengthEncodedBlock.create(value, 100);

        testProject(block, RunLengthEncodedBlock.class);
        testProject(block, RunLengthEncodedBlock.class);
    }

    @Test
    public void testRleBlockWithFailure()
    {
        Block value = createLongSequenceBlock(-43, -42);
        RunLengthEncodedBlock block = (RunLengthEncodedBlock) RunLengthEncodedBlock.create(value, 100);

        testProjectFails(block, RunLengthEncodedBlock.class);
        testProjectFails(block, RunLengthEncodedBlock.class);
    }

    @Test
    public void testDictionaryBlock()
    {
        Block block = createDictionaryBlock(10, 100);

        testProject(block, DictionaryBlock.class);
        testProject(block, DictionaryBlock.class);
    }

    @Test
    public void testDictionaryBlockWithFailure()
    {
        Block block = createDictionaryBlockWithFailure(10, 100);

        testProjectFails(block, DictionaryBlock.class);
        testProjectFails(block, DictionaryBlock.class);
    }

    @Test
    public void testDictionaryBlockProcessingWithUnusedFailure()
    {
        Block block = createDictionaryBlockWithUnusedEntries(10, 100);

        // failures in the dictionary processing will cause a fallback to normal columnar processing
        testProject(block, LongArrayBlock.class);
        testProject(block, LongArrayBlock.class);
    }

    @Test
    public void testDictionaryProcessingEnableDisable()
    {
        DictionaryAwarePageProjection projection = createProjection();

        // function will always process the first dictionary
        Block ineffectiveBlock = createDictionaryBlock(100, 20);
        testProjectRange(ineffectiveBlock, DictionaryBlock.class, projection);
        // dictionary processing can reuse the last dictionary
        testProjectList(ineffectiveBlock, DictionaryBlock.class, projection);

        // last dictionary not effective, and incoming dictionary is also not effective, so dictionary processing is disabled
        Block anotherIneffectiveBlock = createDictionaryBlock(100, 25);
        testProjectRange(anotherIneffectiveBlock, LongArrayBlock.class, projection);
        testProjectList(anotherIneffectiveBlock, LongArrayBlock.class, projection);

        for (int i = 0; i < 15; i++) {
            // Increase usage count of large ineffective dictionary with multiple pages of small positions count
            testProjectRange(anotherIneffectiveBlock, LongArrayBlock.class, projection);
        }

        // last dictionary effective, so dictionary processing is enabled again
        testProjectRange(ineffectiveBlock, DictionaryBlock.class, projection);
        // dictionary processing can reuse the last dictionary
        testProjectList(ineffectiveBlock, DictionaryBlock.class, projection);

        // last dictionary not effective, but incoming dictionary is effective, so dictionary processing stays enabled
        Block effectiveBlock = createDictionaryBlock(10, 100);
        testProjectRange(effectiveBlock, DictionaryBlock.class, projection);
    }

    @Test
    public void testPreservesDictionaryInstance()
    {
        DictionaryAwarePageProjection projection = new DictionaryAwarePageProjection(
                new InputPageProjection(0),
                block -> randomDictionaryId());
        Block dictionary = createLongsBlock(0, 1);
        Block firstDictionaryBlock = DictionaryBlock.create(4, dictionary, new int[] {0, 1, 2, 3});
        Block secondDictionaryBlock = DictionaryBlock.create(4, dictionary, new int[] {3, 2, 1, 0});

        Block firstOutputBlock = projection.project(null, SourcePage.create(firstDictionaryBlock), SelectedPositions.positionsList(new int[] {0, 1}, 0, 2));
        assertThat(firstOutputBlock).isInstanceOf(DictionaryBlock.class);

        Block secondOutputBlock = projection.project(null, SourcePage.create(secondDictionaryBlock), SelectedPositions.positionsList(new int[] {0, 1}, 0, 2));
        assertThat(secondOutputBlock).isInstanceOf(DictionaryBlock.class);

        assertThat(firstOutputBlock).isNotSameAs(secondOutputBlock);
        Block firstDictionary = ((DictionaryBlock) firstOutputBlock).getDictionary();
        Block secondDictionary = ((DictionaryBlock) secondOutputBlock).getDictionary();
        assertThat(firstDictionary).isSameAs(secondDictionary);
        assertThat(firstDictionary).isSameAs(dictionary);
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

    private static void testProject(Block block, Class<? extends Block> expectedResultType)
    {
        testProjectRange(block, expectedResultType, createProjection());
        testProjectList(block, expectedResultType, createProjection());
    }

    private static void testProjectFails(Block block, Class<? extends Block> expectedResultType)
    {
        assertThatThrownBy(() -> testProjectRange(block, expectedResultType, createProjection()))
                .isInstanceOf(NegativeValueException.class)
                .hasMessageContaining("value is negative");
        assertThatThrownBy(() -> testProjectList(block, expectedResultType, createProjection()))
                .isInstanceOf(NegativeValueException.class)
                .hasMessageContaining("value is negative");
    }

    private static void testProjectRange(Block block, Class<? extends Block> expectedResultType, DictionaryAwarePageProjection projection)
    {
        Block result = projection.project(null, SourcePage.create(block), SelectedPositions.positionsRange(5, 10));

        assertBlockEquals(
                BIGINT,
                result,
                block.getRegion(5, 10));
        assertThat(result).isInstanceOf(expectedResultType);
    }

    private static void testProjectList(Block block, Class<? extends Block> expectedResultType, DictionaryAwarePageProjection projection)
    {
        int[] positions = {0, 2, 4, 6, 8, 10};
        Block result = projection.project(null, SourcePage.create(block), SelectedPositions.positionsList(positions, 0, positions.length));

        assertBlockEquals(
                BIGINT,
                result,
                block.copyPositions(positions, 0, positions.length));
        assertThat(result).isInstanceOf(expectedResultType);
    }

    private static DictionaryAwarePageProjection createProjection()
    {
        return new DictionaryAwarePageProjection(
                new TestPageProjection(),
                _ -> randomDictionaryId());
    }

    private static class TestPageProjection
            implements PageProjection
    {
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
        public Block project(ConnectorSession session, SourcePage page, SelectedPositions selectedPositions)
        {
            BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(selectedPositions.size());
            Block block = page.getBlock(0);
            int offset = selectedPositions.getOffset();
            if (selectedPositions.isList()) {
                int[] positions = selectedPositions.getPositions();
                for (int index = offset; index < offset + selectedPositions.size(); index++) {
                    BIGINT.writeLong(blockBuilder, verifyPositive(BIGINT.getLong(block, positions[index])));
                }
            }
            else {
                for (int position = offset; position < offset + selectedPositions.size(); position++) {
                    BIGINT.writeLong(blockBuilder, verifyPositive(BIGINT.getLong(block, position)));
                }
            }
            return blockBuilder.build();
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
