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
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.Work;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.spi.block.DictionaryId.randomDictionaryId;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;
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
        DictionaryAwarePageProjection projection = createProjection(false);
        assertThat(projection.isDeterministic()).isEqualTo(true);
        assertThat(projection.getInputChannels().getInputChannels()).isEqualTo(ImmutableList.of(3));
        assertThat(projection.getType()).isEqualTo(BIGINT);
    }

    @Test
    public void testSimpleBlock()
    {
        ValueBlock block = createLongSequenceBlock(0, 100);
        testProject(block, block.getClass(), true, false);
        testProject(block, block.getClass(), false, true);
        testProject(block, block.getClass(), false, false);
    }

    @Test
    public void testRleBlock()
    {
        Block value = createLongSequenceBlock(42, 43);
        RunLengthEncodedBlock block = (RunLengthEncodedBlock) RunLengthEncodedBlock.create(value, 100);

        testProject(block, RunLengthEncodedBlock.class, true, false);
        testProject(block, RunLengthEncodedBlock.class, false, true);
        testProject(block, RunLengthEncodedBlock.class, false, false);
    }

    @Test
    public void testRleBlockWithFailure()
    {
        Block value = createLongSequenceBlock(-43, -42);
        RunLengthEncodedBlock block = (RunLengthEncodedBlock) RunLengthEncodedBlock.create(value, 100);

        testProjectFails(block, RunLengthEncodedBlock.class, true, false);
        testProjectFails(block, RunLengthEncodedBlock.class, false, true);
        testProjectFails(block, RunLengthEncodedBlock.class, false, false);
    }

    @Test
    public void testDictionaryBlock()
    {
        Block block = createDictionaryBlock(10, 100);

        testProject(block, DictionaryBlock.class, true, false);
        testProject(block, DictionaryBlock.class, false, true);
        testProject(block, DictionaryBlock.class, false, false);
    }

    @Test
    public void testDictionaryBlockWithFailure()
    {
        Block block = createDictionaryBlockWithFailure(10, 100);

        testProjectFails(block, DictionaryBlock.class, true, false);
        testProjectFails(block, DictionaryBlock.class, false, true);
        testProjectFails(block, DictionaryBlock.class, false, false);
    }

    @Test
    public void testDictionaryBlockProcessingWithUnusedFailure()
    {
        Block block = createDictionaryBlockWithUnusedEntries(10, 100);

        // failures in the dictionary processing will cause a fallback to normal columnar processing
        testProject(block, LongArrayBlock.class, true, false);
        testProject(block, LongArrayBlock.class, false, true);
        testProject(block, LongArrayBlock.class, false, false);
    }

    @Test
    public void testDictionaryProcessingIgnoreYield()
    {
        DictionaryAwarePageProjection projection = createProjection(false);

        // the same input block will bypass yield with multiple projections
        Block block = createDictionaryBlock(10, 100);
        testProjectRange(block, DictionaryBlock.class, projection, true, false);
        testProjectFastReturnIgnoreYield(block, projection, false);
        testProjectFastReturnIgnoreYield(block, projection, false);
        testProjectFastReturnIgnoreYield(block, projection, false);
    }

    @Test
    public void testDictionaryProcessingEnableDisable()
    {
        testDictionaryProcessingEnableDisable(true, false);
        testDictionaryProcessingEnableDisable(false, true);
        testDictionaryProcessingEnableDisable(false, false);
    }

    private void testDictionaryProcessingEnableDisable(boolean forceYield, boolean produceLazyBlock)
    {
        DictionaryAwarePageProjection projection = createProjection(produceLazyBlock);

        // function will always process the first dictionary
        Block ineffectiveBlock = createDictionaryBlock(100, 20);
        testProjectRange(ineffectiveBlock, DictionaryBlock.class, projection, forceYield, produceLazyBlock);
        testProjectFastReturnIgnoreYield(ineffectiveBlock, projection, produceLazyBlock);
        // dictionary processing can reuse the last dictionary
        // in this case, we don't even check yield signal; make yieldForce to false
        testProjectList(ineffectiveBlock, DictionaryBlock.class, projection, false, produceLazyBlock);

        // last dictionary not effective, and incoming dictionary is also not effective, so dictionary processing is disabled
        Block anotherIneffectiveBlock = createDictionaryBlock(100, 25);
        testProjectRange(anotherIneffectiveBlock, LongArrayBlock.class, projection, forceYield, produceLazyBlock);
        testProjectList(anotherIneffectiveBlock, LongArrayBlock.class, projection, forceYield, produceLazyBlock);

        for (int i = 0; i < 15; i++) {
            // Increase usage count of large ineffective dictionary with multiple pages of small positions count
            testProjectRange(anotherIneffectiveBlock, LongArrayBlock.class, projection, forceYield, produceLazyBlock);
        }

        // last dictionary effective, so dictionary processing is enabled again
        testProjectRange(ineffectiveBlock, DictionaryBlock.class, projection, forceYield, produceLazyBlock);
        testProjectFastReturnIgnoreYield(ineffectiveBlock, projection, produceLazyBlock);
        // dictionary processing can reuse the last dictionary
        // in this case, we don't even check yield signal; make yieldForce to false
        testProjectList(ineffectiveBlock, DictionaryBlock.class, projection, false, produceLazyBlock);

        // last dictionary not effective, but incoming dictionary is effective, so dictionary processing stays enabled
        Block effectiveBlock = createDictionaryBlock(10, 100);
        testProjectRange(effectiveBlock, DictionaryBlock.class, projection, forceYield, produceLazyBlock);
        testProjectFastReturnIgnoreYield(effectiveBlock, projection, produceLazyBlock);
    }

    @Test
    public void testPreservesDictionaryInstance()
    {
        DictionaryAwarePageProjection projection = new DictionaryAwarePageProjection(
                new InputPageProjection(0, BIGINT),
                block -> randomDictionaryId(),
                false);
        Block dictionary = createLongsBlock(0, 1);
        Block firstDictionaryBlock = DictionaryBlock.create(4, dictionary, new int[] {0, 1, 2, 3});
        Block secondDictionaryBlock = DictionaryBlock.create(4, dictionary, new int[] {3, 2, 1, 0});

        DriverYieldSignal yieldSignal = new DriverYieldSignal();
        Work<Block> firstWork = projection.project(null, yieldSignal, new Page(firstDictionaryBlock), SelectedPositions.positionsList(new int[] {0, 1}, 0, 2));

        assertThat(firstWork.process()).isTrue();
        Block firstOutputBlock = firstWork.getResult();
        assertInstanceOf(firstOutputBlock, DictionaryBlock.class);

        Work<Block> secondWork = projection.project(null, yieldSignal, new Page(secondDictionaryBlock), SelectedPositions.positionsList(new int[] {0, 1}, 0, 2));

        assertThat(secondWork.process()).isTrue();
        Block secondOutputBlock = secondWork.getResult();
        assertInstanceOf(secondOutputBlock, DictionaryBlock.class);

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

    private static Block projectWithYield(Work<Block> work, DriverYieldSignal yieldSignal)
    {
        int yieldCount = 0;
        while (true) {
            yieldSignal.setWithDelay(1, executor);
            yieldSignal.forceYieldForTesting();
            if (work.process()) {
                assertGreaterThan(yieldCount, 0);
                return work.getResult();
            }
            yieldCount++;
            if (yieldCount > 1_000_000) {
                fail("projection is not making progress");
            }
            yieldSignal.reset();
        }
    }

    private static void testProject(Block block, Class<? extends Block> expectedResultType, boolean forceYield, boolean produceLazyBlock)
    {
        testProjectRange(block, expectedResultType, createProjection(produceLazyBlock), forceYield, produceLazyBlock);
        testProjectList(block, expectedResultType, createProjection(produceLazyBlock), forceYield, produceLazyBlock);
        testProjectRange(lazyWrapper(block), expectedResultType, createProjection(produceLazyBlock), forceYield, produceLazyBlock);
        testProjectList(lazyWrapper(block), expectedResultType, createProjection(produceLazyBlock), forceYield, produceLazyBlock);
    }

    private static void testProjectFails(Block block, Class<? extends Block> expectedResultType, boolean forceYield, boolean produceLazyBlock)
    {
        assertThatThrownBy(() -> testProjectRange(block, expectedResultType, createProjection(produceLazyBlock), forceYield, produceLazyBlock))
                .isInstanceOf(NegativeValueException.class)
                .hasMessageContaining("value is negative");
        assertThatThrownBy(() -> testProjectList(block, expectedResultType, createProjection(produceLazyBlock), forceYield, produceLazyBlock))
                .isInstanceOf(NegativeValueException.class)
                .hasMessageContaining("value is negative");
        assertThatThrownBy(() -> testProjectRange(lazyWrapper(block), expectedResultType, createProjection(produceLazyBlock), forceYield, produceLazyBlock))
                .isInstanceOf(NegativeValueException.class)
                .hasMessageContaining("value is negative");
        assertThatThrownBy(() -> testProjectList(lazyWrapper(block), expectedResultType, createProjection(produceLazyBlock), forceYield, produceLazyBlock))
                .isInstanceOf(NegativeValueException.class)
                .hasMessageContaining("value is negative");
    }

    private static void testProjectRange(Block block, Class<? extends Block> expectedResultType, DictionaryAwarePageProjection projection, boolean forceYield, boolean produceLazyBlock)
    {
        if (produceLazyBlock) {
            block = lazyWrapper(block);
        }

        DriverYieldSignal yieldSignal = new DriverYieldSignal();
        Work<Block> work = projection.project(null, yieldSignal, new Page(block), SelectedPositions.positionsRange(5, 10));
        Block result;
        if (forceYield) {
            result = projectWithYield(work, yieldSignal);
        }
        else {
            assertThat(work.process()).isTrue();
            result = work.getResult();
        }

        if (produceLazyBlock) {
            assertInstanceOf(result, LazyBlock.class);
            assertThat(result.isLoaded()).isFalse();
            assertThat(block.isLoaded()).isFalse();
            result = result.getLoadedBlock();
        }

        assertBlockEquals(
                BIGINT,
                result,
                block.getRegion(5, 10));
        assertInstanceOf(result, expectedResultType);
    }

    private static void testProjectList(Block block, Class<? extends Block> expectedResultType, DictionaryAwarePageProjection projection, boolean forceYield, boolean produceLazyBlock)
    {
        if (produceLazyBlock) {
            block = lazyWrapper(block);
        }

        DriverYieldSignal yieldSignal = new DriverYieldSignal();
        int[] positions = {0, 2, 4, 6, 8, 10};
        Work<Block> work = projection.project(null, yieldSignal, new Page(block), SelectedPositions.positionsList(positions, 0, positions.length));
        Block result;
        if (forceYield) {
            result = projectWithYield(work, yieldSignal);
        }
        else {
            assertThat(work.process()).isTrue();
            result = work.getResult();
        }

        if (produceLazyBlock) {
            assertInstanceOf(result, LazyBlock.class);
            assertThat(result.isLoaded()).isFalse();
            assertThat(block.isLoaded()).isFalse();
            result = result.getLoadedBlock();
        }

        assertBlockEquals(
                BIGINT,
                result,
                block.copyPositions(positions, 0, positions.length));
        assertInstanceOf(result, expectedResultType);
    }

    private static void testProjectFastReturnIgnoreYield(Block block, DictionaryAwarePageProjection projection, boolean produceLazyBlock)
    {
        if (produceLazyBlock) {
            block = lazyWrapper(block);
        }

        DriverYieldSignal yieldSignal = new DriverYieldSignal();
        Work<Block> work = projection.project(null, yieldSignal, new Page(block), SelectedPositions.positionsRange(5, 10));
        yieldSignal.setWithDelay(1, executor);
        yieldSignal.forceYieldForTesting();

        // yield signal is ignored given the block has already been loaded
        assertThat(work.process()).isTrue();
        Block result = work.getResult();
        yieldSignal.reset();

        if (produceLazyBlock) {
            assertInstanceOf(result, LazyBlock.class);
            assertThat(result.isLoaded()).isFalse();
            assertThat(block.isLoaded()).isFalse();
            result = result.getLoadedBlock();
        }

        assertBlockEquals(
                BIGINT,
                result,
                block.getRegion(5, 10));
        assertInstanceOf(result, DictionaryBlock.class);
    }

    private static DictionaryAwarePageProjection createProjection(boolean produceLazyBlock)
    {
        return new DictionaryAwarePageProjection(
                new TestPageProjection(),
                block -> randomDictionaryId(),
                produceLazyBlock);
    }

    private static LazyBlock lazyWrapper(Block block)
    {
        return new LazyBlock(block.getPositionCount(), block::getLoadedBlock);
    }

    private static class TestPageProjection
            implements PageProjection
    {
        @Override
        public Type getType()
        {
            return BIGINT;
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
        public Work<Block> project(ConnectorSession session, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
        {
            return new TestPageProjectionWork(yieldSignal, page, selectedPositions);
        }

        private static class TestPageProjectionWork
                implements Work<Block>
        {
            private final DriverYieldSignal yieldSignal;
            private final Block block;
            private final SelectedPositions selectedPositions;

            private BlockBuilder blockBuilder;
            private int nextIndexOrPosition;
            private Block result;

            public TestPageProjectionWork(DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
            {
                this.yieldSignal = yieldSignal;
                this.block = page.getBlock(0);
                this.selectedPositions = selectedPositions;
                this.blockBuilder = BIGINT.createBlockBuilder(null, selectedPositions.size());
            }

            @Override
            public boolean process()
            {
                assertThat(result).isNull();
                if (selectedPositions.isList()) {
                    int offset = selectedPositions.getOffset();
                    int[] positions = selectedPositions.getPositions();
                    for (int index = nextIndexOrPosition + offset; index < offset + selectedPositions.size(); index++) {
                        BIGINT.writeLong(blockBuilder, verifyPositive(BIGINT.getLong(block, positions[index])));
                        if (yieldSignal.isSet()) {
                            nextIndexOrPosition = index + 1 - offset;
                            return false;
                        }
                    }
                }
                else {
                    int offset = selectedPositions.getOffset();
                    for (int position = nextIndexOrPosition + offset; position < offset + selectedPositions.size(); position++) {
                        BIGINT.writeLong(blockBuilder, verifyPositive(BIGINT.getLong(block, position)));
                        if (yieldSignal.isSet()) {
                            nextIndexOrPosition = position + 1 - offset;
                            return false;
                        }
                    }
                }
                result = blockBuilder.build();
                blockBuilder = blockBuilder.newBlockBuilderLike(null);
                return true;
            }

            @Override
            public Block getResult()
            {
                assertThat(result).isNotNull();
                return result;
            }
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
