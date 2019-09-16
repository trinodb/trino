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
package io.prestosql.operator.project;

import com.google.common.collect.ImmutableList;
import io.prestosql.operator.DriverYieldSignal;
import io.prestosql.operator.Work;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.block.LongArrayBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.prestosql.block.BlockAssertions.assertBlockEquals;
import static io.prestosql.block.BlockAssertions.createLongSequenceBlock;
import static io.prestosql.spi.block.DictionaryId.randomDictionaryId;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestDictionaryAwarePageProjection
{
    private static final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("test-%s"));

    @DataProvider(name = "forceYield")
    public static Object[][] forceYieldAndProduceLazyBlock()
    {
        return new Object[][] {
                {true, false},
                {false, true},
                {false, false}};
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testDelegateMethods()
    {
        DictionaryAwarePageProjection projection = createProjection(false);
        assertEquals(projection.isDeterministic(), true);
        assertEquals(projection.getInputChannels().getInputChannels(), ImmutableList.of(3));
        assertEquals(projection.getType(), BIGINT);
    }

    @Test(dataProvider = "forceYield")
    public void testSimpleBlock(boolean forceYield, boolean produceLazyBlock)
    {
        Block block = createLongSequenceBlock(0, 100);
        testProject(block, block.getClass(), forceYield, produceLazyBlock);
    }

    @Test(dataProvider = "forceYield")
    public void testRleBlock(boolean forceYield, boolean produceLazyBlock)
    {
        Block value = createLongSequenceBlock(42, 43);
        RunLengthEncodedBlock block = new RunLengthEncodedBlock(value, 100);

        testProject(block, RunLengthEncodedBlock.class, forceYield, produceLazyBlock);
    }

    @Test(dataProvider = "forceYield")
    public void testRleBlockWithFailure(boolean forceYield, boolean produceLazyBlock)
    {
        Block value = createLongSequenceBlock(-43, -42);
        RunLengthEncodedBlock block = new RunLengthEncodedBlock(value, 100);

        testProjectFails(block, RunLengthEncodedBlock.class, forceYield, produceLazyBlock);
    }

    @Test(dataProvider = "forceYield")
    public void testDictionaryBlock(boolean forceYield, boolean produceLazyBlock)
    {
        DictionaryBlock block = createDictionaryBlock(10, 100);

        testProject(block, DictionaryBlock.class, forceYield, produceLazyBlock);
    }

    @Test(dataProvider = "forceYield")
    public void testDictionaryBlockWithFailure(boolean forceYield, boolean produceLazyBlock)
    {
        DictionaryBlock block = createDictionaryBlockWithFailure(10, 100);

        testProjectFails(block, DictionaryBlock.class, forceYield, produceLazyBlock);
    }

    @Test(dataProvider = "forceYield")
    public void testDictionaryBlockProcessingWithUnusedFailure(boolean forceYield, boolean produceLazyBlock)
    {
        DictionaryBlock block = createDictionaryBlockWithUnusedEntries(10, 100);

        // failures in the dictionary processing will cause a fallback to normal columnar processing
        testProject(block, LongArrayBlock.class, forceYield, produceLazyBlock);
    }

    @Test
    public void testDictionaryProcessingIgnoreYield()
    {
        DictionaryAwarePageProjection projection = createProjection(false);

        // the same input block will bypass yield with multiple projections
        DictionaryBlock block = createDictionaryBlock(10, 100);
        testProjectRange(block, DictionaryBlock.class, projection, true, false);
        testProjectFastReturnIgnoreYield(block, projection, false);
        testProjectFastReturnIgnoreYield(block, projection, false);
        testProjectFastReturnIgnoreYield(block, projection, false);
    }

    @Test(dataProvider = "forceYield")
    public void testDictionaryProcessingEnableDisable(boolean forceYield, boolean produceLazyBlock)
    {
        DictionaryAwarePageProjection projection = createProjection(produceLazyBlock);

        // function will always processes the first dictionary
        DictionaryBlock ineffectiveBlock = createDictionaryBlock(100, 20);
        testProjectRange(ineffectiveBlock, DictionaryBlock.class, projection, forceYield, produceLazyBlock);
        testProjectFastReturnIgnoreYield(ineffectiveBlock, projection, produceLazyBlock);
        // dictionary processing can reuse the last dictionary
        // in this case, we don't even check yield signal; make yieldForce to false
        testProjectList(ineffectiveBlock, DictionaryBlock.class, projection, false, produceLazyBlock);

        // last dictionary not effective, so dictionary processing is disabled
        DictionaryBlock effectiveBlock = createDictionaryBlock(10, 100);
        testProjectRange(effectiveBlock, LongArrayBlock.class, projection, forceYield, produceLazyBlock);
        testProjectList(effectiveBlock, LongArrayBlock.class, projection, forceYield, produceLazyBlock);

        // last dictionary effective, so dictionary processing is enabled again
        testProjectRange(ineffectiveBlock, DictionaryBlock.class, projection, forceYield, produceLazyBlock);
        testProjectFastReturnIgnoreYield(ineffectiveBlock, projection, produceLazyBlock);
        // dictionary processing can reuse the last dictionary
        // in this case, we don't even check yield signal; make yieldForce to false
        testProjectList(ineffectiveBlock, DictionaryBlock.class, projection, false, produceLazyBlock);

        // last dictionary not effective, so dictionary processing is disabled again
        testProjectRange(effectiveBlock, LongArrayBlock.class, projection, forceYield, produceLazyBlock);
        testProjectList(effectiveBlock, LongArrayBlock.class, projection, forceYield, produceLazyBlock);
    }

    private static DictionaryBlock createDictionaryBlock(int dictionarySize, int blockSize)
    {
        Block dictionary = createLongSequenceBlock(0, dictionarySize);
        int[] ids = new int[blockSize];
        Arrays.setAll(ids, index -> index % dictionarySize);
        return new DictionaryBlock(dictionary, ids);
    }

    private static DictionaryBlock createDictionaryBlockWithFailure(int dictionarySize, int blockSize)
    {
        Block dictionary = createLongSequenceBlock(-10, dictionarySize - 10);
        int[] ids = new int[blockSize];
        Arrays.setAll(ids, index -> index % dictionarySize);
        return new DictionaryBlock(dictionary, ids);
    }

    private static DictionaryBlock createDictionaryBlockWithUnusedEntries(int dictionarySize, int blockSize)
    {
        Block dictionary = createLongSequenceBlock(-10, dictionarySize);
        int[] ids = new int[blockSize];
        Arrays.setAll(ids, index -> (index % dictionarySize) + 10);
        return new DictionaryBlock(dictionary, ids);
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
        assertThrows(NegativeValueException.class, () -> testProjectRange(block, expectedResultType, createProjection(produceLazyBlock), forceYield, produceLazyBlock));
        assertThrows(NegativeValueException.class, () -> testProjectList(block, expectedResultType, createProjection(produceLazyBlock), forceYield, produceLazyBlock));
        assertThrows(NegativeValueException.class, () -> testProjectRange(lazyWrapper(block), expectedResultType, createProjection(produceLazyBlock), forceYield, produceLazyBlock));
        assertThrows(NegativeValueException.class, () -> testProjectList(lazyWrapper(block), expectedResultType, createProjection(produceLazyBlock), forceYield, produceLazyBlock));
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
            assertTrue(work.process());
            result = work.getResult();
        }

        if (produceLazyBlock) {
            assertInstanceOf(result, LazyBlock.class);
            assertFalse(result.isLoaded());
            assertFalse(block.isLoaded());
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
            assertTrue(work.process());
            result = work.getResult();
        }

        if (produceLazyBlock) {
            assertInstanceOf(result, LazyBlock.class);
            assertFalse(result.isLoaded());
            assertFalse(block.isLoaded());
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
        assertTrue(work.process());
        Block result = work.getResult();
        yieldSignal.reset();

        if (produceLazyBlock) {
            assertInstanceOf(result, LazyBlock.class);
            assertFalse(result.isLoaded());
            assertFalse(block.isLoaded());
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
        return new LazyBlock(block.getPositionCount(), lazyBlock -> lazyBlock.setBlock(block.getLoadedBlock()));
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
                assertNull(result);
                if (selectedPositions.isList()) {
                    int offset = selectedPositions.getOffset();
                    int[] positions = selectedPositions.getPositions();
                    for (int index = nextIndexOrPosition + offset; index < offset + selectedPositions.size(); index++) {
                        blockBuilder.writeLong(verifyPositive(block.getLong(positions[index], 0)));
                        if (yieldSignal.isSet()) {
                            nextIndexOrPosition = index + 1 - offset;
                            return false;
                        }
                    }
                }
                else {
                    int offset = selectedPositions.getOffset();
                    for (int position = nextIndexOrPosition + offset; position < offset + selectedPositions.size(); position++) {
                        blockBuilder.writeLong(verifyPositive(block.getLong(position, 0)));
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
                assertNotNull(result);
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
