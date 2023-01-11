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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.block.BlockAssertions;
import io.trino.operator.MultiChannelGroupByHash.GetLowCardinalityDictionaryGroupIdsWork;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.testing.TestingSession;
import io.trino.type.BlockTypeOperators;
import io.trino.type.TypeTestUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.google.common.math.DoubleMath.log2;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createStringSequenceBlock;
import static io.trino.operator.GroupByHash.createGroupByHash;
import static io.trino.operator.UpdateMemory.NOOP;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.TypeTestUtils.getHashBlock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestGroupByHash
{
    private static final int MAX_GROUP_ID = 500;
    private static final int[] CONTAINS_CHANNELS = new int[] {0};
    private static final Session TEST_SESSION = TestingSession.testSessionBuilder().build();
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final BlockTypeOperators TYPE_OPERATOR_FACTORY = new BlockTypeOperators(TYPE_OPERATORS);
    private static final JoinCompiler JOIN_COMPILER = new JoinCompiler(TYPE_OPERATORS);

    @DataProvider
    public Object[][] dataType()
    {
        return new Object[][] {{VARCHAR}, {BIGINT}};
    }

    @DataProvider
    public Object[][] groupByHashType()
    {
        return new Object[][] {{GroupByHashType.BIGINT}, {GroupByHashType.MULTI_CHANNEL}};
    }

    private enum GroupByHashType
    {
        BIGINT, MULTI_CHANNEL;

        public GroupByHash createGroupByHash()
        {
            return createGroupByHash(100, NOOP);
        }

        public GroupByHash createGroupByHash(int expectedSize, UpdateMemory updateMemory)
        {
            switch (this) {
                case BIGINT:
                    return new BigintGroupByHash(0, true, expectedSize, updateMemory);
                case MULTI_CHANNEL:
                    return new MultiChannelGroupByHash(
                            ImmutableList.of(BigintType.BIGINT),
                            new int[] {0},
                            Optional.of(1),
                            expectedSize,
                            true,
                            JOIN_COMPILER,
                            TYPE_OPERATOR_FACTORY,
                            updateMemory);
            }

            throw new UnsupportedOperationException();
        }
    }

    @Test(dataProvider = "groupByHashType")
    public void testAddPage(GroupByHashType groupByHashType)
    {
        GroupByHash groupByHash = groupByHashType.createGroupByHash();
        for (int tries = 0; tries < 2; tries++) {
            for (int value = 0; value < MAX_GROUP_ID; value++) {
                Block block = BlockAssertions.createLongsBlock(value);
                Block hashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(BIGINT), block);
                Page page = new Page(block, hashBlock);
                for (int addValuesTries = 0; addValuesTries < 10; addValuesTries++) {
                    groupByHash.addPage(page).process();
                    assertEquals(groupByHash.getGroupCount(), tries == 0 ? value + 1 : MAX_GROUP_ID);

                    // add the page again using get group ids and make sure the group count didn't change
                    Work<GroupByIdBlock> work = groupByHash.getGroupIds(page);
                    work.process();
                    GroupByIdBlock groupIds = work.getResult();
                    assertEquals(groupByHash.getGroupCount(), tries == 0 ? value + 1 : MAX_GROUP_ID);
                    assertEquals(groupIds.getGroupCount(), tries == 0 ? value + 1 : MAX_GROUP_ID);

                    // verify the first position
                    assertEquals(groupIds.getPositionCount(), 1);
                    long groupId = groupIds.getGroupId(0);
                    assertEquals(groupId, value);
                }
            }
        }
    }

    @Test(dataProvider = "groupByHashType")
    public void testRunLengthEncodedInputPage(GroupByHashType groupByHashType)
    {
        GroupByHash groupByHash = groupByHashType.createGroupByHash();
        Block block = BlockAssertions.createLongsBlock(0L);
        Block hashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(BIGINT), block);
        Page page = new Page(
                RunLengthEncodedBlock.create(block, 2),
                RunLengthEncodedBlock.create(hashBlock, 2));

        groupByHash.addPage(page).process();

        assertEquals(groupByHash.getGroupCount(), 1);

        Work<GroupByIdBlock> work = groupByHash.getGroupIds(page);
        work.process();
        GroupByIdBlock groupIds = work.getResult();

        assertEquals(groupIds.getGroupCount(), 1);
        assertEquals(groupIds.getPositionCount(), 2);
        assertEquals(groupIds.getGroupId(0), 0);
        assertEquals(groupIds.getGroupId(1), 0);

        List<Block> children = groupIds.getChildren();
        assertEquals(children.size(), 1);
        assertTrue(children.get(0) instanceof RunLengthEncodedBlock);
    }

    @Test(dataProvider = "groupByHashType")
    public void testDictionaryInputPage(GroupByHashType groupByHashType)
    {
        GroupByHash groupByHash = groupByHashType.createGroupByHash();
        Block block = BlockAssertions.createLongsBlock(0L, 1L);
        Block hashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(BIGINT), block);
        int[] ids = new int[] {0, 0, 1, 1};
        Page page = new Page(
                DictionaryBlock.create(ids.length, block, ids),
                DictionaryBlock.create(ids.length, hashBlock, ids));

        groupByHash.addPage(page).process();

        assertEquals(groupByHash.getGroupCount(), 2);

        Work<GroupByIdBlock> work = groupByHash.getGroupIds(page);
        work.process();
        GroupByIdBlock groupIds = work.getResult();

        assertEquals(groupIds.getGroupCount(), 2);
        assertEquals(groupIds.getPositionCount(), 4);
        assertEquals(groupIds.getGroupId(0), 0);
        assertEquals(groupIds.getGroupId(1), 0);
        assertEquals(groupIds.getGroupId(2), 1);
        assertEquals(groupIds.getGroupId(3), 1);
    }

    @Test(dataProvider = "groupByHashType")
    public void testNullGroup(GroupByHashType groupByHashType)
    {
        GroupByHash groupByHash = groupByHashType.createGroupByHash();

        Block block = createLongsBlock((Long) null);
        Block hashBlock = getHashBlock(ImmutableList.of(BIGINT), block);
        Page page = new Page(block, hashBlock);
        groupByHash.addPage(page).process();

        // Add enough values to force a rehash
        block = createLongSequenceBlock(1, 132748);
        hashBlock = getHashBlock(ImmutableList.of(BIGINT), block);
        page = new Page(block, hashBlock);
        groupByHash.addPage(page).process();

        block = createLongsBlock(0);
        hashBlock = getHashBlock(ImmutableList.of(BIGINT), block);
        page = new Page(block, hashBlock);
        assertFalse(groupByHash.contains(0, page, CONTAINS_CHANNELS));
    }

    @Test(dataProvider = "groupByHashType")
    public void testGetGroupIds(GroupByHashType groupByHashType)
    {
        GroupByHash groupByHash = groupByHashType.createGroupByHash();
        for (int tries = 0; tries < 2; tries++) {
            for (int value = 0; value < MAX_GROUP_ID; value++) {
                Block block = BlockAssertions.createLongsBlock(value);
                Block hashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(BIGINT), block);
                Page page = new Page(block, hashBlock);
                for (int addValuesTries = 0; addValuesTries < 10; addValuesTries++) {
                    Work<GroupByIdBlock> work = groupByHash.getGroupIds(page);
                    work.process();
                    GroupByIdBlock groupIds = work.getResult();
                    assertEquals(groupIds.getGroupCount(), tries == 0 ? value + 1 : MAX_GROUP_ID);
                    assertEquals(groupIds.getPositionCount(), 1);
                    long groupId = groupIds.getGroupId(0);
                    assertEquals(groupId, value);
                }
            }
        }
    }

    @Test
    public void testTypes()
    {
        GroupByHash groupByHash = createGroupByHash(TEST_SESSION, ImmutableList.of(VARCHAR), new int[] {0}, Optional.of(1), 100, JOIN_COMPILER, TYPE_OPERATOR_FACTORY, NOOP);
        // Additional bigint channel for hash
        assertEquals(groupByHash.getTypes(), ImmutableList.of(VARCHAR, BIGINT));
    }

    @Test(dataProvider = "groupByHashType")
    public void testAppendTo(GroupByHashType groupByHashType)
    {
        Block valuesBlock = BlockAssertions.createLongSequenceBlock(0, 100);
        Block hashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(BIGINT), valuesBlock);
        GroupByHash groupByHash = groupByHashType.createGroupByHash();

        Work<GroupByIdBlock> work = groupByHash.getGroupIds(new Page(valuesBlock, hashBlock));
        work.process();
        GroupByIdBlock groupIds = work.getResult();
        for (int i = 0; i < groupIds.getPositionCount(); i++) {
            assertEquals(groupIds.getGroupId(i), i);
        }
        assertEquals(groupByHash.getGroupCount(), 100);

        PageBuilder pageBuilder = new PageBuilder(groupByHash.getTypes());
        for (int i = 0; i < groupByHash.getGroupCount(); i++) {
            pageBuilder.declarePosition();
            groupByHash.appendValuesTo(i, pageBuilder);
        }
        Page page = pageBuilder.build();
        // Ensure that all blocks have the same positionCount
        for (int i = 0; i < groupByHash.getTypes().size(); i++) {
            assertEquals(page.getBlock(i).getPositionCount(), 100);
        }
        assertEquals(page.getPositionCount(), 100);
        BlockAssertions.assertBlockEquals(BIGINT, page.getBlock(0), valuesBlock);
        BlockAssertions.assertBlockEquals(BIGINT, page.getBlock(1), hashBlock);
    }

    @Test(dataProvider = "groupByHashType")
    public void testAppendToMultipleTuplesPerGroup(GroupByHashType groupByHashType)
    {
        List<Long> values = new ArrayList<>();
        for (long i = 0; i < 100; i++) {
            values.add(i % 50);
        }
        Block valuesBlock = BlockAssertions.createLongsBlock(values);
        Block hashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(BIGINT), valuesBlock);

        GroupByHash groupByHash = groupByHashType.createGroupByHash();
        groupByHash.getGroupIds(new Page(valuesBlock, hashBlock)).process();
        assertEquals(groupByHash.getGroupCount(), 50);

        PageBuilder pageBuilder = new PageBuilder(groupByHash.getTypes());
        for (int i = 0; i < groupByHash.getGroupCount(); i++) {
            pageBuilder.declarePosition();
            groupByHash.appendValuesTo(i, pageBuilder);
        }
        Page outputPage = pageBuilder.build();
        assertEquals(outputPage.getPositionCount(), 50);
        BlockAssertions.assertBlockEquals(BIGINT, outputPage.getBlock(0), BlockAssertions.createLongSequenceBlock(0, 50));
    }

    @Test(dataProvider = "groupByHashType")
    public void testContains(GroupByHashType groupByHashType)
    {
        Block valuesBlock = BlockAssertions.createLongSequenceBlock(0, 10);
        Block hashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(BIGINT), valuesBlock);
        GroupByHash groupByHash = groupByHashType.createGroupByHash();
        groupByHash.getGroupIds(new Page(valuesBlock, hashBlock)).process();

        Block testBlock = BlockAssertions.createLongsBlock(3);
        Block testHashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(BIGINT), testBlock);
        assertTrue(groupByHash.contains(0, new Page(testBlock, testHashBlock), CONTAINS_CHANNELS));

        testBlock = BlockAssertions.createLongsBlock(11);
        testHashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(BIGINT), testBlock);
        assertFalse(groupByHash.contains(0, new Page(testBlock, testHashBlock), CONTAINS_CHANNELS));
    }

    @Test
    public void testContainsMultipleColumns()
    {
        Block valuesBlock = BlockAssertions.createDoubleSequenceBlock(0, 10);
        Block stringValuesBlock = BlockAssertions.createStringSequenceBlock(0, 10);
        Block hashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(DOUBLE, VARCHAR), valuesBlock, stringValuesBlock);
        int[] hashChannels = {0, 1};
        GroupByHash groupByHash = createGroupByHash(TEST_SESSION, ImmutableList.of(DOUBLE, VARCHAR), hashChannels, Optional.of(2), 100, JOIN_COMPILER, TYPE_OPERATOR_FACTORY, NOOP);
        groupByHash.getGroupIds(new Page(valuesBlock, stringValuesBlock, hashBlock)).process();

        Block testValuesBlock = BlockAssertions.createDoublesBlock((double) 3);
        Block testStringValuesBlock = BlockAssertions.createStringsBlock("3");
        Block testHashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(DOUBLE, VARCHAR), testValuesBlock, testStringValuesBlock);
        assertTrue(groupByHash.contains(0, new Page(testValuesBlock, testStringValuesBlock, testHashBlock), hashChannels));
    }

    @Test(dataProvider = "groupByHashType")
    public void testForceRehash(GroupByHashType groupByHashType)
    {
        // Create a page with positionCount >> expected size of groupByHash
        Block valuesBlock = BlockAssertions.createLongSequenceBlock(0, 100);
        Block hashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(BIGINT), valuesBlock);

        // Create group by hash with extremely small size
        GroupByHash groupByHash = groupByHashType.createGroupByHash(4, NOOP);
        groupByHash.getGroupIds(new Page(valuesBlock, hashBlock)).process();

        // Ensure that all groups are present in group by hash
        for (int i = 0; i < valuesBlock.getPositionCount(); i++) {
            assertTrue(groupByHash.contains(i, new Page(valuesBlock, hashBlock), CONTAINS_CHANNELS));
        }
    }

    @Test(dataProvider = "dataType")
    public void testUpdateMemory(Type type)
    {
        // Create a page with positionCount >> expected size of groupByHash
        int length = 1_000_000;
        Block valuesBlock;
        if (type == VARCHAR) {
            valuesBlock = createStringSequenceBlock(0, length);
        }
        else if (type == BIGINT) {
            valuesBlock = createLongSequenceBlock(0, length);
        }
        else {
            throw new IllegalArgumentException("unsupported data type");
        }
        Block hashBlock = getHashBlock(ImmutableList.of(type), valuesBlock);

        // Create group by hash with extremely small size
        AtomicInteger rehashCount = new AtomicInteger();
        GroupByHash groupByHash = createGroupByHash(
                ImmutableList.of(type),
                new int[] {0},
                Optional.of(1),
                1,
                false,
                JOIN_COMPILER,
                TYPE_OPERATOR_FACTORY,
                () -> {
                    rehashCount.incrementAndGet();
                    return true;
                });
        groupByHash.addPage(new Page(valuesBlock, hashBlock)).process();

        // assert we call update memory twice every time we rehash; the rehash count = log2(length / FILL_RATIO)
        assertEquals(rehashCount.get(), 2 * log2(length / 0.75, RoundingMode.FLOOR));
    }

    @Test(dataProvider = "dataType")
    public void testMemoryReservationYield(Type type)
    {
        // Create a page with positionCount >> expected size of groupByHash
        int length = 1_000_000;
        Block valuesBlock;
        if (type == VARCHAR) {
            valuesBlock = createStringSequenceBlock(0, length);
        }
        else if (type == BIGINT) {
            valuesBlock = createLongSequenceBlock(0, length);
        }
        else {
            throw new IllegalArgumentException("unsupported data type");
        }
        Block hashBlock = getHashBlock(ImmutableList.of(type), valuesBlock);
        Page page = new Page(valuesBlock, hashBlock);
        AtomicInteger currentQuota = new AtomicInteger(0);
        AtomicInteger allowedQuota = new AtomicInteger(6);
        UpdateMemory updateMemory = () -> {
            if (currentQuota.get() < allowedQuota.get()) {
                currentQuota.getAndIncrement();
                return true;
            }
            return false;
        };
        int yields = 0;

        // test addPage
        GroupByHash groupByHash = createGroupByHash(ImmutableList.of(type), new int[] {0}, Optional.of(1), 1, false, JOIN_COMPILER, TYPE_OPERATOR_FACTORY, updateMemory);
        boolean finish = false;
        Work<?> addPageWork = groupByHash.addPage(page);
        while (!finish) {
            finish = addPageWork.process();
            if (!finish) {
                assertEquals(currentQuota.get(), allowedQuota.get());
                // assert if we are blocked, we are going to be blocked again without changing allowedQuota
                assertFalse(addPageWork.process());
                assertEquals(currentQuota.get(), allowedQuota.get());
                yields++;
                allowedQuota.getAndAdd(6);
            }
        }

        // assert there is not anything missing
        assertEquals(length, groupByHash.getGroupCount());
        // assert we yield for every 3 rehashes
        // currentQuota is essentially the count we have successfully rehashed multiplied by 2 (as updateMemory is called twice per rehash)
        // the rehash count is 20 = log(1_000_000 / 0.75)
        assertEquals(currentQuota.get(), 20 * 2);
        assertEquals(currentQuota.get() / 3 / 2, yields);

        // test getGroupIds
        currentQuota.set(0);
        allowedQuota.set(6);
        yields = 0;
        groupByHash = createGroupByHash(ImmutableList.of(type), new int[] {0}, Optional.of(1), 1, false, JOIN_COMPILER, TYPE_OPERATOR_FACTORY, updateMemory);

        finish = false;
        Work<GroupByIdBlock> getGroupIdsWork = groupByHash.getGroupIds(page);
        while (!finish) {
            finish = getGroupIdsWork.process();
            if (!finish) {
                assertEquals(currentQuota.get(), allowedQuota.get());
                // assert if we are blocked, we are going to be blocked again without changing allowedQuota
                assertFalse(getGroupIdsWork.process());
                assertEquals(currentQuota.get(), allowedQuota.get());
                yields++;
                allowedQuota.getAndAdd(6);
            }
        }
        // assert there is not anything missing
        assertEquals(length, groupByHash.getGroupCount());
        assertEquals(length, getGroupIdsWork.getResult().getPositionCount());
        // assert we yield for every 3 rehashes
        // currentQuota is essentially the count we have successfully rehashed multiplied by 2 (as updateMemory is called twice per rehash)
        // the rehash count is 20 = log2(1_000_000 / 0.75)
        assertEquals(currentQuota.get(), 20 * 2);
        assertEquals(currentQuota.get() / 3 / 2, yields);
    }

    @Test(dataProvider = "groupByHashType")
    public void testMemoryReservationYieldWithDictionary(GroupByHashType groupByHashType)
    {
        // Create a page with positionCount >> expected size of groupByHash
        int dictionaryLength = 1_000;
        int length = 2_000_000;
        int[] ids = IntStream.range(0, dictionaryLength).toArray();
        Block valuesBlock = DictionaryBlock.create(dictionaryLength, createLongSequenceBlock(0, length), ids);
        Block hashBlock = DictionaryBlock.create(dictionaryLength, getHashBlock(ImmutableList.of(BIGINT), valuesBlock), ids);
        Page page = new Page(valuesBlock, hashBlock);
        AtomicInteger currentQuota = new AtomicInteger(0);
        AtomicInteger allowedQuota = new AtomicInteger(6);
        UpdateMemory updateMemory = () -> {
            if (currentQuota.get() < allowedQuota.get()) {
                currentQuota.getAndIncrement();
                return true;
            }
            return false;
        };
        int yields = 0;

        // test addPage
        GroupByHash groupByHash = groupByHashType.createGroupByHash(1, updateMemory);

        boolean finish = false;
        Work<?> addPageWork = groupByHash.addPage(page);
        while (!finish) {
            finish = addPageWork.process();
            if (!finish) {
                assertEquals(currentQuota.get(), allowedQuota.get());
                // assert if we are blocked, we are going to be blocked again without changing allowedQuota
                assertFalse(addPageWork.process());
                assertEquals(currentQuota.get(), allowedQuota.get());
                yields++;
                allowedQuota.getAndAdd(6);
            }
        }

        // assert there is not anything missing
        assertEquals(dictionaryLength, groupByHash.getGroupCount());
        // assert we yield for every 3 rehashes
        // currentQuota is essentially the count we have successfully rehashed multiplied by 2 (as updateMemory is called twice per rehash)
        // the rehash count is 10 = log(1_000 / 0.75)
        assertEquals(currentQuota.get(), 10 * 2);
        assertEquals(currentQuota.get() / 3 / 2, yields);

        // test getGroupIds
        currentQuota.set(0);
        allowedQuota.set(6);
        yields = 0;
        groupByHash = groupByHashType.createGroupByHash(1, updateMemory);

        finish = false;
        Work<GroupByIdBlock> getGroupIdsWork = groupByHash.getGroupIds(page);
        while (!finish) {
            finish = getGroupIdsWork.process();
            if (!finish) {
                assertEquals(currentQuota.get(), allowedQuota.get());
                // assert if we are blocked, we are going to be blocked again without changing allowedQuota
                assertFalse(getGroupIdsWork.process());
                assertEquals(currentQuota.get(), allowedQuota.get());
                yields++;
                allowedQuota.getAndAdd(6);
            }
        }

        // assert there is not anything missing
        assertEquals(dictionaryLength, groupByHash.getGroupCount());
        assertEquals(dictionaryLength, getGroupIdsWork.getResult().getPositionCount());
        // assert we yield for every 3 rehashes
        // currentQuota is essentially the count we have successfully rehashed multiplied by 2 (as updateMemory is called twice per rehash)
        // the rehash count is 10 = log2(1_000 / 0.75)
        assertEquals(currentQuota.get(), 10 * 2);
        assertEquals(currentQuota.get() / 3 / 2, yields);
    }

    @Test
    public void testLowCardinalityDictionariesAddPage()
    {
        GroupByHash groupByHash = createGroupByHash(
                TEST_SESSION,
                ImmutableList.of(BIGINT, BIGINT),
                new int[] {0, 1},
                Optional.empty(),
                100,
                JOIN_COMPILER,
                TYPE_OPERATOR_FACTORY,
                NOOP);
        Block firstBlock = BlockAssertions.createLongDictionaryBlock(0, 1000, 10);
        Block secondBlock = BlockAssertions.createLongDictionaryBlock(0, 1000, 10);
        Page page = new Page(firstBlock, secondBlock);

        Work<?> work = groupByHash.addPage(page);
        assertThat(work).isInstanceOf(MultiChannelGroupByHash.AddLowCardinalityDictionaryPageWork.class);
        work.process();
        assertThat(groupByHash.getGroupCount()).isEqualTo(10); // Blocks are identical so only 10 distinct groups

        firstBlock = BlockAssertions.createLongDictionaryBlock(10, 1000, 5);
        secondBlock = BlockAssertions.createLongDictionaryBlock(10, 1000, 7);
        page = new Page(firstBlock, secondBlock);

        groupByHash.addPage(page).process();
        assertThat(groupByHash.getGroupCount()).isEqualTo(45); // Old 10 groups and 35 new
    }

    @Test
    public void testLowCardinalityDictionariesGetGroupIds()
    {
        // Compare group id results from page with dictionaries only (processed via low cardinality work) and the same page processed normally
        GroupByHash groupByHash = createGroupByHash(
                TEST_SESSION,
                ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT),
                new int[] {0, 1, 2, 3, 4},
                Optional.empty(),
                100,
                JOIN_COMPILER,
                TYPE_OPERATOR_FACTORY,
                NOOP);

        GroupByHash lowCardinalityGroupByHash = createGroupByHash(
                TEST_SESSION,
                ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT),
                new int[] {0, 1, 2, 3},
                Optional.empty(),
                100,
                JOIN_COMPILER,
                TYPE_OPERATOR_FACTORY,
                NOOP);
        Block sameValueBlock = BlockAssertions.createLongRepeatBlock(0, 100);
        Block block1 = BlockAssertions.createLongDictionaryBlock(0, 100, 1);
        Block block2 = BlockAssertions.createLongDictionaryBlock(0, 100, 2);
        Block block3 = BlockAssertions.createLongDictionaryBlock(0, 100, 3);
        Block block4 = BlockAssertions.createLongDictionaryBlock(0, 100, 4);
        // Combining block 2 and 4 will result in only 4 distinct values since 2 and 4 are not coprime

        Page lowCardinalityPage = new Page(block1, block2, block3, block4);
        Page page = new Page(block1, block2, block3, block4, sameValueBlock); // sameValueBlock will prevent low cardinality optimization to fire

        Work<GroupByIdBlock> lowCardinalityWork = lowCardinalityGroupByHash.getGroupIds(lowCardinalityPage);
        assertThat(lowCardinalityWork).isInstanceOf(GetLowCardinalityDictionaryGroupIdsWork.class);
        Work<GroupByIdBlock> work = groupByHash.getGroupIds(page);

        lowCardinalityWork.process();
        work.process();
        GroupByIdBlock lowCardinalityResults = lowCardinalityWork.getResult();
        GroupByIdBlock results = work.getResult();

        assertThat(lowCardinalityResults.getGroupCount()).isEqualTo(results.getGroupCount());
    }

    @Test
    public void testLowCardinalityDictionariesProperGroupIdOrder()
    {
        GroupByHash groupByHash = createGroupByHash(
                TEST_SESSION,
                ImmutableList.of(BIGINT, BIGINT),
                new int[] {0, 1},
                Optional.empty(),
                100,
                JOIN_COMPILER,
                TYPE_OPERATOR_FACTORY,
                NOOP);

        Block dictionary = new LongArrayBlock(2, Optional.empty(), new long[] {0, 1});
        int[] ids = new int[32];
        for (int i = 0; i < 16; i++) {
            ids[i] = 1;
        }
        Block block1 = DictionaryBlock.create(ids.length, dictionary, ids);
        Block block2 = DictionaryBlock.create(ids.length, dictionary, ids);

        Page page = new Page(block1, block2);

        Work<GroupByIdBlock> work = groupByHash.getGroupIds(page);
        assertThat(work).isInstanceOf(GetLowCardinalityDictionaryGroupIdsWork.class);

        work.process();
        GroupByIdBlock results = work.getResult();
        // Records with group id '0' should come before '1' despite being in the end of the block
        for (int i = 0; i < 16; i++) {
            assertThat(results.getGroupId(i)).isEqualTo(0);
        }
        for (int i = 16; i < 32; i++) {
            assertThat(results.getGroupId(i)).isEqualTo(1);
        }
    }

    @Test
    public void testProperWorkTypesSelected()
    {
        Block bigintBlock = BlockAssertions.createLongsBlock(1, 2, 3, 4, 5, 6, 7, 8);
        Block bigintDictionaryBlock = BlockAssertions.createLongDictionaryBlock(0, 8);
        Block bigintRleBlock = BlockAssertions.createRepeatedValuesBlock(42, 8);
        Block varcharBlock = BlockAssertions.createStringsBlock("1", "2", "3", "4", "5", "6", "7", "8");
        Block varcharDictionaryBlock = BlockAssertions.createStringDictionaryBlock(1, 8);
        Block varcharRleBlock = RunLengthEncodedBlock.create(new VariableWidthBlock(1, Slices.EMPTY_SLICE, new int[] {0, 1}, Optional.empty()), 8);
        Block bigintBigDictionaryBlock = BlockAssertions.createLongDictionaryBlock(1, 8, 1000);
        Block bigintSingletonDictionaryBlock = BlockAssertions.createLongDictionaryBlock(1, 500000, 1);
        Block bigintHugeDictionaryBlock = BlockAssertions.createLongDictionaryBlock(1, 500000, 66000); // Above Short.MAX_VALUE

        Page singleBigintPage = new Page(bigintBlock);
        assertGroupByHashWork(singleBigintPage, ImmutableList.of(BIGINT), BigintGroupByHash.GetGroupIdsWork.class);
        Page singleBigintDictionaryPage = new Page(bigintDictionaryBlock);
        assertGroupByHashWork(singleBigintDictionaryPage, ImmutableList.of(BIGINT), BigintGroupByHash.GetDictionaryGroupIdsWork.class);
        Page singleBigintRlePage = new Page(bigintRleBlock);
        assertGroupByHashWork(singleBigintRlePage, ImmutableList.of(BIGINT), BigintGroupByHash.GetRunLengthEncodedGroupIdsWork.class);
        Page singleVarcharPage = new Page(varcharBlock);
        assertGroupByHashWork(singleVarcharPage, ImmutableList.of(VARCHAR), MultiChannelGroupByHash.GetNonDictionaryGroupIdsWork.class);
        Page singleVarcharDictionaryPage = new Page(varcharDictionaryBlock);
        assertGroupByHashWork(singleVarcharDictionaryPage, ImmutableList.of(VARCHAR), MultiChannelGroupByHash.GetDictionaryGroupIdsWork.class);
        Page singleVarcharRlePage = new Page(varcharRleBlock);
        assertGroupByHashWork(singleVarcharRlePage, ImmutableList.of(VARCHAR), MultiChannelGroupByHash.GetRunLengthEncodedGroupIdsWork.class);

        Page lowCardinalityDictionaryPage = new Page(bigintDictionaryBlock, varcharDictionaryBlock);
        assertGroupByHashWork(lowCardinalityDictionaryPage, ImmutableList.of(BIGINT, VARCHAR), MultiChannelGroupByHash.GetLowCardinalityDictionaryGroupIdsWork.class);
        Page highCardinalityDictionaryPage = new Page(bigintDictionaryBlock, bigintBigDictionaryBlock);
        assertGroupByHashWork(highCardinalityDictionaryPage, ImmutableList.of(BIGINT, VARCHAR), MultiChannelGroupByHash.GetNonDictionaryGroupIdsWork.class);

        // Cardinality above Short.MAX_VALUE
        Page lowCardinalityHugeDictionaryPage = new Page(bigintSingletonDictionaryBlock, bigintHugeDictionaryBlock);
        assertGroupByHashWork(lowCardinalityHugeDictionaryPage, ImmutableList.of(BIGINT, BIGINT), MultiChannelGroupByHash.GetNonDictionaryGroupIdsWork.class);
    }

    private void assertGroupByHashWork(Page page, List<Type> types, Class<?> clazz)
    {
        GroupByHash groupByHash = createGroupByHash(
                types,
                IntStream.range(0, types.size()).toArray(),
                Optional.empty(),
                100,
                true,
                JOIN_COMPILER,
                TYPE_OPERATOR_FACTORY,
                NOOP);
        Work<GroupByIdBlock> work = groupByHash.getGroupIds(page);
        // Compare by name since classes are private
        assertThat(work).isInstanceOf(clazz);
    }
}
