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
import io.trino.spi.block.DictionaryId;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.Type;
import io.trino.testing.TestingSession;
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
import static io.trino.block.BlockAssertions.createIntsBlock;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createStringSequenceBlock;
import static io.trino.operator.GroupByHashFactoryTestUtils.createGroupByHashFactory;
import static io.trino.operator.UpdateMemory.NOOP;
import static io.trino.spi.block.DictionaryId.randomDictionaryId;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.TypeTestUtils.getHashBlock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestGroupByHash
{
    private static final int MAX_GROUP_ID = 500;
    private static final Session TEST_SESSION = TestingSession.testSessionBuilder().build();
    private static final GroupByHashFactory GROUP_BY_HASH_FACTORY = createGroupByHashFactory();

    @DataProvider
    public Object[][] dataType()
    {
        return new Object[][] {{VARCHAR}, {BIGINT}};
    }

    @Test
    public void testAddPage()
    {
        GroupByHash groupByHash = GROUP_BY_HASH_FACTORY.createGroupByHash(TEST_SESSION, ImmutableList.of(BIGINT), new int[] {0}, Optional.of(1), 100, NOOP);
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

    @Test
    public void testRunLengthEncodedBigintGroupByHash()
    {
        GroupByHash groupByHash = GROUP_BY_HASH_FACTORY.createGroupByHash(TEST_SESSION, ImmutableList.of(BIGINT), new int[] {0}, Optional.of(1), 100, NOOP);
        Block block = BlockAssertions.createLongsBlock(0L);
        Block hashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(BIGINT), block);
        Page page = new Page(
                new RunLengthEncodedBlock(block, 2),
                new RunLengthEncodedBlock(hashBlock, 2));

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

    @Test
    public void testDictionaryBigintGroupByHash()
    {
        GroupByHash groupByHash = GROUP_BY_HASH_FACTORY.createGroupByHash(TEST_SESSION, ImmutableList.of(BIGINT), new int[] {0}, Optional.of(1), 100, NOOP);
        Block block = BlockAssertions.createLongsBlock(0L, 1L);
        Block hashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(BIGINT), block);
        int[] ids = new int[] {0, 0, 1, 1};
        Page page = new Page(
                new DictionaryBlock(block, ids),
                new DictionaryBlock(hashBlock, ids));

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

    @Test
    public void testNullGroup()
    {
        GroupByHash groupByHash = GROUP_BY_HASH_FACTORY.createGroupByHash(TEST_SESSION, ImmutableList.of(BIGINT), new int[] {0}, Optional.of(1), 100, NOOP);

        Block block = createLongsBlock((Long) null);
        Block hashBlock = getHashBlock(ImmutableList.of(BIGINT), block);
        Page page = new Page(block, hashBlock);
        groupByHash.addPage(page).process();
        assertEquals(groupByHash.getHashCollisions(), 0);

        // Add enough values to force a rehash
        block = createLongSequenceBlock(1, 132748);
        hashBlock = getHashBlock(ImmutableList.of(BIGINT), block);
        page = new Page(block, hashBlock);
        groupByHash.addPage(page).process();

        block = createLongsBlock(0);
        hashBlock = getHashBlock(ImmutableList.of(BIGINT), block);
        page = new Page(block, hashBlock);
        assertFalse(groupByHash.contains(0, page));
    }

    @Test
    public void testNullGroupInt()
    {
        GroupByHash groupByHash = GROUP_BY_HASH_FACTORY.createGroupByHash(TEST_SESSION, ImmutableList.of(INTEGER), new int[] {0}, Optional.of(1), 100, NOOP);

        Block block = createIntsBlock((Integer) null);
        Block hashBlock = getHashBlock(ImmutableList.of(INTEGER), block);
        Page page = new Page(block, hashBlock);
        groupByHash.addPage(page).process();
        assertEquals(groupByHash.getHashCollisions(), 0);

        // Add enough values to force a rehash
        block = createIntsBlock(1, 132748);
        hashBlock = getHashBlock(ImmutableList.of(INTEGER), block);
        page = new Page(block, hashBlock);
        groupByHash.addPage(page).process();

        block = createIntsBlock(0);
        hashBlock = getHashBlock(ImmutableList.of(INTEGER), block);
        page = new Page(block, hashBlock);
        assertFalse(groupByHash.contains(0, page));
    }

    @Test
    public void testGetGroupIds()
    {
        GroupByHash groupByHash = GROUP_BY_HASH_FACTORY.createGroupByHash(TEST_SESSION, ImmutableList.of(BIGINT), new int[] {0}, Optional.of(1), 100, NOOP);
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
        GroupByHash groupByHash = GROUP_BY_HASH_FACTORY.createGroupByHash(TEST_SESSION, ImmutableList.of(VARCHAR), new int[] {0}, Optional.of(1), 100, NOOP);
        // Additional bigint channel for hash
        assertEquals(groupByHash.getTypes(), ImmutableList.of(VARCHAR, BIGINT));
    }

    @Test
    public void testAppendTo()
    {
        Block valuesBlock = BlockAssertions.createStringSequenceBlock(0, 100);
        Block hashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(VARCHAR), valuesBlock);
        GroupByHash groupByHash = GROUP_BY_HASH_FACTORY.createGroupByHash(TEST_SESSION, ImmutableList.of(VARCHAR), new int[] {0}, Optional.of(1), 100, NOOP);

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
        BlockAssertions.assertBlockEquals(VARCHAR, page.getBlock(0), valuesBlock);
        BlockAssertions.assertBlockEquals(BIGINT, page.getBlock(1), hashBlock);
    }

    @Test
    public void testAppendToMultipleTuplesPerGroup()
    {
        List<Long> values = new ArrayList<>();
        for (long i = 0; i < 100; i++) {
            values.add(i % 50);
        }
        Block valuesBlock = BlockAssertions.createLongsBlock(values);
        Block hashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(BIGINT), valuesBlock);

        GroupByHash groupByHash = GROUP_BY_HASH_FACTORY.createGroupByHash(TEST_SESSION, ImmutableList.of(BIGINT), new int[] {0}, Optional.of(1), 100, NOOP);
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

    @Test
    public void testContains()
    {
        Block valuesBlock = BlockAssertions.createDoubleSequenceBlock(0, 10);
        Block hashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(DOUBLE), valuesBlock);
        GroupByHash groupByHash = GROUP_BY_HASH_FACTORY.createGroupByHash(TEST_SESSION, ImmutableList.of(DOUBLE), new int[] {0}, Optional.of(1), 100, NOOP);
        groupByHash.getGroupIds(new Page(valuesBlock, hashBlock)).process();

        Block testBlock = BlockAssertions.createDoublesBlock((double) 3);
        Block testHashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(DOUBLE), testBlock);
        assertTrue(groupByHash.contains(0, new Page(testBlock, testHashBlock)));

        testBlock = BlockAssertions.createDoublesBlock(11.0);
        testHashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(DOUBLE), testBlock);
        assertFalse(groupByHash.contains(0, new Page(testBlock, testHashBlock)));
    }

    @Test
    public void testContainsMultipleColumns()
    {
        Block valuesBlock = BlockAssertions.createDoubleSequenceBlock(0, 10);
        Block stringValuesBlock = BlockAssertions.createStringSequenceBlock(0, 10);
        Block hashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(DOUBLE, VARCHAR), valuesBlock, stringValuesBlock);
        int[] hashChannels = {0, 1};
        GroupByHash groupByHash = GROUP_BY_HASH_FACTORY.createGroupByHash(TEST_SESSION, ImmutableList.of(DOUBLE, VARCHAR), hashChannels, Optional.of(2), 100, NOOP);
        groupByHash.getGroupIds(new Page(valuesBlock, stringValuesBlock, hashBlock)).process();

        Block testValuesBlock = BlockAssertions.createDoublesBlock((double) 3);
        Block testStringValuesBlock = BlockAssertions.createStringsBlock("3");
        Block testHashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(DOUBLE, VARCHAR), testValuesBlock, testStringValuesBlock);
        assertTrue(groupByHash.contains(0, new Page(testValuesBlock, testStringValuesBlock, testHashBlock)));
    }

    @Test
    public void testForceRehash()
    {
        // Create a page with positionCount >> expected size of groupByHash
        Block valuesBlock = BlockAssertions.createStringSequenceBlock(0, 100);
        Block hashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(VARCHAR), valuesBlock);

        // Create group by hash with extremely small size
        GroupByHash groupByHash = GROUP_BY_HASH_FACTORY.createGroupByHash(TEST_SESSION, ImmutableList.of(VARCHAR), new int[] {0}, Optional.of(1), 4, NOOP);
        groupByHash.getGroupIds(new Page(valuesBlock, hashBlock)).process();

        // Ensure that all groups are present in group by hash
        for (int i = 0; i < valuesBlock.getPositionCount(); i++) {
            assertTrue(groupByHash.contains(i, new Page(valuesBlock, hashBlock)));
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
        GroupByHash groupByHash = GROUP_BY_HASH_FACTORY.createGroupByHash(
                ImmutableList.of(type),
                new int[] {0},
                Optional.of(1),
                1,
                false,
                () -> {
                    rehashCount.incrementAndGet();
                    return true;
                });
        groupByHash.addPage(new Page(valuesBlock, hashBlock)).process();

        // assert we call update memory every time we rehash; the rehash count = log2(length / FILL_RATIO)
        assertEquals(rehashCount.get(), log2(length / 0.75, RoundingMode.FLOOR));
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
        AtomicInteger allowedQuota = new AtomicInteger(3);
        UpdateMemory updateMemory = () -> {
            if (currentQuota.get() < allowedQuota.get()) {
                currentQuota.getAndIncrement();
                return true;
            }
            return false;
        };
        int yields = 0;

        // test addPage
        GroupByHash groupByHash = GROUP_BY_HASH_FACTORY.createGroupByHash(ImmutableList.of(type), new int[] {0}, Optional.of(1), 1, false, updateMemory);
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
                allowedQuota.getAndAdd(3);
            }
        }

        // assert there is not anything missing
        assertEquals(length, groupByHash.getGroupCount());
        // assert we yield for every 3 rehashes
        // currentQuota is essentially the count we have successfully rehashed
        // the rehash count is 20 = log(1_000_000 / 0.75)
        assertEquals(currentQuota.get(), 20);
        assertEquals(currentQuota.get() / 3, yields);

        // test getGroupIds
        currentQuota.set(0);
        allowedQuota.set(3);
        yields = 0;
        groupByHash = GROUP_BY_HASH_FACTORY.createGroupByHash(ImmutableList.of(type), new int[] {0}, Optional.of(1), 1, false, updateMemory);

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
                allowedQuota.getAndAdd(3);
            }
        }
        // assert there is not anything missing
        assertEquals(length, groupByHash.getGroupCount());
        assertEquals(length, getGroupIdsWork.getResult().getPositionCount());
        // assert we yield for every 3 rehashes
        // currentQuota is essentially the count we have successfully rehashed
        // the rehash count is 20 = log2(1_000_000 / 0.75)
        assertEquals(currentQuota.get(), 20);
        assertEquals(currentQuota.get() / 3, yields);
    }

    @Test
    public void testMemoryReservationYieldWithDictionary()
    {
        // Create a page with positionCount >> expected size of groupByHash
        int dictionaryLength = 1_000;
        int length = 2_000_000;
        int[] ids = IntStream.range(0, dictionaryLength).toArray();
        DictionaryId dictionaryId = randomDictionaryId();
        Block valuesBlock = new DictionaryBlock(dictionaryLength, createStringSequenceBlock(0, length), ids, dictionaryId);
        Block hashBlock = new DictionaryBlock(dictionaryLength, getHashBlock(ImmutableList.of(VARCHAR), valuesBlock), ids, dictionaryId);
        Page page = new Page(valuesBlock, hashBlock);
        AtomicInteger currentQuota = new AtomicInteger(0);
        AtomicInteger allowedQuota = new AtomicInteger(3);
        UpdateMemory updateMemory = () -> {
            if (currentQuota.get() < allowedQuota.get()) {
                currentQuota.getAndIncrement();
                return true;
            }
            return false;
        };
        int yields = 0;

        // test addPage
        GroupByHash groupByHash = GROUP_BY_HASH_FACTORY.createGroupByHash(ImmutableList.of(VARCHAR), new int[] {0}, Optional.of(1), 1, true, updateMemory);

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
                allowedQuota.getAndAdd(3);
            }
        }

        // assert there is not anything missing
        assertEquals(dictionaryLength, groupByHash.getGroupCount());
        // assert we yield for every 3 rehashes
        // currentQuota is essentially the count we have successfully rehashed
        // the rehash count is 10 = log(1_000 / 0.75)
        assertEquals(currentQuota.get(), 10);
        assertEquals(currentQuota.get() / 3, yields);

        // test getGroupIds
        currentQuota.set(0);
        allowedQuota.set(3);
        yields = 0;
        groupByHash = GROUP_BY_HASH_FACTORY.createGroupByHash(ImmutableList.of(VARCHAR), new int[] {0}, Optional.of(1), 1, true, updateMemory);

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
                allowedQuota.getAndAdd(3);
            }
        }

        // assert there is not anything missing
        assertEquals(dictionaryLength, groupByHash.getGroupCount());
        assertEquals(dictionaryLength, getGroupIdsWork.getResult().getPositionCount());
        // assert we yield for every 3 rehashes
        // currentQuota is essentially the count we have successfully rehashed
        // the rehash count is 10 = log2(1_000 / 0.75)
        assertEquals(currentQuota.get(), 10);
        assertEquals(currentQuota.get() / 3, yields);
    }

    @Test
    public void testLowCardinalityDictionariesAddPage()
    {
        GroupByHash groupByHash = GROUP_BY_HASH_FACTORY.createGroupByHash(
                TEST_SESSION,
                ImmutableList.of(BIGINT, BIGINT),
                new int[] {0, 1},
                Optional.empty(),
                100,
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
        GroupByHash groupByHash = GROUP_BY_HASH_FACTORY.createGroupByHash(
                TEST_SESSION,
                ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT),
                new int[] {0, 1, 2, 3, 4},
                Optional.empty(),
                100,
                NOOP);

        GroupByHash lowCardinalityGroupByHash = GROUP_BY_HASH_FACTORY.createGroupByHash(
                TEST_SESSION,
                ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT),
                new int[] {0, 1, 2, 3},
                Optional.empty(),
                100,
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
    public void testProperWorkTypesSelected()
    {
        Block bigintBlock = BlockAssertions.createLongsBlock(1, 2, 3, 4, 5, 6, 7, 8);
        Block bigintDictionaryBlock = BlockAssertions.createLongDictionaryBlock(0, 8);
        Block bigintRleBlock = BlockAssertions.createRLEBlock(42, 8);
        Block varcharBlock = BlockAssertions.createStringsBlock("1", "2", "3", "4", "5", "6", "7", "8");
        Block varcharDictionaryBlock = BlockAssertions.createStringDictionaryBlock(1, 8);
        Block varcharRleBlock = new RunLengthEncodedBlock(new VariableWidthBlock(1, Slices.EMPTY_SLICE, new int[] {0, 1}, Optional.empty()), 8);
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
        GroupByHash groupByHash = GROUP_BY_HASH_FACTORY.createGroupByHash(
                types,
                IntStream.range(0, types.size()).toArray(),
                Optional.empty(),
                100,
                true,
                NOOP);
        Work<GroupByIdBlock> work = groupByHash.getGroupIds(page);
        // Compare by name since classes are private
        assertThat(work).isInstanceOf(clazz);
    }
}
