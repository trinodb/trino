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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createStringSequenceBlock;
import static io.trino.operator.GroupByHash.createGroupByHash;
import static io.trino.operator.UpdateMemory.NOOP;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.TypeTestUtils.getHashBlock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestGroupByHash
{
    private static final int MAX_GROUP_ID = 500;
    private static final Session TEST_SESSION = TestingSession.testSessionBuilder().build();
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final JoinCompiler JOIN_COMPILER = new JoinCompiler(TYPE_OPERATORS);
    private static final int BIGINT_EXPECTED_REHASH = 20;
    // first rehash moves from the initial capacity to 1024 (batch size) and last hash moves to 1024 * 1024,
    // which is 1 initial rehash + 10 additional rehashes
    private static final int VARCHAR_EXPECTED_REHASH = 11;

    @DataProvider
    public Object[][] dataType()
    {
        return new Object[][] {{VARCHAR}, {BIGINT}};
    }

    @DataProvider
    public Object[][] groupByHashType()
    {
        return new Object[][] {{GroupByHashType.BIGINT}, {GroupByHashType.MULTI_CHANNEL}, {GroupByHashType.FLAT}};
    }

    private enum GroupByHashType
    {
        BIGINT, MULTI_CHANNEL, FLAT;

        public GroupByHash createGroupByHash()
        {
            return createGroupByHash(100, NOOP);
        }

        public GroupByHash createGroupByHash(int expectedSize, UpdateMemory updateMemory)
        {
            return switch (this) {
                case BIGINT -> new BigintGroupByHash(true, expectedSize, updateMemory);
                case MULTI_CHANNEL -> new MultiChannelGroupByHash(
                        ImmutableList.of(BigintType.BIGINT),
                        true,
                        expectedSize,
                        true,
                        JOIN_COMPILER,
                        TYPE_OPERATORS,
                        updateMemory);
                case FLAT -> new FlatGroupByHash(
                        ImmutableList.of(BigintType.BIGINT),
                        true,
                        expectedSize,
                        true,
                        JOIN_COMPILER,
                        updateMemory);
            };
        }
    }

    @Test(dataProvider = "groupByHashType")
    public void testAddPage(GroupByHashType groupByHashType)
    {
        GroupByHash groupByHash = groupByHashType.createGroupByHash();
        for (int tries = 0; tries < 2; tries++) {
            for (int value = 0; value < MAX_GROUP_ID; value++) {
                Block block = createLongsBlock(value);
                Block hashBlock = getHashBlock(ImmutableList.of(BIGINT), block);
                Page page = new Page(block, hashBlock);
                for (int addValuesTries = 0; addValuesTries < 10; addValuesTries++) {
                    groupByHash.addPage(page).process();
                    assertEquals(groupByHash.getGroupCount(), tries == 0 ? value + 1 : MAX_GROUP_ID);

                    // add the page again using get group ids and make sure the group count didn't change
                    int[] groupIds = getGroupIds(groupByHash, page);
                    assertEquals(groupByHash.getGroupCount(), tries == 0 ? value + 1 : MAX_GROUP_ID);

                    // verify the first position
                    assertEquals(groupIds.length, 1);
                    int groupId = groupIds[0];
                    assertEquals(groupId, value);
                }
            }
        }
    }

    @Test(dataProvider = "groupByHashType")
    public void testRunLengthEncodedInputPage(GroupByHashType groupByHashType)
    {
        GroupByHash groupByHash = groupByHashType.createGroupByHash();
        Block block = createLongsBlock(0L);
        Block hashBlock = getHashBlock(ImmutableList.of(BIGINT), block);
        Page page = new Page(
                RunLengthEncodedBlock.create(block, 2),
                RunLengthEncodedBlock.create(hashBlock, 2));

        groupByHash.addPage(page).process();

        assertEquals(groupByHash.getGroupCount(), 1);

        Work<int[]> work = groupByHash.getGroupIds(page);
        switch (groupByHashType) {
            case FLAT -> assertThat(work).isInstanceOf(FlatGroupByHash.GetRunLengthEncodedGroupIdsWork.class);
            case MULTI_CHANNEL -> assertThat(work).isInstanceOf(MultiChannelGroupByHash.GetRunLengthEncodedGroupIdsWork.class);
            default -> assertThat(work).isInstanceOf(BigintGroupByHash.GetRunLengthEncodedGroupIdsWork.class);
        }
        work.process();
        int[] groupIds = work.getResult();

        assertEquals(groupByHash.getGroupCount(), 1);
        assertEquals(groupIds.length, 2);
        assertEquals(groupIds[0], 0);
        assertEquals(groupIds[1], 0);
    }

    @Test(dataProvider = "groupByHashType")
    public void testDictionaryInputPage(GroupByHashType groupByHashType)
    {
        GroupByHash groupByHash = groupByHashType.createGroupByHash();
        Block block = createLongsBlock(0L, 1L);
        Block hashBlock = getHashBlock(ImmutableList.of(BIGINT), block);
        int[] ids = new int[] {0, 0, 1, 1};
        Page page = new Page(
                DictionaryBlock.create(ids.length, block, ids),
                DictionaryBlock.create(ids.length, hashBlock, ids));

        groupByHash.addPage(page).process();

        assertEquals(groupByHash.getGroupCount(), 2);

        int[] groupIds = getGroupIds(groupByHash, page);
        assertEquals(groupByHash.getGroupCount(), 2);
        assertEquals(groupIds.length, 4);
        assertEquals(groupIds[0], 0);
        assertEquals(groupIds[1], 0);
        assertEquals(groupIds[2], 1);
        assertEquals(groupIds[3], 1);
    }

    @Test(dataProvider = "groupByHashType")
    public void testNullGroup(GroupByHashType groupByHashType)
    {
        GroupByHash groupByHash = groupByHashType.createGroupByHash();

        Block block = createLongsBlock(0L, null);
        Block hashBlock = getHashBlock(ImmutableList.of(BIGINT), block);
        Page page = new Page(block, hashBlock);
        // assign null a groupId (which is one since is it the second value added)
        assertThat(getGroupIds(groupByHash, page))
                .containsExactly(0, 1);

        // Add enough values to force a rehash
        block = createLongSequenceBlock(1, 132748);
        hashBlock = getHashBlock(ImmutableList.of(BIGINT), block);
        page = new Page(block, hashBlock);
        groupByHash.addPage(page).process();

        block = createLongsBlock((Long) null);
        hashBlock = getHashBlock(ImmutableList.of(BIGINT), block);
        // null groupId will be 0 (as set above)
        assertThat(getGroupIds(groupByHash, new Page(block, hashBlock)))
                .containsExactly(1);
    }

    @Test(dataProvider = "groupByHashType")
    public void testGetGroupIds(GroupByHashType groupByHashType)
    {
        GroupByHash groupByHash = groupByHashType.createGroupByHash();
        for (int tries = 0; tries < 2; tries++) {
            for (int value = 0; value < MAX_GROUP_ID; value++) {
                Block block = createLongsBlock(value);
                Block hashBlock = getHashBlock(ImmutableList.of(BIGINT), block);
                Page page = new Page(block, hashBlock);
                for (int addValuesTries = 0; addValuesTries < 10; addValuesTries++) {
                    int[] groupIds = getGroupIds(groupByHash, page);
                    assertEquals(groupByHash.getGroupCount(), tries == 0 ? value + 1 : MAX_GROUP_ID);
                    assertEquals(groupIds.length, 1);
                    long groupId = groupIds[0];
                    assertEquals(groupId, value);
                }
            }
        }
    }

    @Test(dataProvider = "groupByHashType")
    public void testAppendTo(GroupByHashType groupByHashType)
    {
        Block valuesBlock = createLongSequenceBlock(0, 100);
        Block hashBlock = getHashBlock(ImmutableList.of(BIGINT), valuesBlock);
        GroupByHash groupByHash = groupByHashType.createGroupByHash();

        int[] groupIds = getGroupIds(groupByHash, new Page(valuesBlock, hashBlock));
        for (int i = 0; i < valuesBlock.getPositionCount(); i++) {
            assertEquals(groupIds[i], i);
        }
        assertEquals(groupByHash.getGroupCount(), 100);

        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BIGINT, BIGINT));
        for (int i = 0; i < groupByHash.getGroupCount(); i++) {
            pageBuilder.declarePosition();
            groupByHash.appendValuesTo(i, pageBuilder);
        }
        Page page = pageBuilder.build();
        // Ensure that all blocks have the same positionCount
        for (int i = 0; i < page.getChannelCount(); i++) {
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
        Block valuesBlock = createLongsBlock(values);
        Block hashBlock = getHashBlock(ImmutableList.of(BIGINT), valuesBlock);

        GroupByHash groupByHash = groupByHashType.createGroupByHash();
        groupByHash.getGroupIds(new Page(valuesBlock, hashBlock)).process();
        assertEquals(groupByHash.getGroupCount(), 50);

        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BIGINT, BIGINT));
        for (int i = 0; i < groupByHash.getGroupCount(); i++) {
            pageBuilder.declarePosition();
            groupByHash.appendValuesTo(i, pageBuilder);
        }
        Page outputPage = pageBuilder.build();
        assertEquals(outputPage.getPositionCount(), 50);
        BlockAssertions.assertBlockEquals(BIGINT, outputPage.getBlock(0), createLongSequenceBlock(0, 50));
    }

    @Test(dataProvider = "groupByHashType")
    public void testForceRehash(GroupByHashType groupByHashType)
    {
        // Create a page with positionCount >> expected size of groupByHash
        Block valuesBlock = createLongSequenceBlock(0, 100);
        Block hashBlock = getHashBlock(ImmutableList.of(BIGINT), valuesBlock);

        // Create GroupByHash with tiny size
        GroupByHash groupByHash = groupByHashType.createGroupByHash(4, NOOP);
        groupByHash.getGroupIds(new Page(valuesBlock, hashBlock)).process();

        // Ensure that all groups are present in GroupByHash
        int groupCount = groupByHash.getGroupCount();
        for (int groupId : getGroupIds(groupByHash, new Page(valuesBlock, hashBlock))) {
            assertThat(groupId).isLessThan(groupCount);
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

        // Create GroupByHash with tiny size
        AtomicInteger rehashCount = new AtomicInteger();
        GroupByHash groupByHash = createGroupByHash(true, ImmutableList.of(type), true, 1, false, JOIN_COMPILER, TYPE_OPERATORS, () -> {
            rehashCount.incrementAndGet();
            return true;
        });
        groupByHash.addPage(new Page(valuesBlock, hashBlock)).process();

        // assert we call update memory twice every time we rehash; the rehash count = log2(length / FILL_RATIO)
        assertEquals(rehashCount.get(), 2 * (type == VARCHAR ? VARCHAR_EXPECTED_REHASH : BIGINT_EXPECTED_REHASH));
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
        GroupByHash groupByHash = createGroupByHash(true, ImmutableList.of(type), true, 1, false, JOIN_COMPILER, TYPE_OPERATORS, updateMemory);
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
        assertEquals(currentQuota.get(), 2 * (type == VARCHAR ? VARCHAR_EXPECTED_REHASH : BIGINT_EXPECTED_REHASH));
        assertEquals(currentQuota.get() / 3 / 2, yields);

        // test getGroupIds
        currentQuota.set(0);
        allowedQuota.set(6);
        yields = 0;
        groupByHash = createGroupByHash(true, ImmutableList.of(type), true, 1, false, JOIN_COMPILER, TYPE_OPERATORS, updateMemory);

        finish = false;
        Work<int[]> getGroupIdsWork = groupByHash.getGroupIds(page);
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
        assertEquals(length, getGroupIdsWork.getResult().length);
        // rehash count is the same as above
        assertEquals(currentQuota.get(), 2 * (type == VARCHAR ? VARCHAR_EXPECTED_REHASH : BIGINT_EXPECTED_REHASH));
        assertEquals(currentQuota.get() / 3 / 2, yields);
    }

    @Test(dataProvider = "groupByHashType")
    public void testMemoryReservationYieldWithDictionary(GroupByHashType groupByHashType)
    {
        // Create a page with positionCount >> expected size of groupByHash
        int dictionaryLength = 10_000;
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
        assertEquals(currentQuota.get(), 2 * (groupByHashType == GroupByHashType.FLAT ? 4 : 13));
        assertEquals(currentQuota.get() / 3 / 2, yields);

        // test getGroupIds
        currentQuota.set(0);
        allowedQuota.set(6);
        yields = 0;
        groupByHash = groupByHashType.createGroupByHash(1, updateMemory);

        finish = false;
        Work<int[]> getGroupIdsWork = groupByHash.getGroupIds(page);
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
        assertEquals(dictionaryLength, getGroupIdsWork.getResult().length);
        // assert we yield for every 3 rehashes
        // currentQuota is essentially the count we have successfully rehashed multiplied by 2 (as updateMemory is called twice per rehash)
        // the rehash count is 10 = log2(1_000 / 0.75)
        assertEquals(currentQuota.get(), 2 * (groupByHashType == GroupByHashType.FLAT ? 4 : 13));
        assertEquals(currentQuota.get() / 3 / 2, yields);
    }

    @Test
    public void testLowCardinalityDictionariesAddPage()
    {
        GroupByHash groupByHash = createGroupByHash(
                TEST_SESSION,
                ImmutableList.of(BIGINT, BIGINT),
                false,
                100,
                JOIN_COMPILER,
                TYPE_OPERATORS,
                NOOP);
        Block firstBlock = BlockAssertions.createLongDictionaryBlock(0, 1000, 10);
        Block secondBlock = BlockAssertions.createLongDictionaryBlock(0, 1000, 10);
        Page page = new Page(firstBlock, secondBlock);

        Work<?> work = groupByHash.addPage(page);
        assertThat(work).isInstanceOf(FlatGroupByHash.AddLowCardinalityDictionaryPageWork.class);
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
        // Compare group ids results from page with dictionaries only (processed via low cardinality work) and the same page processed normally
        GroupByHash groupByHash = createGroupByHash(
                TEST_SESSION,
                ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT),
                false,
                100,
                JOIN_COMPILER,
                TYPE_OPERATORS,
                NOOP);

        GroupByHash lowCardinalityGroupByHash = createGroupByHash(
                TEST_SESSION,
                ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT),
                false,
                100,
                JOIN_COMPILER,
                TYPE_OPERATORS,
                NOOP);
        Block sameValueBlock = BlockAssertions.createLongRepeatBlock(0, 100);
        Block block1 = BlockAssertions.createLongDictionaryBlock(0, 100, 1);
        Block block2 = BlockAssertions.createLongDictionaryBlock(0, 100, 2);
        Block block3 = BlockAssertions.createLongDictionaryBlock(0, 100, 3);
        Block block4 = BlockAssertions.createLongDictionaryBlock(0, 100, 4);
        // Combining block 2 and 4 will result in only 4 distinct values since 2 and 4 are not coprime

        Page lowCardinalityPage = new Page(block1, block2, block3, block4);
        Page page = new Page(block1, block2, block3, block4, sameValueBlock); // sameValueBlock will prevent low cardinality optimization to fire

        Work<int[]> lowCardinalityWork = lowCardinalityGroupByHash.getGroupIds(lowCardinalityPage);
        assertThat(lowCardinalityWork).isInstanceOf(FlatGroupByHash.GetLowCardinalityDictionaryGroupIdsWork.class);
        Work<int[]> work = groupByHash.getGroupIds(page);

        lowCardinalityWork.process();
        work.process();

        assertThat(lowCardinalityGroupByHash.getGroupCount()).isEqualTo(groupByHash.getGroupCount());
        int[] lowCardinalityResults = lowCardinalityWork.getResult();
        int[] results = work.getResult();
        assertThat(lowCardinalityResults).isEqualTo(results);
    }

    @Test
    public void testLowCardinalityDictionariesProperGroupIdOrder()
    {
        GroupByHash groupByHash = createGroupByHash(
                TEST_SESSION,
                ImmutableList.of(BIGINT, BIGINT),
                false,
                100,
                JOIN_COMPILER,
                TYPE_OPERATORS,
                NOOP);

        Block dictionary = new LongArrayBlock(2, Optional.empty(), new long[] {0, 1});
        int[] ids = new int[32];
        for (int i = 0; i < 16; i++) {
            ids[i] = 1;
        }
        Block block1 = DictionaryBlock.create(ids.length, dictionary, ids);
        Block block2 = DictionaryBlock.create(ids.length, dictionary, ids);

        Page page = new Page(block1, block2);

        Work<int[]> work = groupByHash.getGroupIds(page);
        assertThat(work).isInstanceOf(FlatGroupByHash.GetLowCardinalityDictionaryGroupIdsWork.class);

        work.process();
        int[] results = work.getResult();
        // Records with group id '0' should come before '1' despite being at the end of the block
        for (int i = 0; i < 16; i++) {
            assertThat(results[i]).isEqualTo(0);
        }
        for (int i = 16; i < 32; i++) {
            assertThat(results[i]).isEqualTo(1);
        }
    }

    @Test
    public void testProperWorkTypesSelected()
    {
        Block bigintBlock = createLongsBlock(1, 2, 3, 4, 5, 6, 7, 8);
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
        assertGroupByHashWork(singleVarcharPage, ImmutableList.of(VARCHAR), FlatGroupByHash.GetNonDictionaryGroupIdsWork.class);
        Page singleVarcharDictionaryPage = new Page(varcharDictionaryBlock);
        assertGroupByHashWork(singleVarcharDictionaryPage, ImmutableList.of(VARCHAR), FlatGroupByHash.GetDictionaryGroupIdsWork.class);
        Page singleVarcharRlePage = new Page(varcharRleBlock);
        assertGroupByHashWork(singleVarcharRlePage, ImmutableList.of(VARCHAR), FlatGroupByHash.GetRunLengthEncodedGroupIdsWork.class);

        Page lowCardinalityDictionaryPage = new Page(bigintDictionaryBlock, varcharDictionaryBlock);
        assertGroupByHashWork(lowCardinalityDictionaryPage, ImmutableList.of(BIGINT, VARCHAR), FlatGroupByHash.GetLowCardinalityDictionaryGroupIdsWork.class);
        Page highCardinalityDictionaryPage = new Page(bigintDictionaryBlock, bigintBigDictionaryBlock);
        assertGroupByHashWork(highCardinalityDictionaryPage, ImmutableList.of(BIGINT, VARCHAR), FlatGroupByHash.GetNonDictionaryGroupIdsWork.class);

        // Cardinality above Short.MAX_VALUE
        Page lowCardinalityHugeDictionaryPage = new Page(bigintSingletonDictionaryBlock, bigintHugeDictionaryBlock);
        assertGroupByHashWork(lowCardinalityHugeDictionaryPage, ImmutableList.of(BIGINT, BIGINT), FlatGroupByHash.GetNonDictionaryGroupIdsWork.class);
    }

    private static void assertGroupByHashWork(Page page, List<Type> types, Class<?> clazz)
    {
        GroupByHash groupByHash = createGroupByHash(true, types, false, 100, true, JOIN_COMPILER, TYPE_OPERATORS, NOOP);
        Work<int[]> work = groupByHash.getGroupIds(page);
        // Compare by name since classes are private
        assertThat(work).isInstanceOf(clazz);
    }

    private static int[] getGroupIds(GroupByHash groupByHash, Page page)
    {
        Work<int[]> work = groupByHash.getGroupIds(page);
        work.process();
        int[] groupIds = work.getResult();
        return groupIds;
    }
}
