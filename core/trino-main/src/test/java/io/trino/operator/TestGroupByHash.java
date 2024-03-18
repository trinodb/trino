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
import io.trino.type.TypeTestUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.google.common.math.DoubleMath.log2;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createStringSequenceBlock;
import static io.trino.block.BlockAssertions.createTypedLongsBlock;
import static io.trino.operator.GroupByHash.createGroupByHash;
import static io.trino.operator.UpdateMemory.NOOP;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.TypeTestUtils.getHashBlock;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGroupByHash
{
    private static final int MAX_GROUP_ID = 500;
    private static final int BIGINT_EXPECTED_REHASH = 20;
    // first rehash moves from the initial capacity to 1024 (batch size) and last hash moves to 1024 * 1024,
    // which is 1 initial rehash + 10 additional rehashes
    private static final int VARCHAR_EXPECTED_REHASH = 11;

    private static final List<Type> DATA_TYPES = ImmutableList.of(VARCHAR, BIGINT);

    private enum GroupByHashType
    {
        BIGINT, FLAT;

        public GroupByHash createGroupByHash(Type hashType)
        {
            return createGroupByHash(100, NOOP, hashType);
        }

        public GroupByHash createGroupByHash(int expectedSize, UpdateMemory updateMemory, Type hashType)
        {
            return switch (this) {
                case BIGINT -> new BigintGroupByHash(true, expectedSize, updateMemory, hashType);
                case FLAT -> new FlatGroupByHash(
                        ImmutableList.of(BigintType.BIGINT),
                        true,
                        expectedSize,
                        true,
                        new FlatHashStrategyCompiler(new TypeOperators()),
                        updateMemory);
            };
        }

        public int getMaxGroupId(Type hashType)
        {
            if (hashType.equals(TINYINT)) {
                return Byte.MAX_VALUE;
            }
            return MAX_GROUP_ID;
        }
    }

    private static List<Type> getHashTypes(GroupByHashType groupByHashType)
    {
        return switch (groupByHashType) {
            case FLAT -> ImmutableList.of(BIGINT);
            case BIGINT -> ImmutableList.of(BIGINT, INTEGER, SMALLINT, TINYINT, DATE);
        };
    }

    @Test
    public void testAddPage()
    {
        for (GroupByHashType groupByHashType : GroupByHashType.values()) {
            for (Type hashType : getHashTypes(groupByHashType)) {
                GroupByHash groupByHash = groupByHashType.createGroupByHash(hashType);
                int maxGroupId = groupByHashType.getMaxGroupId(hashType);
                for (int tries = 0; tries < 2; tries++) {
                    for (int value = 0; value < maxGroupId; value++) {
                        Block block = BlockAssertions.createTypedLongsBlock(hashType, (long) value);
                        Block hashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(hashType), block);
                        Page page = new Page(block, hashBlock);
                        for (int addValuesTries = 0; addValuesTries < 10; addValuesTries++) {
                            groupByHash.addPage(page).process();
                            assertThat(groupByHash.getGroupCount()).isEqualTo(tries == 0 ? value + 1 : maxGroupId);

                            // add the page again using get group ids and make sure the group count didn't change
                            int[] groupIds = getGroupIds(groupByHash, page);
                            assertThat(groupByHash.getGroupCount()).isEqualTo(tries == 0 ? value + 1 : maxGroupId);

                            // verify the first position
                            assertThat(groupIds).hasSize(1);
                            int groupId = groupIds[0];
                            assertThat(groupId).isEqualTo(value);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testRunLengthEncodedInputPage()
    {
        for (GroupByHashType groupByHashType : GroupByHashType.values()) {
            for (Type hashType : getHashTypes(groupByHashType)) {
                GroupByHash groupByHash = groupByHashType.createGroupByHash(hashType);
                Block block = BlockAssertions.createTypedLongsBlock(hashType, 0L);
                Block hashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(hashType), block);
                Page page = new Page(
                        RunLengthEncodedBlock.create(block, 2),
                        RunLengthEncodedBlock.create(hashBlock, 2));

                groupByHash.addPage(page).process();

                assertThat(groupByHash.getGroupCount()).isEqualTo(1);

                Work<int[]> work = groupByHash.getGroupIds(page);
                if (groupByHashType == GroupByHashType.FLAT) {
                    assertThat(work).isInstanceOf(FlatGroupByHash.GetRunLengthEncodedGroupIdsWork.class);
                }
                else {
                    assertThat(work).isInstanceOf(BigintGroupByHash.GetRunLengthEncodedGroupIdsWork.class);
                }
                work.process();
                int[] groupIds = work.getResult();

                assertThat(groupByHash.getGroupCount()).isEqualTo(1);
                assertThat(groupIds).hasSize(2);
                assertThat(groupIds[0]).isEqualTo(0);
                assertThat(groupIds[1]).isEqualTo(0);
            }
        }
    }

    @Test
    public void testDictionaryInputPage()
    {
        for (GroupByHashType groupByHashType : GroupByHashType.values()) {
            for (Type hashType : getHashTypes(groupByHashType)) {
                GroupByHash groupByHash = groupByHashType.createGroupByHash(hashType);
                Block block = BlockAssertions.createTypedLongsBlock(hashType, 0L, 1L);
                Block hashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(hashType), block);
                int[] ids = new int[] {0, 0, 1, 1};
                Page page = new Page(
                        DictionaryBlock.create(ids.length, block, ids),
                        DictionaryBlock.create(ids.length, hashBlock, ids));

                groupByHash.addPage(page).process();

                assertThat(groupByHash.getGroupCount()).isEqualTo(2);

                int[] groupIds = getGroupIds(groupByHash, page);
                assertThat(groupByHash.getGroupCount()).isEqualTo(2);
                assertThat(groupIds).hasSize(4);
                assertThat(groupIds[0]).isEqualTo(0);
                assertThat(groupIds[1]).isEqualTo(0);
                assertThat(groupIds[2]).isEqualTo(1);
                assertThat(groupIds[3]).isEqualTo(1);
            }
        }
    }

    @Test
    public void testNullGroup()
    {
        for (GroupByHashType groupByHashType : GroupByHashType.values()) {
            for (Type hashType : getHashTypes(groupByHashType)) {
                GroupByHash groupByHash = groupByHashType.createGroupByHash(hashType);

                Block block = BlockAssertions.createTypedLongsBlock(hashType, 0L, null);
                Block hashBlock = getHashBlock(ImmutableList.of(hashType), block);
                Page page = new Page(block, hashBlock);
                // assign null a groupId (which is one since is it the second value added)
                assertThat(getGroupIds(groupByHash, page))
                        .containsExactly(0, 1);

                // Add enough values to force a rehash
                int rehashThreshold = 132748;
                if (hashType == SMALLINT) {
                    rehashThreshold = Short.MAX_VALUE;
                }
                else if (hashType == TINYINT) {
                    rehashThreshold = Byte.MAX_VALUE;
                }

                block = createLongSequenceBlock(1, rehashThreshold, hashType);
                hashBlock = getHashBlock(ImmutableList.of(hashType), block);
                page = new Page(block, hashBlock);
                groupByHash.addPage(page).process();

                block = BlockAssertions.createTypedLongsBlock(hashType, (Long) null);
                hashBlock = getHashBlock(ImmutableList.of(hashType), block);
                // null groupId will be 0 (as set above)
                assertThat(getGroupIds(groupByHash, new Page(block, hashBlock)))
                        .containsExactly(1);
            }
        }
    }

    @Test
    public void testGetGroupIds()
    {
        for (GroupByHashType groupByHashType : GroupByHashType.values()) {
            for (Type hashType : getHashTypes(groupByHashType)) {
                GroupByHash groupByHash = groupByHashType.createGroupByHash(hashType);
                for (int tries = 0; tries < 2; tries++) {
                    for (int value = 0; value < groupByHashType.getMaxGroupId(hashType); value++) {
                        Block block = BlockAssertions.createTypedLongsBlock(hashType, (long) value);
                        Block hashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(hashType), block);
                        Page page = new Page(block, hashBlock);
                        for (int addValuesTries = 0; addValuesTries < 10; addValuesTries++) {
                            int[] groupIds = getGroupIds(groupByHash, page);
                            assertThat(groupByHash.getGroupCount()).isEqualTo(tries == 0 ? value + 1 : groupByHashType.getMaxGroupId(hashType));
                            assertThat(groupIds).hasSize(1);
                            long groupId = groupIds[0];
                            assertThat(groupId).isEqualTo(value);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testAppendTo()
    {
        for (GroupByHashType groupByHashType : GroupByHashType.values()) {
            for (Type hashType : getHashTypes(groupByHashType)) {
                Block valuesBlock = BlockAssertions.createLongSequenceBlock(0, 100, hashType);
                Block hashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(hashType), valuesBlock);
                GroupByHash groupByHash = groupByHashType.createGroupByHash(hashType);

                int[] groupIds = getGroupIds(groupByHash, new Page(valuesBlock, hashBlock));
                for (int i = 0; i < valuesBlock.getPositionCount(); i++) {
                    assertThat(groupIds[i]).isEqualTo(i);
                }
                assertThat(groupByHash.getGroupCount()).isEqualTo(100);

                PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(hashType, BIGINT));
                for (int i = 0; i < groupByHash.getGroupCount(); i++) {
                    pageBuilder.declarePosition();
                    groupByHash.appendValuesTo(i, pageBuilder);
                }
                Page page = pageBuilder.build();
                // Ensure that all blocks have the same positionCount
                for (int i = 0; i < page.getChannelCount(); i++) {
                    assertThat(page.getBlock(i).getPositionCount()).isEqualTo(100);
                }
                assertThat(page.getPositionCount()).isEqualTo(100);
                BlockAssertions.assertBlockEquals(hashType, page.getBlock(0), valuesBlock);
                BlockAssertions.assertBlockEquals(BIGINT, page.getBlock(1), hashBlock);
            }
        }
    }

    @Test
    public void testAppendToMultipleTuplesPerGroup()
    {
        for (GroupByHashType groupByHashType : GroupByHashType.values()) {
            for (Type hashType : getHashTypes(groupByHashType)) {
                List<Long> values = new ArrayList<>();
                for (long i = 0; i < 100; i++) {
                    values.add(i % 50);
                }
                Block valuesBlock = createTypedLongsBlock(hashType, values);
                Block hashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(hashType), valuesBlock);

                GroupByHash groupByHash = groupByHashType.createGroupByHash(hashType);
                groupByHash.getGroupIds(new Page(valuesBlock, hashBlock)).process();
                assertThat(groupByHash.getGroupCount()).isEqualTo(50);

                PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(hashType, BIGINT));
                for (int i = 0; i < groupByHash.getGroupCount(); i++) {
                    pageBuilder.declarePosition();
                    groupByHash.appendValuesTo(i, pageBuilder);
                }
                Page outputPage = pageBuilder.build();
                assertThat(outputPage.getPositionCount()).isEqualTo(50);
                BlockAssertions.assertBlockEquals(hashType, outputPage.getBlock(0), BlockAssertions.createLongSequenceBlock(0, 50, hashType));
            }
        }
    }

    @Test
    public void testForceRehash()
    {
        for (GroupByHashType groupByHashType : GroupByHashType.values()) {
            for (Type hashType : getHashTypes(groupByHashType)) {
                // Create a page with positionCount >> expected size of groupByHash
                Block valuesBlock = BlockAssertions.createLongSequenceBlock(0, 100, hashType);
                Block hashBlock = TypeTestUtils.getHashBlock(ImmutableList.of(hashType), valuesBlock);

                // Create group by hash with extremely small size
                GroupByHash groupByHash = groupByHashType.createGroupByHash(4, NOOP, hashType);
                groupByHash.getGroupIds(new Page(valuesBlock, hashBlock)).process();

                // Ensure that all groups are present in GroupByHash
                int groupCount = groupByHash.getGroupCount();
                for (int groupId : getGroupIds(groupByHash, new Page(valuesBlock, hashBlock))) {
                    assertThat(groupId).isLessThan(groupCount);
                }
            }
        }
    }

    @Test
    public void testUpdateMemory()
    {
        for (Type type : DATA_TYPES) {
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
            GroupByHash groupByHash = createGroupByHash(ImmutableList.of(type), true, 1, false, new FlatHashStrategyCompiler(new TypeOperators()), () -> {
                rehashCount.incrementAndGet();
                return true;
            });
            groupByHash.addPage(new Page(valuesBlock, hashBlock)).process();

            // assert we call update memory twice every time we rehash; the rehash count = log2(length / FILL_RATIO)
            assertThat(rehashCount.get()).isEqualTo(2 * (type == VARCHAR ? VARCHAR_EXPECTED_REHASH : BIGINT_EXPECTED_REHASH));
        }
    }

    @Test
    public void testMemoryReservationYield()
    {
        for (Type type : DATA_TYPES) {
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
            GroupByHash groupByHash = createGroupByHash(ImmutableList.of(type), true, 1, false, new FlatHashStrategyCompiler(new TypeOperators()), updateMemory);
            boolean finish = false;
            Work<?> addPageWork = groupByHash.addPage(page);
            while (!finish) {
                finish = addPageWork.process();
                if (!finish) {
                    assertThat(currentQuota.get()).isEqualTo(allowedQuota.get());
                    // assert if we are blocked, we are going to be blocked again without changing allowedQuota
                    assertThat(addPageWork.process()).isFalse();
                    assertThat(currentQuota.get()).isEqualTo(allowedQuota.get());
                    yields++;
                    allowedQuota.getAndAdd(6);
                }
            }

            // assert there is not anything missing
            assertThat(groupByHash.getGroupCount()).isEqualTo(length);
            // assert we yield for every 3 rehashes
            // currentQuota is essentially the count we have successfully rehashed multiplied by 2 (as updateMemory is called twice per rehash)
            assertThat(currentQuota.get()).isEqualTo(2 * (type == VARCHAR ? VARCHAR_EXPECTED_REHASH : BIGINT_EXPECTED_REHASH));
            assertThat(currentQuota.get() / 3 / 2).isEqualTo(yields);

            // test getGroupIds
            currentQuota.set(0);
            allowedQuota.set(6);
            yields = 0;
            groupByHash = createGroupByHash(ImmutableList.of(type), true, 1, false, new FlatHashStrategyCompiler(new TypeOperators()), updateMemory);

            finish = false;
            Work<int[]> getGroupIdsWork = groupByHash.getGroupIds(page);
            while (!finish) {
                finish = getGroupIdsWork.process();
                if (!finish) {
                    assertThat(currentQuota.get()).isEqualTo(allowedQuota.get());
                    // assert if we are blocked, we are going to be blocked again without changing allowedQuota
                    assertThat(getGroupIdsWork.process()).isFalse();
                    assertThat(currentQuota.get()).isEqualTo(allowedQuota.get());
                    yields++;
                    allowedQuota.getAndAdd(6);
                }
            }
            // assert there is not anything missing
            assertThat(groupByHash.getGroupCount()).isEqualTo(length);
            assertThat(getGroupIdsWork.getResult()).hasSize(length);
            // rehash count is the same as above
            assertThat(currentQuota.get()).isEqualTo(2 * (type == VARCHAR ? VARCHAR_EXPECTED_REHASH : BIGINT_EXPECTED_REHASH));
            assertThat(currentQuota.get() / 3 / 2).isEqualTo(yields);
        }
    }

    @Test
    public void testMemoryReservationYieldWithDictionary()
    {
        for (GroupByHashType groupByHashType : GroupByHashType.values()) {
            for (Type hashType : getHashTypes(groupByHashType)) {
                // Create a page with positionCount >> expected size of groupByHash
                int length = 2_000_000;
                if (hashType == SMALLINT) {
                    length = Short.MAX_VALUE;
                }
                else if (hashType == TINYINT) {
                    length = Byte.MAX_VALUE;
                }

                int dictionaryLength = 10_000;
                if (hashType == SMALLINT) {
                    dictionaryLength = 16;
                }
                else if (hashType == TINYINT) {
                    dictionaryLength = Byte.MAX_VALUE;
                }

                int[] ids = IntStream.range(0, dictionaryLength).toArray();
                Block valuesBlock = DictionaryBlock.create(dictionaryLength, createLongSequenceBlock(0, length, hashType), ids);
                Block hashBlock = DictionaryBlock.create(dictionaryLength, getHashBlock(ImmutableList.of(hashType), valuesBlock), ids);
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
                GroupByHash groupByHash = groupByHashType.createGroupByHash(1, updateMemory, hashType);

                boolean finish = false;
                Work<?> addPageWork = groupByHash.addPage(page);
                while (!finish) {
                    finish = addPageWork.process();
                    if (!finish) {
                        assertThat(currentQuota.get()).isEqualTo(allowedQuota.get());
                        // assert if we are blocked, we are going to be blocked again without changing allowedQuota
                        assertThat(addPageWork.process()).isFalse();
                        assertThat(currentQuota.get()).isEqualTo(allowedQuota.get());
                        yields++;
                        allowedQuota.getAndAdd(6);
                    }
                }

                // assert there is not anything missing
                assertThat(groupByHash.getGroupCount()).isEqualTo(dictionaryLength);
                // assert we yield for every 3 rehashes
                // currentQuota is essentially the count we have successfully rehashed multiplied by 2 (as updateMemory is called twice per rehash)
                // the rehash count is 10 = log(1_000 / 0.75)
                int expectedCurrentQuota = (int) log2(dictionaryLength / 0.75);

                assertThat(currentQuota.get()).isEqualTo(2 * (groupByHashType == GroupByHashType.FLAT ? 4 : expectedCurrentQuota));
                assertThat(currentQuota.get() / 3 / 2).isEqualTo(yields);

                // test getGroupIds
                currentQuota.set(0);
                allowedQuota.set(6);
                yields = 0;
                groupByHash = groupByHashType.createGroupByHash(1, updateMemory, hashType);

                finish = false;
                Work<int[]> getGroupIdsWork = groupByHash.getGroupIds(page);
                while (!finish) {
                    finish = getGroupIdsWork.process();
                    if (!finish) {
                        assertThat(currentQuota.get()).isEqualTo(allowedQuota.get());
                        // assert if we are blocked, we are going to be blocked again without changing allowedQuota
                        assertThat(getGroupIdsWork.process()).isFalse();
                        assertThat(currentQuota.get()).isEqualTo(allowedQuota.get());
                        yields++;
                        allowedQuota.getAndAdd(6);
                    }
                }

                // assert there is not anything missing
                assertThat(groupByHash.getGroupCount()).isEqualTo(dictionaryLength);
                assertThat(getGroupIdsWork.getResult()).hasSize(dictionaryLength);
                // assert we yield for every 3 rehashes
                // currentQuota is essentially the count we have successfully rehashed multiplied by 2 (as updateMemory is called twice per rehash)
                // the rehash count is 10 = log2(1_000 / 0.75)
                assertThat(currentQuota.get()).isEqualTo(2 * (groupByHashType == GroupByHashType.FLAT ? 4 : expectedCurrentQuota));
                assertThat(currentQuota.get() / 3 / 2).isEqualTo(yields);
            }
        }
    }

    @Test
    public void testLowCardinalityDictionariesAddPage()
    {
        GroupByHash groupByHash = new FlatGroupByHash(ImmutableList.of(BIGINT, BIGINT), false, 100, false, new FlatHashStrategyCompiler(new TypeOperators()), NOOP);
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
        GroupByHash groupByHash = new FlatGroupByHash(
                ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT),
                false,
                100,
                false,
                new FlatHashStrategyCompiler(new TypeOperators()),
                NOOP);

        GroupByHash lowCardinalityGroupByHash = new FlatGroupByHash(
                ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT),
                false,
                100,
                false,
                new FlatHashStrategyCompiler(new TypeOperators()),
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
        GroupByHash groupByHash = new FlatGroupByHash(
                ImmutableList.of(BIGINT, BIGINT),
                false,
                100,
                false,
                new FlatHashStrategyCompiler(new TypeOperators()),
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
        Block bigintRleBlock = BlockAssertions.createRepeatedValuesBlock(42L, 8);

        Block varcharBlock = BlockAssertions.createStringsBlock("1", "2", "3", "4", "5", "6", "7", "8");
        Block varcharDictionaryBlock = BlockAssertions.createStringDictionaryBlock(1, 8);
        Block varcharRleBlock = RunLengthEncodedBlock.create(new VariableWidthBlock(1, Slices.EMPTY_SLICE, new int[] {0, 1}, Optional.empty()), 8);

        Block bigintBigDictionaryBlock = BlockAssertions.createLongDictionaryBlock(1, 8, 1000);
        Block bigintSingletonDictionaryBlock = BlockAssertions.createLongDictionaryBlock(1, 500000, 1);
        Block bigintHugeDictionaryBlock = BlockAssertions.createLongDictionaryBlock(1, 500000, 66000); // Above Short.MAX_VALUE

        Block intBlock = BlockAssertions.createIntsBlock(1, 2, 3, 4, 5, 6, 7, 8);
        Block intDictionaryBlock = BlockAssertions.createDictionaryBlock(0, 8, INTEGER);
        Block intRleBlock = BlockAssertions.createRepeatedValuesBlock(42, 8, INTEGER);

        Block smallintBlock = BlockAssertions.createSmallintsBlock(1, 2, 3, 4, 5, 6, 7, 8);
        Block smallintDictionaryBlock = BlockAssertions.createDictionaryBlock(0, 8, SMALLINT);
        Block smallintRleBlock = BlockAssertions.createRepeatedValuesBlock(42, 8, SMALLINT);

        Block tinyintBlock = BlockAssertions.createTinyintsBlock(1, 2, 3, 4, 5, 6, 7, 8);
        Block tinyintDictionaryBlock = BlockAssertions.createDictionaryBlock(0, 8, TINYINT);
        Block tinyintRleBlock = BlockAssertions.createRepeatedValuesBlock(42, 8, TINYINT);

        Block dateBlock = BlockAssertions.createDateSequenceBlock(0, 8);
        Block dateDictionaryBlock = BlockAssertions.createDictionaryBlock(0, 8, INTEGER);
        Block dateRleBlock = BlockAssertions.createRepeatedValuesBlock(42, 8, INTEGER);

        Page singleBigintPage = new Page(bigintBlock);
        assertGroupByHashWork(singleBigintPage, ImmutableList.of(BIGINT), BigintGroupByHash.GetGroupIdsWork.class);
        Page singleBigintDictionaryPage = new Page(bigintDictionaryBlock);
        assertGroupByHashWork(singleBigintDictionaryPage, ImmutableList.of(BIGINT), BigintGroupByHash.GetDictionaryGroupIdsWork.class);
        Page singleBigintRlePage = new Page(bigintRleBlock);
        assertGroupByHashWork(singleBigintRlePage, ImmutableList.of(BIGINT), BigintGroupByHash.GetRunLengthEncodedGroupIdsWork.class);

        Page singleSmallintPage = new Page(smallintBlock);
        assertGroupByHashWork(singleSmallintPage, ImmutableList.of(SMALLINT), BigintGroupByHash.GetGroupIdsWork.class);
        Page singleSmallintDictionaryPage = new Page(smallintDictionaryBlock);
        assertGroupByHashWork(singleSmallintDictionaryPage, ImmutableList.of(SMALLINT), BigintGroupByHash.GetDictionaryGroupIdsWork.class);
        Page singleSmallintRlePage = new Page(smallintRleBlock);
        assertGroupByHashWork(singleSmallintRlePage, ImmutableList.of(SMALLINT), BigintGroupByHash.GetRunLengthEncodedGroupIdsWork.class);

        Page singleIntPage = new Page(intBlock);
        assertGroupByHashWork(singleIntPage, ImmutableList.of(INTEGER), BigintGroupByHash.GetGroupIdsWork.class);
        Page singleIntDictionaryPage = new Page(intDictionaryBlock);
        assertGroupByHashWork(singleIntDictionaryPage, ImmutableList.of(INTEGER), BigintGroupByHash.GetDictionaryGroupIdsWork.class);
        Page singleIntRlePage = new Page(intRleBlock);
        assertGroupByHashWork(singleIntRlePage, ImmutableList.of(INTEGER), BigintGroupByHash.GetRunLengthEncodedGroupIdsWork.class);

        Page singleTinyintPage = new Page(tinyintBlock);
        assertGroupByHashWork(singleTinyintPage, ImmutableList.of(TINYINT), BigintGroupByHash.GetGroupIdsWork.class);
        Page singleTinyintDictionaryPage = new Page(tinyintDictionaryBlock);
        assertGroupByHashWork(singleTinyintDictionaryPage, ImmutableList.of(TINYINT), BigintGroupByHash.GetDictionaryGroupIdsWork.class);
        Page singleTinyintRlePage = new Page(tinyintRleBlock);
        assertGroupByHashWork(singleTinyintRlePage, ImmutableList.of(TINYINT), BigintGroupByHash.GetRunLengthEncodedGroupIdsWork.class);

        Page singleDatePage = new Page(dateBlock);
        assertGroupByHashWork(singleDatePage, ImmutableList.of(DATE), BigintGroupByHash.GetGroupIdsWork.class);
        Page singleDateDictionaryPage = new Page(dateDictionaryBlock);
        assertGroupByHashWork(singleDateDictionaryPage, ImmutableList.of(DATE), BigintGroupByHash.GetDictionaryGroupIdsWork.class);
        Page singleDateRlePage = new Page(dateRleBlock);
        assertGroupByHashWork(singleDateRlePage, ImmutableList.of(DATE), BigintGroupByHash.GetRunLengthEncodedGroupIdsWork.class);

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
        GroupByHash groupByHash = createGroupByHash(types, false, 100, true, new FlatHashStrategyCompiler(new TypeOperators()), NOOP);
        Work<int[]> work = groupByHash.getGroupIds(page);
        // Compare by name since classes are private
        assertThat(work.getClass().getName()).isEqualTo(clazz.getName());
    }

    private static int[] getGroupIds(GroupByHash groupByHash, Page page)
    {
        Work<int[]> work = groupByHash.getGroupIds(page);
        work.process();
        int[] groupIds = work.getResult();
        return groupIds;
    }
}
