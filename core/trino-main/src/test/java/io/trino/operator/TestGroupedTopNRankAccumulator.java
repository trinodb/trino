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

import io.trino.array.LongBigArray;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.collect.Lists.cartesianProduct;
import static java.lang.Math.min;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestGroupedTopNRankAccumulator
{
    private static final RowIdComparisonHashStrategy STRATEGY = new RowIdComparisonHashStrategy()
    {
        @Override
        public int compare(long leftRowId, long rightRowId)
        {
            return Long.compare(leftRowId, rightRowId);
        }

        @Override
        public long hashCode(long rowId)
        {
            return rowId;
        }
    };

    @DataProvider
    public static Object[][] parameters()
    {
        List<Integer> topNs = Arrays.asList(1, 2, 3);
        List<Integer> valueCounts = Arrays.asList(0, 1, 2, 4, 8);
        List<Integer> groupCounts = Arrays.asList(1, 2, 3);
        List<Boolean> drainWithRankings = Arrays.asList(true, false);
        return to2DArray(cartesianProduct(topNs, valueCounts, groupCounts, drainWithRankings));
    }

    private static Object[][] to2DArray(List<List<Object>> nestedList)
    {
        Object[][] array = new Object[nestedList.size()][];
        for (int i = 0; i < nestedList.size(); i++) {
            array[i] = nestedList.get(i).toArray();
        }
        return array;
    }

    @Test(dataProvider = "parameters")
    public void testSinglePeerGroupInsert(int topN, long valueCount, long groupCount, boolean drainWithRanking)
    {
        List<Long> evicted = new LongArrayList();
        GroupedTopNRankAccumulator accumulator = new GroupedTopNRankAccumulator(STRATEGY, topN, evicted::add);
        accumulator.verifyIntegrity();

        // Add the same value repeatedly, so everything should be accepted, and all results will have a rank of 1
        int rowId = -1;

        for (int i = 0; i < valueCount; i++) {
            for (int groupId = 0; groupId < groupCount; groupId++) {
                assertTrue(accumulator.add(groupId, toRowReference(rowId)));
                accumulator.verifyIntegrity();

                // No evictions because rank does not change for the same input
                assertTrue(evicted.isEmpty());
            }
        }

        for (int groupId = 0; groupId < groupCount; groupId++) {
            LongBigArray rowIdOutput = new LongBigArray();
            LongBigArray rankingOutput = new LongBigArray();
            if (drainWithRanking) {
                assertEquals(accumulator.drainTo(groupId, rowIdOutput, rankingOutput), valueCount);
            }
            else {
                assertEquals(accumulator.drainTo(groupId, rowIdOutput), valueCount);
            }
            accumulator.verifyIntegrity();

            for (int i = 0; i < valueCount; i++) {
                assertEquals(rowIdOutput.get(i), rowId);
                if (drainWithRanking) {
                    // Everything should have a rank of 1
                    assertEquals(rankingOutput.get(i), 1);
                }
            }
        }
    }

    @Test(dataProvider = "parameters")
    public void testIncreasingAllUniqueValues(int topN, long valueCount, long groupCount, boolean drainWithRanking)
    {
        List<Long> evicted = new LongArrayList();
        GroupedTopNRankAccumulator accumulator = new GroupedTopNRankAccumulator(STRATEGY, topN, evicted::add);
        accumulator.verifyIntegrity();

        for (int rowId = 0; rowId < valueCount; rowId++) {
            for (int groupId = 0; groupId < groupCount; groupId++) {
                // Since rowIds are in increasing order, only the first topN will be accepted
                assertEquals(accumulator.add(groupId, toRowReference(rowId)), rowId < topN);
                accumulator.verifyIntegrity();

                // No evictions because all results should be rejected at add()
                assertTrue(evicted.isEmpty());
            }
        }

        long expectedResultCount = min(valueCount, topN);
        for (int groupId = 0; groupId < groupCount; groupId++) {
            LongBigArray rowIdOutput = new LongBigArray();
            LongBigArray rankingOutput = new LongBigArray();
            if (drainWithRanking) {
                assertEquals(accumulator.drainTo(groupId, rowIdOutput, rankingOutput), expectedResultCount);
            }
            else {
                assertEquals(accumulator.drainTo(groupId, rowIdOutput), expectedResultCount);
            }
            accumulator.verifyIntegrity();

            for (int rowId = 0; rowId < expectedResultCount; rowId++) {
                // The rowId is simultaneously the index
                assertEquals(rowIdOutput.get(rowId), rowId);
                if (drainWithRanking) {
                    // Results should have a rank of rowId + 1
                    assertEquals(rankingOutput.get(rowId), rowId + 1);
                }
            }
        }
    }

    @Test(dataProvider = "parameters")
    public void testDecreasingAllUniqueValues(int topN, long valueCount, long groupCount, boolean drainWithRanking)
    {
        List<Long> evicted = new LongArrayList();
        GroupedTopNRankAccumulator accumulator = new GroupedTopNRankAccumulator(STRATEGY, topN, evicted::add);
        accumulator.verifyIntegrity();

        List<Long> expectedEvicted = new ArrayList<>();
        for (long rowId = valueCount - 1; rowId >= 0; rowId--) {
            for (int groupId = 0; groupId < groupCount; groupId++) {
                // Since rowIds are in decreasing order, new rowIds will always be accepted, potentially evicting older rows
                assertTrue(accumulator.add(groupId, toRowReference(rowId)));
                accumulator.verifyIntegrity();

                if (rowId >= topN) {
                    expectedEvicted.add(rowId);
                }
            }
        }

        // The largest elements should be evicted
        assertEquals(evicted, expectedEvicted);

        for (int groupId = 0; groupId < groupCount; groupId++) {
            LongBigArray rowIdOutput = new LongBigArray();
            LongBigArray rankingOutput = new LongBigArray();
            long expectedResultCount = min(valueCount, topN);
            if (drainWithRanking) {
                assertEquals(accumulator.drainTo(groupId, rowIdOutput, rankingOutput), expectedResultCount);
            }
            else {
                assertEquals(accumulator.drainTo(groupId, rowIdOutput), expectedResultCount);
            }
            accumulator.verifyIntegrity();

            for (int rowId = 0; rowId < expectedResultCount; rowId++) {
                // The rowId is simultaneously the index
                assertEquals(rowIdOutput.get(rowId), rowId);
                if (drainWithRanking) {
                    // Results should have a rank of rowId + 1
                    assertEquals(rankingOutput.get(rowId), rowId + 1);
                }
            }
        }
    }

    @Test
    public void testMultipleDuplicateValues()
    {
        int topN = 3;

        List<Long> evicted = new LongArrayList();
        GroupedTopNRankAccumulator accumulator = new GroupedTopNRankAccumulator(STRATEGY, topN, evicted::add);
        accumulator.verifyIntegrity();

        // Add rowId 0
        assertTrue(accumulator.add(0, toRowReference(0)));
        accumulator.verifyIntegrity();
        assertTrue(evicted.isEmpty());

        // Add rowId 1
        assertTrue(accumulator.add(0, toRowReference(1)));
        accumulator.verifyIntegrity();
        assertTrue(evicted.isEmpty());

        // Add rowId 0 again, putting rowId 1 at effective rank of 3
        assertTrue(accumulator.add(0, toRowReference(0)));
        accumulator.verifyIntegrity();
        assertTrue(evicted.isEmpty());

        // Add rowId 1 again, but rowId 1 should still have an effective rank of 3
        assertTrue(accumulator.add(0, toRowReference(1)));
        accumulator.verifyIntegrity();
        assertTrue(evicted.isEmpty());

        // Add rowId 0 again, which should force both values of rowId1 to be evicted
        assertTrue(accumulator.add(0, toRowReference(0)));
        accumulator.verifyIntegrity();
        assertEquals(evicted, Arrays.asList(1L, 1L));

        // Add rowId -1, putting rowId 0 at rank 2
        assertTrue(accumulator.add(0, toRowReference(-1)));
        accumulator.verifyIntegrity();
        assertEquals(evicted, Arrays.asList(1L, 1L));

        // Add rowId -1 again, putting rowId 0 at rank 3
        assertTrue(accumulator.add(0, toRowReference(-1)));
        accumulator.verifyIntegrity();
        assertEquals(evicted, Arrays.asList(1L, 1L));

        // Drain
        LongBigArray rowIdOutput = new LongBigArray();
        LongBigArray rankingOutput = new LongBigArray();
        assertEquals(accumulator.drainTo(0, rowIdOutput, rankingOutput), 5);

        assertEquals(rowIdOutput.get(0), -1);
        assertEquals(rankingOutput.get(0), 1);
        assertEquals(rowIdOutput.get(1), -1);
        assertEquals(rankingOutput.get(1), 1);
        assertEquals(rowIdOutput.get(2), 0);
        assertEquals(rankingOutput.get(2), 3);
        assertEquals(rowIdOutput.get(3), 0);
        assertEquals(rankingOutput.get(3), 3);
        assertEquals(rowIdOutput.get(4), 0);
        assertEquals(rankingOutput.get(4), 3);
    }

    private static RowReference toRowReference(long rowId)
    {
        return new RowReference()
        {
            @Override
            public int compareTo(RowIdComparisonStrategy strategy, long otherRowId)
            {
                return strategy.compare(rowId, otherRowId);
            }

            @Override
            public boolean equals(RowIdHashStrategy strategy, long otherRowId)
            {
                return strategy.equals(rowId, otherRowId);
            }

            @Override
            public long hash(RowIdHashStrategy strategy)
            {
                return strategy.hashCode(rowId);
            }

            @Override
            public long allocateRowId()
            {
                return rowId;
            }
        };
    }
}
