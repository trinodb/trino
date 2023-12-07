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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Math.min;
import static org.assertj.core.api.Assertions.assertThat;

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

    @Test
    public void testSinglePeerGroupInsert()
    {
        for (int topN : Arrays.asList(1, 2, 3)) {
            for (int valueCount : Arrays.asList(0, 1, 2, 4, 8)) {
                for (int groupCount : Arrays.asList(1, 2, 3)) {
                    testSinglePeerGroupInsert(topN, valueCount, groupCount, true);
                    testSinglePeerGroupInsert(topN, valueCount, groupCount, false);
                }
            }
        }
    }

    private void testSinglePeerGroupInsert(int topN, long valueCount, long groupCount, boolean drainWithRanking)
    {
        List<Long> evicted = new LongArrayList();
        GroupedTopNRankAccumulator accumulator = new GroupedTopNRankAccumulator(STRATEGY, topN, evicted::add);
        accumulator.verifyIntegrity();

        // Add the same value repeatedly, so everything should be accepted, and all results will have a rank of 1
        int rowId = -1;

        for (int i = 0; i < valueCount; i++) {
            for (int groupId = 0; groupId < groupCount; groupId++) {
                assertThat(accumulator.add(groupId, toRowReference(rowId))).isTrue();
                accumulator.verifyIntegrity();

                // No evictions because rank does not change for the same input
                assertThat(evicted.isEmpty()).isTrue();
            }
        }

        for (int groupId = 0; groupId < groupCount; groupId++) {
            LongBigArray rowIdOutput = new LongBigArray();
            LongBigArray rankingOutput = new LongBigArray();
            if (drainWithRanking) {
                assertThat(accumulator.drainTo(groupId, rowIdOutput, rankingOutput)).isEqualTo(valueCount);
            }
            else {
                assertThat(accumulator.drainTo(groupId, rowIdOutput)).isEqualTo(valueCount);
            }
            accumulator.verifyIntegrity();

            for (int i = 0; i < valueCount; i++) {
                assertThat(rowIdOutput.get(i)).isEqualTo(rowId);
                if (drainWithRanking) {
                    // Everything should have a rank of 1
                    assertThat(rankingOutput.get(i)).isEqualTo(1);
                }
            }
        }
    }

    @Test
    public void testIncreasingAllUniqueValues()
    {
        for (int topN : Arrays.asList(1, 2, 3)) {
            for (int valueCount : Arrays.asList(0, 1, 2, 4, 8)) {
                for (int groupCount : Arrays.asList(1, 2, 3)) {
                    testIncreasingAllUniqueValues(topN, valueCount, groupCount, true);
                    testIncreasingAllUniqueValues(topN, valueCount, groupCount, false);
                }
            }
        }
    }

    private void testIncreasingAllUniqueValues(int topN, long valueCount, long groupCount, boolean drainWithRanking)
    {
        List<Long> evicted = new LongArrayList();
        GroupedTopNRankAccumulator accumulator = new GroupedTopNRankAccumulator(STRATEGY, topN, evicted::add);
        accumulator.verifyIntegrity();

        for (int rowId = 0; rowId < valueCount; rowId++) {
            for (int groupId = 0; groupId < groupCount; groupId++) {
                // Since rowIds are in increasing order, only the first topN will be accepted
                assertThat(accumulator.add(groupId, toRowReference(rowId))).isEqualTo(rowId < topN);
                accumulator.verifyIntegrity();

                // No evictions because all results should be rejected at add()
                assertThat(evicted.isEmpty()).isTrue();
            }
        }

        long expectedResultCount = min(valueCount, topN);
        for (int groupId = 0; groupId < groupCount; groupId++) {
            LongBigArray rowIdOutput = new LongBigArray();
            LongBigArray rankingOutput = new LongBigArray();
            if (drainWithRanking) {
                assertThat(accumulator.drainTo(groupId, rowIdOutput, rankingOutput)).isEqualTo(expectedResultCount);
            }
            else {
                assertThat(accumulator.drainTo(groupId, rowIdOutput)).isEqualTo(expectedResultCount);
            }
            accumulator.verifyIntegrity();

            for (int rowId = 0; rowId < expectedResultCount; rowId++) {
                // The rowId is simultaneously the index
                assertThat(rowIdOutput.get(rowId)).isEqualTo(rowId);
                if (drainWithRanking) {
                    // Results should have a rank of rowId + 1
                    assertThat(rankingOutput.get(rowId)).isEqualTo(rowId + 1);
                }
            }
        }
    }

    @Test
    public void testDecreasingAllUniqueValues()
    {
        for (int topN : Arrays.asList(1, 2, 3)) {
            for (int valueCount : Arrays.asList(0, 1, 2, 4, 8)) {
                for (int groupCount : Arrays.asList(1, 2, 3)) {
                    testDecreasingAllUniqueValues(topN, valueCount, groupCount, true);
                    testDecreasingAllUniqueValues(topN, valueCount, groupCount, false);
                }
            }
        }
    }

    private void testDecreasingAllUniqueValues(int topN, long valueCount, long groupCount, boolean drainWithRanking)
    {
        List<Long> evicted = new LongArrayList();
        GroupedTopNRankAccumulator accumulator = new GroupedTopNRankAccumulator(STRATEGY, topN, evicted::add);
        accumulator.verifyIntegrity();

        List<Long> expectedEvicted = new ArrayList<>();
        for (long rowId = valueCount - 1; rowId >= 0; rowId--) {
            for (int groupId = 0; groupId < groupCount; groupId++) {
                // Since rowIds are in decreasing order, new rowIds will always be accepted, potentially evicting older rows
                assertThat(accumulator.add(groupId, toRowReference(rowId))).isTrue();
                accumulator.verifyIntegrity();

                if (rowId >= topN) {
                    expectedEvicted.add(rowId);
                }
            }
        }

        // The largest elements should be evicted
        assertThat(evicted).isEqualTo(expectedEvicted);

        for (int groupId = 0; groupId < groupCount; groupId++) {
            LongBigArray rowIdOutput = new LongBigArray();
            LongBigArray rankingOutput = new LongBigArray();
            long expectedResultCount = min(valueCount, topN);
            if (drainWithRanking) {
                assertThat(accumulator.drainTo(groupId, rowIdOutput, rankingOutput)).isEqualTo(expectedResultCount);
            }
            else {
                assertThat(accumulator.drainTo(groupId, rowIdOutput)).isEqualTo(expectedResultCount);
            }
            accumulator.verifyIntegrity();

            for (int rowId = 0; rowId < expectedResultCount; rowId++) {
                // The rowId is simultaneously the index
                assertThat(rowIdOutput.get(rowId)).isEqualTo(rowId);
                if (drainWithRanking) {
                    // Results should have a rank of rowId + 1
                    assertThat(rankingOutput.get(rowId)).isEqualTo(rowId + 1);
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
        assertThat(accumulator.add(0, toRowReference(0))).isTrue();
        accumulator.verifyIntegrity();
        assertThat(evicted.isEmpty()).isTrue();

        // Add rowId 1
        assertThat(accumulator.add(0, toRowReference(1))).isTrue();
        accumulator.verifyIntegrity();
        assertThat(evicted.isEmpty()).isTrue();

        // Add rowId 0 again, putting rowId 1 at effective rank of 3
        assertThat(accumulator.add(0, toRowReference(0))).isTrue();
        accumulator.verifyIntegrity();
        assertThat(evicted.isEmpty()).isTrue();

        // Add rowId 1 again, but rowId 1 should still have an effective rank of 3
        assertThat(accumulator.add(0, toRowReference(1))).isTrue();
        accumulator.verifyIntegrity();
        assertThat(evicted.isEmpty()).isTrue();

        // Add rowId 0 again, which should force both values of rowId1 to be evicted
        assertThat(accumulator.add(0, toRowReference(0))).isTrue();
        accumulator.verifyIntegrity();
        assertThat(evicted).isEqualTo(Arrays.asList(1L, 1L));

        // Add rowId -1, putting rowId 0 at rank 2
        assertThat(accumulator.add(0, toRowReference(-1))).isTrue();
        accumulator.verifyIntegrity();
        assertThat(evicted).isEqualTo(Arrays.asList(1L, 1L));

        // Add rowId -1 again, putting rowId 0 at rank 3
        assertThat(accumulator.add(0, toRowReference(-1))).isTrue();
        accumulator.verifyIntegrity();
        assertThat(evicted).isEqualTo(Arrays.asList(1L, 1L));

        // Drain
        LongBigArray rowIdOutput = new LongBigArray();
        LongBigArray rankingOutput = new LongBigArray();
        assertThat(accumulator.drainTo(0, rowIdOutput, rankingOutput)).isEqualTo(5);

        assertThat(rowIdOutput.get(0)).isEqualTo(-1);
        assertThat(rankingOutput.get(0)).isEqualTo(1);
        assertThat(rowIdOutput.get(1)).isEqualTo(-1);
        assertThat(rankingOutput.get(1)).isEqualTo(1);
        assertThat(rowIdOutput.get(2)).isEqualTo(0);
        assertThat(rankingOutput.get(2)).isEqualTo(3);
        assertThat(rowIdOutput.get(3)).isEqualTo(0);
        assertThat(rankingOutput.get(3)).isEqualTo(3);
        assertThat(rowIdOutput.get(4)).isEqualTo(0);
        assertThat(rankingOutput.get(4)).isEqualTo(3);
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
