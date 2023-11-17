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
import it.unimi.dsi.fastutil.longs.LongArraySet;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class TestGroupedTopNRowNumberAccumulator
{
    @Test
    public void testSingleGroupTopN1()
    {
        int topN = 1;
        List<Long> evicted = new LongArrayList();
        GroupedTopNRowNumberAccumulator accumulator = new GroupedTopNRowNumberAccumulator(Long::compare, topN, evicted::add);
        accumulator.verifyIntegrity();

        TestingRowReference rowReference = new TestingRowReference();

        // Add one row to fill the group
        rowReference.setRowId(0);
        assertThat(accumulator.add(0, rowReference)).isTrue();
        accumulator.verifyIntegrity();
        assertThat(rowReference.isRowIdExtracted()).isTrue();
        assertThat(evicted.isEmpty()).isTrue();

        // Add a row which should be ignored because it is not in the topN and group is full
        rowReference.setRowId(1);
        assertThat(accumulator.add(0, rowReference)).isFalse();
        accumulator.verifyIntegrity();
        assertThat(rowReference.isRowIdExtracted()).isFalse();
        assertThat(evicted.isEmpty()).isTrue();

        // Add a row which should replace the existing buffered row
        rowReference.setRowId(-1);
        assertThat(accumulator.add(0, rowReference)).isTrue();
        accumulator.verifyIntegrity();
        assertThat(rowReference.isRowIdExtracted()).isTrue();
        assertThat(evicted).isEqualTo(Arrays.asList(0L));

        LongBigArray rowIdOutput = new LongBigArray();
        assertThat(accumulator.drainTo(0, rowIdOutput)).isEqualTo(1);
        accumulator.verifyIntegrity();
        assertThat(rowIdOutput.get(0)).isEqualTo(-1);
    }

    @Test
    public void testSingleGroupTopN2()
    {
        int topN = 2;
        List<Long> evicted = new LongArrayList();
        GroupedTopNRowNumberAccumulator accumulator = new GroupedTopNRowNumberAccumulator(Long::compare, topN, evicted::add);
        accumulator.verifyIntegrity();

        TestingRowReference rowReference = new TestingRowReference();

        // Add one row to the group
        rowReference.setRowId(0);
        assertThat(accumulator.add(0, rowReference)).isTrue();
        accumulator.verifyIntegrity();
        assertThat(rowReference.isRowIdExtracted()).isTrue();
        assertThat(evicted.isEmpty()).isTrue();

        // Add another row to fill the group
        rowReference.setRowId(1);
        assertThat(accumulator.add(0, rowReference)).isTrue();
        accumulator.verifyIntegrity();
        assertThat(rowReference.isRowIdExtracted()).isTrue();
        assertThat(evicted.isEmpty()).isTrue();

        // Add a row which should be ignored because it is not in the topN and group is full
        rowReference.setRowId(2);
        assertThat(accumulator.add(0, rowReference)).isFalse();
        accumulator.verifyIntegrity();
        assertThat(rowReference.isRowIdExtracted()).isFalse();
        assertThat(evicted.isEmpty()).isTrue();

        // Add a row which should replace the leaf of the heap
        rowReference.setRowId(-2);
        assertThat(accumulator.add(0, rowReference)).isTrue();
        accumulator.verifyIntegrity();
        assertThat(rowReference.isRowIdExtracted()).isTrue();
        assertThat(evicted).isEqualTo(Arrays.asList(1L));

        // Add a row which should replace the root of the heap
        rowReference.setRowId(-1);
        assertThat(accumulator.add(0, rowReference)).isTrue();
        accumulator.verifyIntegrity();
        assertThat(rowReference.isRowIdExtracted()).isTrue();
        assertThat(evicted).isEqualTo(Arrays.asList(1L, 0L));

        LongBigArray rowIdOutput = new LongBigArray();
        assertThat(accumulator.drainTo(0, rowIdOutput)).isEqualTo(2);
        accumulator.verifyIntegrity();
        assertThat(rowIdOutput.get(0)).isEqualTo(-2);
        assertThat(rowIdOutput.get(1)).isEqualTo(-1);
    }

    @Test
    public void testSingleGroupTopN2PartialFill()
    {
        int topN = 2;
        List<Long> evicted = new LongArrayList();
        GroupedTopNRowNumberAccumulator accumulator = new GroupedTopNRowNumberAccumulator(Long::compare, topN, evicted::add);
        accumulator.verifyIntegrity();

        // Add 1 row to partially fill the top N of 2 before draining
        TestingRowReference rowReference = new TestingRowReference();
        rowReference.setRowId(0);
        assertThat(accumulator.add(0, rowReference)).isTrue();
        accumulator.verifyIntegrity();
        assertThat(rowReference.isRowIdExtracted()).isTrue();
        assertThat(evicted.isEmpty()).isTrue();

        LongBigArray rowIdOutput = new LongBigArray();
        assertThat(accumulator.drainTo(0, rowIdOutput)).isEqualTo(1);
        accumulator.verifyIntegrity();
        assertThat(rowIdOutput.get(0)).isEqualTo(0);
    }

    @Test
    public void testSingleGroupTopN4PartialFill()
    {
        int topN = 4;
        List<Long> evicted = new LongArrayList();
        GroupedTopNRowNumberAccumulator accumulator = new GroupedTopNRowNumberAccumulator(Long::compare, topN, evicted::add);
        accumulator.verifyIntegrity();

        // Add 2 rows to partially fill the top N of 4 before draining
        TestingRowReference rowReference = new TestingRowReference();
        rowReference.setRowId(0);
        assertThat(accumulator.add(0, rowReference)).isTrue();
        accumulator.verifyIntegrity();
        rowReference.setRowId(1);
        assertThat(accumulator.add(0, rowReference)).isTrue();
        accumulator.verifyIntegrity();
        assertThat(rowReference.isRowIdExtracted()).isTrue();
        assertThat(evicted.isEmpty()).isTrue();

        LongBigArray rowIdOutput = new LongBigArray();
        assertThat(accumulator.drainTo(0, rowIdOutput)).isEqualTo(2);
        accumulator.verifyIntegrity();
        assertThat(rowIdOutput.get(0)).isEqualTo(0);
        assertThat(rowIdOutput.get(1)).isEqualTo(1);
    }

    @Test
    public void testMultipleGroups()
    {
        int groupCount = 10;
        int topN = 100;
        Set<Long> evicted = new LongArraySet();
        GroupedTopNRowNumberAccumulator accumulator = new GroupedTopNRowNumberAccumulator(Long::compare, topN, evicted::add);
        accumulator.verifyIntegrity();

        // Fill every group with monotonically increasing elements
        TestingRowReference rowReference = new TestingRowReference();
        int bulkInsertionCount = topN * groupCount;
        Set<Long> firstInsertionBatch = new LongArraySet();
        for (int i = bulkInsertionCount; i < bulkInsertionCount * 2; i++) {
            rowReference.setRowId(i);
            int groupId = i % groupCount;
            assertThat(accumulator.add(groupId, rowReference)).isTrue();
            accumulator.verifyIntegrity();
            assertThat(rowReference.isRowIdExtracted()).isTrue();
            assertThat(evicted.isEmpty()).isTrue();
            firstInsertionBatch.add((long) i);
        }

        // Larger elements will be rejected at add() time
        for (int i = bulkInsertionCount * 2; i < bulkInsertionCount * 3; i++) {
            rowReference.setRowId(i);
            int groupId = i % groupCount;
            assertThat(accumulator.add(groupId, rowReference)).isFalse();
            accumulator.verifyIntegrity();
            assertThat(rowReference.isRowIdExtracted()).isFalse();
            assertThat(evicted.isEmpty()).isTrue();
        }

        // Add monotonically decreasing smaller elements to force every group to be fully replaced
        for (int i = bulkInsertionCount - 1; i >= 0; i--) {
            rowReference.setRowId(i);
            int groupId = i % groupCount;
            assertThat(accumulator.add(groupId, rowReference)).isTrue();
            accumulator.verifyIntegrity();
            assertThat(rowReference.isRowIdExtracted()).isTrue();
        }

        // Everything from the first insertion batch should now be evicted.
        assertThat(evicted).isEqualTo(firstInsertionBatch);

        // Verify that draining produces the expected data in sorted order
        LongBigArray rowIdOutput = new LongBigArray();
        for (int i = 0; i < groupCount; i++) {
            assertThat(accumulator.drainTo(i, rowIdOutput)).isEqualTo(topN);
            accumulator.verifyIntegrity();
            for (int j = 0; j < topN; j++) {
                assertThat(rowIdOutput.get(j)).isEqualTo(j * groupCount + i);
            }
        }
    }

    @Test
    public void testEmptyDrain()
    {
        int topN = 1;
        List<Long> evicted = new LongArrayList();
        GroupedTopNRowNumberAccumulator accumulator = new GroupedTopNRowNumberAccumulator(Long::compare, topN, evicted::add);
        accumulator.verifyIntegrity();

        TestingRowReference rowReference = new TestingRowReference();
        rowReference.setRowId(0);
        // Adding groupId 1 implies that groupId 0 also exists, but has no data yet.
        accumulator.add(1, rowReference);
        accumulator.verifyIntegrity();

        LongBigArray rowIdOutput = new LongBigArray();
        assertThat(accumulator.drainTo(0, rowIdOutput)).isEqualTo(0);
        accumulator.verifyIntegrity();
    }

    private static class TestingRowReference
            implements RowReference
    {
        private long rowId;
        private boolean rowIdExtracted;

        public void setRowId(long rowId)
        {
            this.rowId = rowId;
            rowIdExtracted = false;
        }

        public boolean isRowIdExtracted()
        {
            return rowIdExtracted;
        }

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
            rowIdExtracted = true;
            return rowId;
        }
    }
}
