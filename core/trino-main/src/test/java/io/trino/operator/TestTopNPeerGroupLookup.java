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

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTopNPeerGroupLookup
{
    private static final RowIdHashStrategy HASH_STRATEGY = new RowIdHashStrategy()
    {
        @Override
        public boolean equals(long leftRowId, long rightRowId)
        {
            return leftRowId == rightRowId;
        }

        @Override
        public long hashCode(long rowId)
        {
            return rowId;
        }
    };
    private static final long UNMAPPED_GROUP_ID = Long.MIN_VALUE;
    private static final long DEFAULT_RETURN_VALUE = -1L;

    @Test
    public void testCombinations()
    {
        for (int expectedSize : Arrays.asList(0, 1, 2, 3, 1_000)) {
            for (float fillFactor : Arrays.asList(0.1f, 0.9f, 1f)) {
                testCombinations(expectedSize, fillFactor, 1L, 1L);
                testCombinations(expectedSize, fillFactor, 10L, 1L);
                testCombinations(expectedSize, fillFactor, 1L, 1_000L);
                testCombinations(expectedSize, fillFactor, 10L, 1_000L);
            }
        }
    }

    private void testCombinations(int expectedSize, float fillFactor, long totalGroupIds, long totalRowIds)
    {
        TopNPeerGroupLookup lookup = new TopNPeerGroupLookup(expectedSize, fillFactor, HASH_STRATEGY, UNMAPPED_GROUP_ID, DEFAULT_RETURN_VALUE);

        assertThat(lookup.size()).isEqualTo(0);
        assertThat(lookup.isEmpty()).isTrue();

        // Put values
        int count = 0;
        for (int groupId = 0; groupId < totalGroupIds; groupId++) {
            for (int rowId = 0; rowId < totalRowIds; rowId++) {
                // Value should not exist yet
                assertThat(lookup.get(groupId, rowId)).isEqualTo(DEFAULT_RETURN_VALUE);
                assertThat(lookup.get(groupId, toRowReference(rowId))).isEqualTo(DEFAULT_RETURN_VALUE);
                assertThat(lookup.remove(groupId, rowId)).isEqualTo(DEFAULT_RETURN_VALUE);

                // Insert the value
                assertThat(lookup.put(groupId, rowId, count)).isEqualTo(DEFAULT_RETURN_VALUE);

                count++;

                assertThat(lookup.isEmpty()).isFalse();
                assertThat(lookup.size()).isEqualTo(count);
            }
        }

        // Get values
        count = 0;
        long totalEntries = totalGroupIds * totalRowIds;
        for (int groupId = 0; groupId < totalGroupIds; groupId++) {
            for (int rowId = 0; rowId < totalRowIds; rowId++) {
                assertThat(lookup.get(groupId, rowId)).isEqualTo(count);
                assertThat(lookup.get(groupId, toRowReference(rowId))).isEqualTo(count);
                count++;

                assertThat(lookup.isEmpty()).isFalse();
                assertThat(lookup.size()).isEqualTo(totalEntries);
            }
        }

        // Overwrite values
        count = 0;
        for (int groupId = 0; groupId < totalGroupIds; groupId++) {
            for (int rowId = 0; rowId < totalRowIds; rowId++) {
                assertThat(lookup.put(groupId, rowId, count + 1)).isEqualTo(count);
                count++;

                assertThat(lookup.isEmpty()).isFalse();
                assertThat(lookup.size()).isEqualTo(totalEntries);
            }
        }

        // Remove values
        count = 0;
        for (int groupId = 0; groupId < totalGroupIds; groupId++) {
            for (int rowId = 0; rowId < totalRowIds; rowId++) {
                assertThat(lookup.isEmpty()).isFalse();
                assertThat(lookup.size()).isEqualTo(totalEntries - count);

                // Removed value should be the overwritten value
                assertThat(lookup.remove(groupId, rowId)).isEqualTo(count + 1);
                count++;
            }
        }
        assertThat(lookup.isEmpty()).isTrue();
        assertThat(lookup.size()).isEqualTo(0);
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
