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

import io.trino.spi.connector.SortOrder;

import static java.util.Objects.requireNonNull;

/**
 * Fills the bits of one sort channel into the composite sort key prefixes of a
 * {@link PagesIndex} position range.
 */
public interface SortKeyPrefixFiller
{
    /**
     * @return whether any null was encoded without a null indicator bit, in which case nulls may
     *         collide with extreme values and the prefix can not decide ties
     */
    boolean fill(PagesIndex pagesIndex, int startPosition, int endPosition, long[] keys, int keysOffset);

    /**
     * Bit layout of one sort channel within the composite prefix: {@code valueBits} of the sort
     * key prefix shifted to {@code shift}, preceded by a null indicator bit when
     * {@code hasNullBit} is set. Fields with a null indicator bit encode nulls exactly; fields
     * without one (only the sole field of a single-field prefix) let nulls collide with extreme
     * values, which the sort resolves through the comparator.
     */
    record SortKeyPrefixLayout(int sortChannel, SortOrder sortOrder, int valueBits, boolean hasNullBit, int shift)
    {
        public SortKeyPrefixLayout
        {
            requireNonNull(sortOrder, "sortOrder is null");
        }

        public boolean descending()
        {
            return !sortOrder.isAscending();
        }

        public boolean collideNullsWithValues()
        {
            return !hasNullBit;
        }

        public int extractShift()
        {
            return 64 - valueBits;
        }

        public long nullSubKey()
        {
            if (sortOrder.isNullsFirst()) {
                return 0;
            }
            if (hasNullBit) {
                // the null indicator is the top bit of the field
                return 1L << valueBits;
            }
            if (valueBits == 64) {
                return -1;
            }
            return (1L << valueBits) - 1;
        }

        public long valueBase()
        {
            if (hasNullBit && sortOrder.isNullsFirst()) {
                return 1L << valueBits;
            }
            return 0;
        }
    }
}
