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
package io.trino.spi.connector;

import io.trino.spi.Page;

public interface BucketFunction
{
    /**
     * Gets the bucket for the tuple at the specified position.
     * Note the tuple values may be null.
     *
     * @deprecated Use {@link #getBuckets}.
     */
    @Deprecated
    int getBucket(Page page, int position);

    /**
     * Gets the buckets for all tuples within specified position range.
     * Note the tuple values may be null.
     *
     * @param buckets the array to hold the buckets of length at least {@code length}.
     * Value at index {@code i} should hold the bucket for position {@code positionOffset + i}.
     */
    default void getBuckets(Page page, int positionOffset, int length, int[] buckets)
    {
        for (int i = 0; i < length; i++) {
            buckets[i] = getBucket(page, positionOffset + i);
        }
    }

    /**
     * Gets the buckets for all tuples within specified position range.
     * Note the tuple values may be null.
     *
     * @param buckets the array to hold the buckets of length at least {@code length}.
     * Value at index {@code i} should hold the bucket for position {@code positionOffset + i}.
     */
    default void getBuckets(Page page, int positionOffset, int length, boolean[] mask, int[] buckets)
    {
        for (int i = 0; i < length; i++) {
            if (mask[i]) {
                buckets[i] = getBucket(page, positionOffset + i);
            }
        }
    }
}
