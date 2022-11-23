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

import it.unimi.dsi.fastutil.HashCommon;

/**
 * Hash array size supplier that uses larger load factors for larger arrays
 */
public class IncrementalLoadFactorHashArraySizeSupplier
        implements HashArraySizeSupplier
{
    // Threshold values provide good performance for smaller hash arrays
    // while keeping memory usage limited for larger hash arrays.
    // This is tuned based on TPC-DS SF1000.
    public static final int THRESHOLD_25 = 1 << 16; // 65536
    public static final int THRESHOLD_50 = 1 << 20; // 1048576

    private final int multiplier;

    public IncrementalLoadFactorHashArraySizeSupplier(int multiplier)
    {
        this.multiplier = multiplier;
    }

    @Override
    public int getHashArraySize(int expectedCount)
    {
        if (expectedCount <= THRESHOLD_25 * multiplier) {
            return HashCommon.arraySize(expectedCount, 0.25f);
        }
        if (expectedCount <= THRESHOLD_50 * multiplier) {
            return HashCommon.arraySize(expectedCount, 0.50f);
        }
        return HashCommon.arraySize(expectedCount, 0.75f);
    }
}
