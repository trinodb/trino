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

import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static io.trino.operator.IncrementalLoadFactorHashArraySizeSupplier.THRESHOLD_25;
import static io.trino.operator.IncrementalLoadFactorHashArraySizeSupplier.THRESHOLD_50;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIncrementalLoadFactorHashArraySizeSupplier
{
    @Test
    public void testSizeIncreasesMonotonically()
    {
        IncrementalLoadFactorHashArraySizeSupplier sizeSupplier = new IncrementalLoadFactorHashArraySizeSupplier(1);
        IncrementalLoadFactorHashArraySizeSupplier sizeSupplierWithMultiplier = new IncrementalLoadFactorHashArraySizeSupplier(4);
        int previousSize = sizeSupplier.getHashArraySize(THRESHOLD_25 - 1);
        previousSize = assertHashArraySizeIncreases(THRESHOLD_25, previousSize, sizeSupplier, sizeSupplierWithMultiplier);
        previousSize = assertHashArraySizeIncreases(THRESHOLD_25 + 1, previousSize, sizeSupplier, sizeSupplierWithMultiplier);
        previousSize = assertHashArraySizeIncreases(THRESHOLD_50 - 1, previousSize, sizeSupplier, sizeSupplierWithMultiplier);
        previousSize = assertHashArraySizeIncreases(THRESHOLD_50, previousSize, sizeSupplier, sizeSupplierWithMultiplier);
        assertHashArraySizeIncreases(THRESHOLD_50 + 1, previousSize, sizeSupplier, sizeSupplierWithMultiplier);
    }

    private int assertHashArraySizeIncreases(
            int expectedCount,
            int previousSize,
            IncrementalLoadFactorHashArraySizeSupplier sizeSupplier,
            IncrementalLoadFactorHashArraySizeSupplier sizeSupplierWithMultiplier)
    {
        int size = sizeSupplier.getHashArraySize(expectedCount);
        assertGreaterThanOrEqual(size, previousSize);
        assertThat(sizeSupplier.getHashArraySize(expectedCount) * 4).isEqualTo(sizeSupplierWithMultiplier.getHashArraySize(expectedCount * 4));
        return size;
    }
}
