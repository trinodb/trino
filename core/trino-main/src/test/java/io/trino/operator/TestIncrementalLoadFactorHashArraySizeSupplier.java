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

import static io.trino.operator.HashArraySizeSupplier.defaultHashArraySizeSupplier;
import static io.trino.operator.IncrementalLoadFactorHashArraySizeSupplier.THRESHOLD_25;
import static io.trino.operator.IncrementalLoadFactorHashArraySizeSupplier.THRESHOLD_50;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static it.unimi.dsi.fastutil.HashCommon.maxFill;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIncrementalLoadFactorHashArraySizeSupplier
{
    @Test
    public void testMaximumHashArraySize()
    {
        int maxCount = maxFill(1 << 30, 0.75f);

        assertHashArraySize(defaultHashArraySizeSupplier(), maxCount);
        assertHashArraySize(new IncrementalLoadFactorHashArraySizeSupplier(1), maxCount);
        assertHashArraySize(new IncrementalLoadFactorHashArraySizeSupplier(8192), maxFill(1 << 30, 0.25f));
        assertHashArraySize(new IncrementalLoadFactorHashArraySizeSupplier(1024), maxFill(1 << 30, 0.50f));
    }

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
        assertThat(size).isGreaterThanOrEqualTo(previousSize);
        assertThat(sizeSupplier.getHashArraySize(expectedCount) * 4).isEqualTo(sizeSupplierWithMultiplier.getHashArraySize(expectedCount * 4));
        return size;
    }

    private static void assertHashArraySize(HashArraySizeSupplier sizeSupplier, int maxExpectedCount)
    {
        assertThat(sizeSupplier.getHashArraySize(maxExpectedCount)).isEqualTo(1 << 30);
        assertTrinoExceptionThrownBy(() -> sizeSupplier.getHashArraySize(maxExpectedCount + 1))
                .hasErrorCode(GENERIC_INSUFFICIENT_RESOURCES)
                .hasMessage("Size of join hash table can not exceed %s entries".formatted(maxExpectedCount));
    }
}
