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

import io.trino.Session;
import io.trino.spi.TrinoException;
import it.unimi.dsi.fastutil.HashCommon;

import static io.trino.SystemSessionProperties.isIncrementalHashArrayLoadFactorEnabled;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;

public interface HashArraySizeSupplier
{
    static HashArraySizeSupplier defaultHashArraySizeSupplier()
    {
        return expectedCount -> calculateHashArraySize(expectedCount, 0.75f);
    }

    static int calculateHashArraySize(int expectedCount, float loadFactor)
    {
        // HashCommon.arraySize limits array to 1 << 30 slots.
        int maxCount = HashCommon.maxFill(1 << 30, loadFactor);
        if (expectedCount > maxCount) {
            throw new TrinoException(
                    GENERIC_INSUFFICIENT_RESOURCES,
                    "Size of join hash table can not exceed %s entries".formatted(maxCount));
        }
        return HashCommon.arraySize(expectedCount, loadFactor);
    }

    static HashArraySizeSupplier incrementalLoadFactorHashArraySizeSupplier(Session session)
    {
        return incrementalLoadFactorHashArraySizeSupplier(session, 1);
    }

    static HashArraySizeSupplier incrementalLoadFactorHashArraySizeSupplier(Session session, int multiplier)
    {
        if (isIncrementalHashArrayLoadFactorEnabled(session)) {
            return new IncrementalLoadFactorHashArraySizeSupplier(multiplier);
        }

        return defaultHashArraySizeSupplier();
    }

    int getHashArraySize(int expectedCount);
}
