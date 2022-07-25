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
import it.unimi.dsi.fastutil.HashCommon;

import static io.trino.SystemSessionProperties.isIncrementalHashArrayLoadFactorEnabled;

public interface HashArraySizeSupplier
{
    static HashArraySizeSupplier defaultHashArraySizeSupplier()
    {
        return expectedCount -> HashCommon.arraySize(expectedCount, 0.75f);
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
