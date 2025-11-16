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
package io.trino.operator.scalar;

import io.trino.spi.block.Block;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;

import java.util.concurrent.ThreadLocalRandom;

@ScalarFunction(value = "shuffle", deterministic = false)
@Description("Generates a random permutation of the given array.")
public final class ArrayShuffleFunction
{
    private ArrayShuffleFunction() {}

    @TypeParameter("E")
    @SqlType("array(E)")
    public static Block shuffle(@SqlType("array(E)") Block block)
    {
        int length = block.getPositionCount();
        int[] positions = new int[length];
        for (int i = 0; i < length; i++) {
            positions[i] = i;
        }

        // Fisher-Yates shuffle
        // Randomly swap a pair of positions
        for (int i = length - 1; i > 0; i--) {
            int index = ThreadLocalRandom.current().nextInt(i + 1);
            int swap = positions[i];
            positions[i] = positions[index];
            positions[index] = swap;
        }

        return block.copyPositions(positions, 0, length);
    }
}
