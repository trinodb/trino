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
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.ArrayType;

import static io.trino.type.UnknownType.UNKNOWN;

public final class ArrayFunctions
{
    private ArrayFunctions() {}

    @ScalarFunction(value = ArrayConstructor.NAME, hidden = true)
    @SqlType("array(unknown)")
    public static Block arrayConstructor()
    {
        BlockBuilder blockBuilder = new ArrayType(UNKNOWN).createBlockBuilder(null, 0);
        return blockBuilder.build();
    }
}
