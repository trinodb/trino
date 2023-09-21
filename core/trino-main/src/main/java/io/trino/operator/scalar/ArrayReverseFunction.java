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
import io.trino.spi.block.BufferedArrayValueBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;

@ScalarFunction("reverse")
@Description("Returns an array which has the reversed order of the given array.")
public final class ArrayReverseFunction
{
    private final BufferedArrayValueBuilder arrayValueBuilder;

    @TypeParameter("E")
    public ArrayReverseFunction(@TypeParameter("E") Type elementType)
    {
        arrayValueBuilder = BufferedArrayValueBuilder.createBuffered(new ArrayType(elementType));
    }

    @TypeParameter("E")
    @SqlType("array(E)")
    public Block reverse(
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block block)
    {
        int arrayLength = block.getPositionCount();

        if (arrayLength < 2) {
            return block;
        }

        return arrayValueBuilder.build(arrayLength, elementBuilder -> {
            for (int i = arrayLength - 1; i >= 0; i--) {
                type.appendTo(block, i, elementBuilder);
            }
        });
    }
}
