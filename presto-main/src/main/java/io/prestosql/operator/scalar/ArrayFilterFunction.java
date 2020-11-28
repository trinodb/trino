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
package io.prestosql.operator.scalar;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.function.TypeParameterSpecialization;
import io.prestosql.spi.type.Type;

import static java.lang.Boolean.TRUE;

@Description("Return array containing elements that match the given predicate")
@ScalarFunction(value = "filter", deterministic = false)
public final class ArrayFilterFunction
{
    private ArrayFilterFunction() {}

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = long.class)
    @SqlType("array(T)")
    public static Block filterLong(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") LongToBooleanFunction function)
    {
        int positionCount = arrayBlock.getPositionCount();
        BlockBuilder resultBuilder = elementType.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            Long input = null;
            if (!arrayBlock.isNull(position)) {
                input = elementType.getLong(arrayBlock, position);
            }

            Boolean keep = function.apply(input);
            if (TRUE.equals(keep)) {
                elementType.appendTo(arrayBlock, position, resultBuilder);
            }
        }
        return resultBuilder.build();
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = double.class)
    @SqlType("array(T)")
    public static Block filterDouble(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") DoubleToBooleanFunction function)
    {
        int positionCount = arrayBlock.getPositionCount();
        BlockBuilder resultBuilder = elementType.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            Double input = null;
            if (!arrayBlock.isNull(position)) {
                input = elementType.getDouble(arrayBlock, position);
            }

            Boolean keep = function.apply(input);
            if (TRUE.equals(keep)) {
                elementType.appendTo(arrayBlock, position, resultBuilder);
            }
        }
        return resultBuilder.build();
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = boolean.class)
    @SqlType("array(T)")
    public static Block filterBoolean(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") BooleanToBooleanFunction function)
    {
        int positionCount = arrayBlock.getPositionCount();
        BlockBuilder resultBuilder = elementType.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            Boolean input = null;
            if (!arrayBlock.isNull(position)) {
                input = elementType.getBoolean(arrayBlock, position);
            }

            Boolean keep = function.apply(input);
            if (TRUE.equals(keep)) {
                elementType.appendTo(arrayBlock, position, resultBuilder);
            }
        }
        return resultBuilder.build();
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Object.class)
    @SqlType("array(T)")
    public static Block filterObject(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") ObjectToBooleanFunction function)
    {
        int positionCount = arrayBlock.getPositionCount();
        BlockBuilder resultBuilder = elementType.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            Object input = null;
            if (!arrayBlock.isNull(position)) {
                input = elementType.getObject(arrayBlock, position);
            }

            Boolean keep = function.apply(input);
            if (TRUE.equals(keep)) {
                elementType.appendTo(arrayBlock, position, resultBuilder);
            }
        }
        return resultBuilder.build();
    }
}
