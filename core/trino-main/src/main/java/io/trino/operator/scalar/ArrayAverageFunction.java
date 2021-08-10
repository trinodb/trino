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
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.function.TypeParameterSpecialization;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;

import java.math.BigDecimal;
import java.util.function.BiFunction;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.Decimals.decodeUnscaledValue;
import static io.trino.spi.type.Decimals.isLongDecimal;
import static io.trino.spi.type.Decimals.isShortDecimal;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;

@ScalarFunction("array_average")
@Description("Calculates the average of all non-null elements in the given array. Returns null if no non-null elements are found.")
public final class ArrayAverageFunction
{
    private ArrayAverageFunction() {}

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = double.class)
    @SqlType(StandardTypes.DOUBLE)
    @SqlNullable
    public static Double doubleArrayAverage(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        return calculateAverage(block, elementType::getDouble);
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = long.class)
    @SqlType(StandardTypes.DOUBLE)
    @SqlNullable
    public static Double longArrayAverage(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        return calculateAverage(block, (type, position) -> createLongConvertFunction(elementType).apply(block, position));
    }

    private static BiFunction<Block, Integer, Double> createLongConvertFunction(Type type)
    {
        if (type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(BIGINT)) {
            return (block, position) -> (double) type.getLong(block, position);
        }
        if (isShortDecimal(type)) {
            int scale = ((DecimalType) type).getScale();
            return (block, position) -> BigDecimal.valueOf(type.getLong(block, position), scale).doubleValue();
        }
        if (isLongDecimal(type)) {
            int scale = ((DecimalType) type).getScale();
            return (block, position) -> new BigDecimal(decodeUnscaledValue(type.getSlice(block, position)), scale).doubleValue();
        }
        throw new IllegalArgumentException("Type is not supported: " + type);
    }

    private static Double calculateAverage(Block block, BiFunction<Block, Integer, Double> convertFunction)
    {
        int positionCount = block.getPositionCount();
        double sum = 0;
        int nonNullElementsCount = 0;
        for (int position = 0; position < positionCount; position++) {
            if (block.isNull(position)) {
                continue;
            }
            nonNullElementsCount++;
            sum += convertFunction.apply(block, position);
        }
        return nonNullElementsCount > 0 ? sum / nonNullElementsCount : null;
    }
}
