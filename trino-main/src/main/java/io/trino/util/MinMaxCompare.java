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
package io.trino.util;

import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.FunctionDependencies;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionComparison;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static io.trino.spi.function.OperatorType.COMPARISON;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.Float.intBitsToFloat;
import static java.lang.invoke.MethodHandles.filterReturnValue;

public final class MinMaxCompare
{
    private static final MethodHandle MIN_FUNCTION = methodHandle(MinMaxCompare.class, "min", long.class);
    private static final MethodHandle MAX_FUNCTION = methodHandle(MinMaxCompare.class, "max", long.class);

    public static final MethodHandle MAX_REAL_FUNCTION = methodHandle(MinMaxCompare.class, "maxReal", long.class, long.class);
    public static final MethodHandle MAX_DOUBLE_FUNCTION = methodHandle(MinMaxCompare.class, "maxDouble", double.class, double.class);

    private MinMaxCompare() {}

    public static MethodHandle getMinMaxCompare(FunctionDependencies dependencies, Type type, InvocationConvention convention, boolean min)
    {
        if (!min && type.equals(REAL)) {
            return MAX_REAL_FUNCTION;
        }
        if (!min && type.equals(DOUBLE)) {
            return MAX_DOUBLE_FUNCTION;
        }
        MethodHandle handle = dependencies.getOperatorInvoker(COMPARISON, List.of(type, type), convention).getMethodHandle();
        return filterReturnValue(handle, min ? MIN_FUNCTION : MAX_FUNCTION);
    }

    public static BlockPositionComparison getMaxCompare(BlockTypeOperators operators, Type type)
    {
        if (type.equals(REAL)) {
            return (leftBlock, leftPosition, rightBlock, rightPosition) -> {
                float left = toReal(REAL.getLong(leftBlock, leftPosition));
                float right = toReal(REAL.getLong(rightBlock, rightPosition));
                if (Float.isNaN(left) && Float.isNaN(right)) {
                    return 0;
                }
                if (Float.isNaN(left)) {
                    return -1;
                }
                if (Float.isNaN(right)) {
                    return 1;
                }
                return Float.compare(left, right);
            };
        }
        if (type.equals(DOUBLE)) {
            return (leftBlock, leftPosition, rightBlock, rightPosition) -> {
                double left = DOUBLE.getDouble(leftBlock, leftPosition);
                double right = DOUBLE.getDouble(rightBlock, rightPosition);
                if (Double.isNaN(left) && Double.isNaN(right)) {
                    return 0;
                }
                if (Double.isNaN(left)) {
                    return -1;
                }
                if (Double.isNaN(right)) {
                    return 1;
                }
                return Double.compare(left, right);
            };
        }
        return operators.getComparisonOperator(type);
    }

    @UsedByGeneratedCode
    public static boolean min(long comparisonResult)
    {
        return comparisonResult < 0;
    }

    @UsedByGeneratedCode
    public static boolean max(long comparisonResult)
    {
        return comparisonResult > 0;
    }

    @UsedByGeneratedCode
    public static boolean maxReal(long intLeft, long intRight)
    {
        float left = toReal(intLeft);
        float right = toReal(intRight);
        return (left > right) || Float.isNaN(right);
    }

    @UsedByGeneratedCode
    public static boolean maxDouble(double left, double right)
    {
        return (left > right) || Double.isNaN(right);
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    private static float toReal(long value)
    {
        return intBitsToFloat((int) value);
    }
}
