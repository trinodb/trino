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
import io.trino.spi.function.Convention;
import io.trino.spi.function.Description;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_FIRST;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.util.Failures.internalError;
import static java.lang.Float.intBitsToFloat;

@ScalarFunction("array_max")
@Description("Get maximum value of array")
public final class ArrayMaxFunction
{
    private ArrayMaxFunction() {}

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Long longArrayMax(
            @OperatorDependency(
                    operator = COMPARISON_UNORDERED_FIRST,
                    argumentTypes = {"T", "T"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = FAIL_ON_NULL)) MethodHandle compareMethodHandle,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        int selectedPosition = findMaxArrayElement(compareMethodHandle, block);
        if (selectedPosition < 0) {
            return null;
        }
        return elementType.getLong(block, selectedPosition);
    }

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Boolean booleanArrayMax(
            @OperatorDependency(
                    operator = COMPARISON_UNORDERED_FIRST,
                    argumentTypes = {"T", "T"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = FAIL_ON_NULL)) MethodHandle compareMethodHandle,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        int selectedPosition = findMaxArrayElement(compareMethodHandle, block);
        if (selectedPosition < 0) {
            return null;
        }
        return elementType.getBoolean(block, selectedPosition);
    }

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Double doubleArrayMax(
            @OperatorDependency(
                    operator = COMPARISON_UNORDERED_FIRST,
                    argumentTypes = {"T", "T"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = FAIL_ON_NULL)) MethodHandle compareMethodHandle,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        int selectedPosition = findMaxArrayElement(compareMethodHandle, block);
        if (selectedPosition < 0) {
            return null;
        }
        return elementType.getDouble(block, selectedPosition);
    }

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Object objectArrayMax(
            @OperatorDependency(
                    operator = COMPARISON_UNORDERED_FIRST,
                    argumentTypes = {"T", "T"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = FAIL_ON_NULL)) MethodHandle compareMethodHandle,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        int selectedPosition = findMaxArrayElement(compareMethodHandle, block);
        if (selectedPosition < 0) {
            return null;
        }
        return elementType.getObject(block, selectedPosition);
    }

    private static int findMaxArrayElement(MethodHandle compareMethodHandle, Block block)
    {
        try {
            int selectedPosition = -1;
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (block.isNull(position)) {
                    return -1;
                }
                if (selectedPosition < 0 || ((long) compareMethodHandle.invokeExact(block, position, block, selectedPosition)) > 0) {
                    selectedPosition = position;
                }
            }
            return selectedPosition;
        }
        catch (Throwable t) {
            throw internalError(t);
        }
    }

    @SqlType("double")
    @SqlNullable
    public static Double doubleTypeArrayMax(@SqlType("array(double)") Block block)
    {
        if (block.getPositionCount() == 0) {
            return null;
        }
        int selectedPosition = -1;
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                return null;
            }
            if (selectedPosition < 0 || doubleGreater(DOUBLE.getDouble(block, position), DOUBLE.getDouble(block, selectedPosition))) {
                selectedPosition = position;
            }
        }
        return DOUBLE.getDouble(block, selectedPosition);
    }

    private static boolean doubleGreater(double left, double right)
    {
        return (left > right) || Double.isNaN(right);
    }

    @SqlType("real")
    @SqlNullable
    public static Long realTypeArrayMax(@SqlType("array(real)") Block block)
    {
        if (block.getPositionCount() == 0) {
            return null;
        }
        int selectedPosition = -1;
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                return null;
            }
            if (selectedPosition < 0 || floatGreater(getReal(block, position), getReal(block, selectedPosition))) {
                selectedPosition = position;
            }
        }
        return REAL.getLong(block, selectedPosition);
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    private static float getReal(Block block, int position)
    {
        return intBitsToFloat((int) REAL.getLong(block, position));
    }

    private static boolean floatGreater(float left, float right)
    {
        return (left > right) || Float.isNaN(right);
    }
}
