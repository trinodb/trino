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

import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.function.Convention;
import io.trino.spi.function.Description;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.StandardTypes;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.util.Failures.internalError;

@Description("Determines whether given value exists in the array")
@ScalarFunction("contains")
public final class ArrayContains
{
    private ArrayContains() {}

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean contains(
            @OperatorDependency(
                    operator = EQUAL,
                    argumentTypes = {"T", "T"},
                    convention = @Convention(arguments = {BLOCK_POSITION_NOT_NULL, NEVER_NULL}, result = NULLABLE_RETURN))
                    MethodHandle equals,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("T") Object value)
    {
        boolean foundNull = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                foundNull = true;
                continue;
            }
            try {
                Boolean result = (Boolean) equals.invoke(arrayBlock, i, value);
                checkNotIndeterminate(result);
                if (result) {
                    return true;
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean contains(
            @OperatorDependency(
                    operator = EQUAL,
                    argumentTypes = {"T", "T"},
                    convention = @Convention(arguments = {BLOCK_POSITION_NOT_NULL, NEVER_NULL}, result = NULLABLE_RETURN))
                    MethodHandle equals,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("T") long value)
    {
        boolean foundNull = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                foundNull = true;
                continue;
            }
            try {
                Boolean result = (Boolean) equals.invokeExact(arrayBlock, i, value);
                checkNotIndeterminate(result);
                if (result) {
                    return true;
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean contains(
            @OperatorDependency(
                    operator = EQUAL,
                    argumentTypes = {"T", "T"},
                    convention = @Convention(arguments = {BLOCK_POSITION_NOT_NULL, NEVER_NULL}, result = NULLABLE_RETURN))
                    MethodHandle equals,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("T") boolean value)
    {
        boolean foundNull = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                foundNull = true;
                continue;
            }
            try {
                Boolean result = (Boolean) equals.invokeExact(arrayBlock, i, value);
                checkNotIndeterminate(result);
                if (result) {
                    return true;
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean contains(
            @OperatorDependency(
                    operator = EQUAL,
                    argumentTypes = {"T", "T"},
                    convention = @Convention(arguments = {BLOCK_POSITION_NOT_NULL, NEVER_NULL}, result = NULLABLE_RETURN))
                    MethodHandle equals,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("T") double value)
    {
        boolean foundNull = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                foundNull = true;
                continue;
            }
            try {
                Boolean result = (Boolean) equals.invokeExact(arrayBlock, i, value);
                checkNotIndeterminate(result);
                if (result) {
                    return true;
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    private static void checkNotIndeterminate(Boolean equalsResult)
    {
        if (equalsResult == null) {
            throw new TrinoException(NOT_SUPPORTED, "contains does not support arrays with elements that are null or contain null");
        }
    }
}
