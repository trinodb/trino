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

import io.airlift.units.DataSize;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.type.UnknownType.UNKNOWN;
import static io.trino.util.Failures.checkCondition;
import static java.lang.Math.toIntExact;

@ScalarFunction("repeat")
@Description("Repeat an element for a given number of times")
public final class RepeatFunction
{
    private static final long MAX_RESULT_ENTRIES = 100_000;
    private static final long MAX_SIZE_IN_BYTES = DataSize.of(4, MEGABYTE).toBytes();

    private RepeatFunction() {}

    @SqlType("array(unknown)")
    public static Block repeat(
            @SqlNullable @SqlType("unknown") Boolean element,
            @SqlType(StandardTypes.BIGINT) long count)
    {
        checkCondition(element == null, INVALID_FUNCTION_ARGUMENT, "expect null values");
        checkCountConditions(count);
        return repeatNullValues(UNKNOWN, count);
    }

    @TypeParameter("T")
    @SqlType("array(T)")
    public static Block repeat(
            @TypeParameter("T") Type type,
            @SqlNullable @SqlType("T") Object element,
            @SqlType(StandardTypes.BIGINT) long count)
    {
        Block result = repeatValue(type, element, count);
        checkMaxSize(result);
        return result;
    }

    @TypeParameter("T")
    @SqlType("array(T)")
    public static Block repeat(
            @TypeParameter("T") Type type,
            @SqlNullable @SqlType("T") Long element,
            @SqlType(StandardTypes.BIGINT) long count)
    {
        return repeatValue(type, element, count);
    }

    @TypeParameter("T")
    @SqlType("array(T)")
    public static Block repeat(
            @TypeParameter("T") Type type,
            @SqlNullable @SqlType("T") Boolean element,
            @SqlType(StandardTypes.BIGINT) long count)
    {
        return repeatValue(type, element, count);
    }

    @TypeParameter("T")
    @SqlType("array(T)")
    public static Block repeat(
            @TypeParameter("T") Type type,
            @SqlNullable @SqlType("T") Double element,
            @SqlType(StandardTypes.BIGINT) long count)
    {
        return repeatValue(type, element, count);
    }

    private static Block repeatValue(Type type, Object element, long count)
    {
        checkCountConditions(count);
        if (element == null) {
            return repeatNullValues(type, count);
        }
        return RunLengthEncodedBlock.create(type, element, toIntExact(count));
    }

    private static void checkCountConditions(long count)
    {
        checkCondition(count <= MAX_RESULT_ENTRIES, INVALID_FUNCTION_ARGUMENT, "count argument of repeat function must be less than or equal to %s", MAX_RESULT_ENTRIES);
        checkCondition(count >= 0, INVALID_FUNCTION_ARGUMENT, "count argument of repeat function must be greater than or equal to 0");
    }

    private static Block repeatNullValues(Type type, long count)
    {
        return RunLengthEncodedBlock.create(type, null, toIntExact(count));
    }

    private static void checkMaxSize(Block block)
    {
        checkCondition(
                block.getSizeInBytes() <= MAX_SIZE_IN_BYTES,
                INVALID_FUNCTION_ARGUMENT,
                "result of repeat function must not take more than %s bytes",
                MAX_SIZE_IN_BYTES);
    }
}
