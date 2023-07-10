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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.StandardTypes;
import io.trino.type.DateTimeOperators;

import java.util.concurrent.TimeUnit;

import static io.trino.operator.scalar.DateTimeFunctions.diffDate;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.util.Failures.checkCondition;
import static java.lang.Math.abs;
import static java.lang.Math.toIntExact;

public final class SequenceFunction
{
    private static final long MAX_RESULT_ENTRIES = 10_000;
    private static final Slice MONTH = Slices.utf8Slice("month");

    private SequenceFunction() {}

    @Description("Sequence function to generate synthetic arrays")
    @ScalarFunction("sequence")
    @SqlType("array(bigint)")
    public static Block sequence(
            @SqlType(StandardTypes.BIGINT) long start,
            @SqlType(StandardTypes.BIGINT) long stop,
            @SqlType(StandardTypes.BIGINT) long step)
    {
        return fixedWidthSequence(start, stop, step, BIGINT);
    }

    @ScalarFunction("sequence")
    @SqlType("array(bigint)")
    public static Block sequenceDefaultStep(
            @SqlType(StandardTypes.BIGINT) long start,
            @SqlType(StandardTypes.BIGINT) long stop)
    {
        return fixedWidthSequence(start, stop, stop >= start ? 1 : -1, BIGINT);
    }

    @ScalarFunction("sequence")
    @SqlType("array(date)")
    public static Block sequenceDateDefaultStep(
            @SqlType(StandardTypes.DATE) long start,
            @SqlType(StandardTypes.DATE) long stop)
    {
        return fixedWidthSequence(start, stop, stop >= start ? 1 : -1, DATE);
    }

    @ScalarFunction("sequence")
    @SqlType("array(date)")
    public static Block sequenceDateDayToSecond(
            @SqlType(StandardTypes.DATE) long start,
            @SqlType(StandardTypes.DATE) long stop,
            @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long step)
    {
        checkCondition(
                step % TimeUnit.DAYS.toMillis(1) == 0,
                INVALID_FUNCTION_ARGUMENT,
                "sequence step must be a day interval if start and end values are dates");
        return fixedWidthSequence(start, stop, step / TimeUnit.DAYS.toMillis(1), DATE);
    }

    @ScalarFunction("sequence")
    @SqlType("array(date)")
    public static Block sequenceDateYearToMonth(
            @SqlType(StandardTypes.DATE) long start,
            @SqlType(StandardTypes.DATE) long stop,
            @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long step)
    {
        checkValidStep(start, stop, step);

        int length = checkMaxEntry(diffDate(MONTH, start, stop) / step + 1);

        BlockBuilder blockBuilder = DATE.createBlockBuilder(null, length);

        int value = 0;
        for (int i = 0; i < length; ++i) {
            DATE.writeLong(blockBuilder, DateTimeOperators.datePlusIntervalYearToMonth(start, value));
            value += step;
        }

        return blockBuilder.build();
    }

    private static Block fixedWidthSequence(long start, long stop, long step, FixedWidthType type)
    {
        checkValidStep(start, stop, step);

        int length = getLength(start, stop, step);

        BlockBuilder blockBuilder = type.createBlockBuilder(null, length);
        for (long i = 0, value = start; i < length; ++i, value += step) {
            type.writeLong(blockBuilder, value);
        }
        return blockBuilder.build();
    }

    private static int getLength(long start, long stop, long step)
    {
        // handle the case when start and stop are either both positive, or both negative
        if ((start > 0 && stop > 0) || (start < 0 && stop < 0)) {
            int length = checkMaxEntry((stop - start) / step);
            return checkMaxEntry(length + 1);
        }

        // handle small step
        if (step == -1 || step == 1) {
            checkMaxEntry(start);
            checkMaxEntry(stop);
            return checkMaxEntry((stop - start) / step + 1);
        }

        // handle the remaining cases: start and step are of different sign or zero; step absolute value is greater than 1
        int startLength = abs(checkMaxEntry(start / step));
        int stopLength = abs(checkMaxEntry(stop / step));
        long startRemain = start % step;
        long stopRemain = stop % step;
        int remainLength;
        if (step > 0) {
            remainLength = startRemain + step <= stopRemain ? 2 : 1;
        }
        else {
            remainLength = startRemain + step >= stopRemain ? 2 : 1;
        }
        return checkMaxEntry(startLength + stopLength + remainLength);
    }

    public static void checkValidStep(long start, long stop, long step)
    {
        checkCondition(
                step != 0,
                INVALID_FUNCTION_ARGUMENT,
                "step must not be zero");
        checkCondition(
                step > 0 ? stop >= start : stop <= start,
                INVALID_FUNCTION_ARGUMENT,
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
    }

    public static int checkMaxEntry(long length)
    {
        checkCondition(
                -MAX_RESULT_ENTRIES <= length && length <= MAX_RESULT_ENTRIES,
                INVALID_FUNCTION_ARGUMENT,
                "result of sequence function must not have more than %d entries".formatted(MAX_RESULT_ENTRIES));

        return toIntExact(length);
    }
}
