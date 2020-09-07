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
package io.prestosql.operator.scalar.timestamp;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TimestampType;

import static io.prestosql.operator.scalar.SequenceFunction.checkMaxEntry;
import static io.prestosql.operator.scalar.SequenceFunction.checkValidStep;
import static io.prestosql.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static io.prestosql.spi.type.TimestampType.createTimestampType;
import static io.prestosql.spi.type.TimestampTypes.writeLongTimestamp;
import static io.prestosql.type.DateTimes.MICROSECONDS_PER_MILLISECOND;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;

@ScalarFunction("sequence")
public final class SequenceIntervalDayToSecond
{
    // We need these because it's currently not possible to inject the fully-bound type into the methods that require them below
    private static final TimestampType SHORT_TYPE = createTimestampType(0);
    private static final TimestampType LONG_TYPE = createTimestampType(MAX_SHORT_PRECISION + 1);

    private SequenceIntervalDayToSecond() {}

    @LiteralParameters("p")
    @SqlType("array(timestamp(p))")
    public static Block sequence(
            @SqlType("timestamp(p)") long start,
            @SqlType("timestamp(p)") long stop,
            @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long step)
    {
        // scale to micros
        step = multiplyExact(step, MICROSECONDS_PER_MILLISECOND);

        checkValidStep(start, stop, step);

        int length = toIntExact((stop - start) / step + 1L);
        checkMaxEntry(length);

        BlockBuilder blockBuilder = SHORT_TYPE.createBlockBuilder(null, length);
        for (long i = 0, value = start; i < length; ++i, value += step) {
            SHORT_TYPE.writeLong(blockBuilder, value);
        }
        return blockBuilder.build();
    }

    @LiteralParameters("p")
    @SqlType("array(timestamp(p))")
    public static Block sequence(
            @SqlType("timestamp(p)") LongTimestamp start,
            @SqlType("timestamp(p)") LongTimestamp stop,
            @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long step)
    {
        step = multiplyExact(step, MICROSECONDS_PER_MILLISECOND); // scale to micros

        long startMicros = start.getEpochMicros();
        long stopMicros = stop.getEpochMicros();
        checkValidStep(startMicros, stopMicros, step);

        int length = toIntExact((stopMicros - startMicros) / step + 1L);
        checkMaxEntry(length);

        BlockBuilder blockBuilder = LONG_TYPE.createBlockBuilder(null, length);
        for (long i = 0, epochMicros = startMicros; i < length; ++i, epochMicros += step) {
            writeLongTimestamp(blockBuilder, epochMicros, start.getPicosOfMicro());
        }
        return blockBuilder.build();
    }
}
