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
package io.trino.operator.scalar.timestamp;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Constraint;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.type.LongInterval;

import java.math.BigInteger;

import static io.trino.operator.scalar.SequenceFunction.checkMaxEntry;
import static io.trino.operator.scalar.SequenceFunction.checkValidStep;
import static io.trino.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampTypes.writeLongTimestamp;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;

@ScalarFunction("sequence")
public final class SequenceIntervalDayToSecond
{
    // We need these because it's currently not possible to inject the fully-bound type into the methods that require them below
    private static final TimestampType SHORT_TYPE = createTimestampType(0);
    private static final TimestampType LONG_TYPE = createTimestampType(MAX_SHORT_PRECISION + 1);

    private SequenceIntervalDayToSecond() {}

    @LiteralParameters({"p", "q", "r", "u"})
    @Constraint(variable = "u", expression = "max(p, r)")
    @SqlType("array(timestamp(u))")
    public static Block sequence(
            @SqlType("timestamp(p)") long start,
            @SqlType("timestamp(p)") long stop,
            @SqlType("interval day(q) to second(r)") long step)
    {
        checkValidStep(start, stop, step);
        int length = sequenceLength(BigInteger.valueOf(start), BigInteger.valueOf(stop), BigInteger.valueOf(step));

        BlockBuilder blockBuilder = SHORT_TYPE.createFixedSizeBlockBuilder(length);
        for (long i = 0, value = start; i < length; ++i, value += step) {
            SHORT_TYPE.writeLong(blockBuilder, value);
        }
        return blockBuilder.build();
    }

    @LiteralParameters({"p", "q", "r", "u"})
    @Constraint(variable = "u", expression = "max(p, r)")
    @SqlType("array(timestamp(u))")
    public static Block sequence(
            @SqlType("timestamp(p)") LongTimestamp start,
            @SqlType("timestamp(p)") LongTimestamp stop,
            @SqlType("interval day(q) to second(r)") long step)
    {
        return sequence(start, stop, new LongInterval(step, 0));
    }

    @LiteralParameters({"p", "q", "r", "u"})
    @Constraint(variable = "u", expression = "max(p, r)")
    @SqlType("array(timestamp(u))")
    public static Block sequence(
            @SqlType("timestamp(p)") long start,
            @SqlType("timestamp(p)") long stop,
            @SqlType("interval day(q) to second(r)") LongInterval step)
    {
        return sequence(new LongTimestamp(start, 0), new LongTimestamp(stop, 0), step);
    }

    @LiteralParameters({"p", "q", "r", "u"})
    @Constraint(variable = "u", expression = "max(p, r)")
    @SqlType("array(timestamp(u))")
    public static Block sequence(
            @SqlType("timestamp(p)") LongTimestamp start,
            @SqlType("timestamp(p)") LongTimestamp stop,
            @SqlType("interval day(q) to second(r)") LongInterval step)
    {
        BigInteger startPicos = picoseconds(start.getEpochMicros(), start.getPicosOfMicro());
        BigInteger stopPicos = picoseconds(stop.getEpochMicros(), stop.getPicosOfMicro());
        BigInteger stepPicos = picoseconds(step.getMicros(), step.getPicosOfMicro());
        checkValidStep(0, stopPicos.compareTo(startPicos), stepPicos.signum());
        int length = sequenceLength(startPicos, stopPicos, stepPicos);

        BlockBuilder blockBuilder = LONG_TYPE.createFixedSizeBlockBuilder(length);
        LongTimestamp value = start;
        for (int i = 0; i < length; i++) {
            writeLongTimestamp(blockBuilder, value.getEpochMicros(), value.getPicosOfMicro());
            if (i + 1 < length) {
                value = TimestampOperators.TimestampPlusIntervalDayToSecond.add(value, step);
            }
        }
        return blockBuilder.build();
    }

    private static BigInteger picoseconds(long micros, int picosOfMicro)
    {
        return BigInteger.valueOf(micros).multiply(BigInteger.valueOf(PICOSECONDS_PER_MICROSECOND)).add(BigInteger.valueOf(picosOfMicro));
    }

    private static int sequenceLength(BigInteger start, BigInteger stop, BigInteger step)
    {
        // The endpoint difference can exceed a long even when the sequence has only a few entries.
        BigInteger length = stop.subtract(start).divide(step).add(BigInteger.ONE);
        return checkMaxEntry(length.bitLength() > 63 ? Long.MAX_VALUE : length.longValueExact());
    }
}
