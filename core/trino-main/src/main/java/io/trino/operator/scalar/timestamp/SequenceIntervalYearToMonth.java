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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.operator.scalar.timestamp.TimestampOperators.TimestampPlusIntervalYearToMonth;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimestampType;

import static io.trino.operator.scalar.SequenceFunction.checkMaxEntry;
import static io.trino.operator.scalar.SequenceFunction.checkValidStep;
import static io.trino.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static io.trino.spi.type.TimestampType.createTimestampType;

@ScalarFunction("sequence")
public final class SequenceIntervalYearToMonth
{
    // We need these because it's currently not possible to inject the fully-bound type into the methods that require them below
    private static final TimestampType SHORT_TYPE = createTimestampType(0);
    private static final TimestampType LONG_TYPE = createTimestampType(MAX_SHORT_PRECISION + 1);

    private static final Slice MONTH = Slices.utf8Slice("month");

    private SequenceIntervalYearToMonth() {}

    @LiteralParameters("p")
    @SqlType("array(timestamp(p))")
    public static Block sequence(
            @SqlType("timestamp(p)") long start,
            @SqlType("timestamp(p)") long stop,
            @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long step)
    {
        checkValidStep(start, stop, step);

        int length = checkMaxEntry(DateDiff.diff(MONTH, start, stop) / step + 1);

        BlockBuilder blockBuilder = SHORT_TYPE.createBlockBuilder(null, length);

        int offset = 0;
        for (int i = 0; i < length; ++i) {
            long value = TimestampPlusIntervalYearToMonth.add(start, offset);
            SHORT_TYPE.writeLong(blockBuilder, value);
            offset += step;
        }

        return blockBuilder.build();
    }

    @LiteralParameters("p")
    @SqlType("array(timestamp(p))")
    public static Block sequence(
            ConnectorSession session,
            @SqlType("timestamp(p)") LongTimestamp start,
            @SqlType("timestamp(p)") LongTimestamp stop,
            @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long step)
    {
        checkValidStep(start.getEpochMicros(), stop.getEpochMicros(), step);

        int length = checkMaxEntry(DateDiff.diff(MONTH, start, stop) / step + 1);

        BlockBuilder blockBuilder = LONG_TYPE.createBlockBuilder(null, length);

        int offset = 0;
        for (int i = 0; i < length; ++i) {
            LongTimestamp value = TimestampPlusIntervalYearToMonth.add(start, offset);
            LONG_TYPE.writeObject(blockBuilder, value);
            offset += step;
        }

        return blockBuilder.build();
    }
}
