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
package io.trino.server.protocol.spooling.encoding.arrow;

import io.trino.spi.block.Block;
import org.apache.arrow.vector.FixedWidthVector;

public abstract sealed class FixedWidthWriter<V extends FixedWidthVector>
        extends PrimitiveWriter<V> permits
        BigintWriter,
        BooleanWriter,
        DateWriter,
        DecimalWriter,
        DoubleWriter,
        IntegerWriter,
        IntervalDayWriter,
        IntervalYearMonthWriter,
        RealWriter,
        SmallIntWriter,
        TimeSecWriter,
        TimeMilliWriter,
        TimeMicroWriter,
        TimeNanoWriter,
        TimestampSecWriter,
        TimestampMilliWriter,
        TimestampMicroWriter,
        TimestampNanoWriter,
        TimestampWithTimeZoneSecWriter,
        TimestampWithTimeZoneMilliWriter,
        TimestampWithTimeZoneMicroWriter,
        TimestampWithTimeZoneNanoWriter,
        TimeWithTimeZoneSecWriter,
        TimeWithTimeZoneMilliWriter,
        TimeWithTimeZoneMicroWriter,
        TimeWithTimeZoneNanoWriter,
        TinyIntWriter,
        UuidWriter
{
    protected FixedWidthWriter(V vector)
    {
        super(vector);
    }

    @Override
    protected void initialize(Block block)
    {
        vector.allocateNew(block.getPositionCount());
    }
}
