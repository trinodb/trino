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
package io.prestosql.parquet.reader;

import io.prestosql.parquet.RichColumnDescriptor;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.LongTimestampWithTimeZone;
import io.prestosql.spi.type.Type;

import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.prestosql.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.toIntExact;

public class TimestampMicrosColumnReader
        extends PrimitiveColumnReader
{
    public TimestampMicrosColumnReader(RichColumnDescriptor descriptor)
    {
        super(descriptor);
    }

    @Override
    protected void readValue(BlockBuilder blockBuilder, Type type)
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            long epochMicros = valuesReader.readLong();
            // TODO: specialize the class at creation time
            if (type.equals(TIMESTAMP_MICROS)) {
                type.writeLong(blockBuilder, epochMicros);
            }
            else if (type.equals(TIMESTAMP_TZ_MICROS)) {
                long epochMillis = floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND);
                int picosOfMillis = toIntExact(epochMicros % MICROSECONDS_PER_MILLISECOND) * PICOSECONDS_PER_MICROSECOND;
                type.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, picosOfMillis, UTC_KEY));
            }
            else {
                throw new IllegalArgumentException("wrong type: " + type);
            }
        }
        else if (isValueNull()) {
            blockBuilder.appendNull();
        }
    }

    @Override
    protected void skipValue()
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            valuesReader.readLong();
        }
    }
}
