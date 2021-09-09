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
package io.trino.parquet.reader;

import io.trino.parquet.RichColumnDescriptor;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.Type;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.String.format;

public class Int64TimestampNanosColumnReader
        extends PrimitiveColumnReader
{
    public Int64TimestampNanosColumnReader(RichColumnDescriptor descriptor)
    {
        super(descriptor);
    }

    @Override
    protected void readValue(BlockBuilder blockBuilder, Type type)
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            long epochNanos = valuesReader.readLong();
            // TODO: specialize the class at creation time
            if (type == TIMESTAMP_MILLIS) {
                type.writeLong(blockBuilder, Timestamps.round(epochNanos, 6) / NANOSECONDS_PER_MICROSECOND);
            }
            else if (type == TIMESTAMP_MICROS) {
                type.writeLong(blockBuilder, Timestamps.round(epochNanos, 3) / NANOSECONDS_PER_MICROSECOND);
            }
            else if (type == TIMESTAMP_NANOS) {
                type.writeObject(blockBuilder, new LongTimestamp(
                        floorDiv(epochNanos, NANOSECONDS_PER_MICROSECOND),
                        floorMod(epochNanos, NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND));
            }
            else {
                throw new TrinoException(NOT_SUPPORTED, format("Unsupported Trino column type (%s) for Parquet column (%s)", type, columnDescriptor));
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
