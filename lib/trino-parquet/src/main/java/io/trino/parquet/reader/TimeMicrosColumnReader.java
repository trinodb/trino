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
import io.trino.spi.type.TimeType;
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.Type;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;

public class TimeMicrosColumnReader
        extends PrimitiveColumnReader
{
    public TimeMicrosColumnReader(RichColumnDescriptor descriptor)
    {
        super(descriptor);
    }

    @Override
    protected void readValue(BlockBuilder blockBuilder, Type type)
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            long picos = valuesReader.readLong() * Timestamps.PICOSECONDS_PER_MICROSECOND;
            if (type instanceof TimeType) {
                type.writeLong(blockBuilder, picos);
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
