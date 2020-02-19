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
import io.prestosql.spi.type.Type;
import org.apache.parquet.io.api.Binary;

import static io.prestosql.parquet.ParquetTimestampUtils.getTimestampMillis;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.parquet.schema.OriginalType.TIMESTAMP_MICROS;
import static org.apache.parquet.schema.OriginalType.TIMESTAMP_MILLIS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;

public class TimestampColumnReader
        extends PrimitiveColumnReader
{
    public TimestampColumnReader(RichColumnDescriptor columnDescriptor, Type sourceType, Type targetType)
    {
        super(columnDescriptor, sourceType, targetType);
    }

    @Override
    protected void readValue(BlockBuilder blockBuilder)
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            if (columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == INT64) {
                if (columnDescriptor.getPrimitiveType().getOriginalType() == TIMESTAMP_MILLIS) {
                    sourceType.writeLong(blockBuilder, MILLISECONDS.toMillis(valuesReader.readLong()));
                }
                else if (columnDescriptor.getPrimitiveType().getOriginalType() == TIMESTAMP_MICROS) {
                    sourceType.writeLong(blockBuilder, MICROSECONDS.toMillis(valuesReader.readLong()));
                }
            }
            else if (columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == INT96) {
                Binary binary = valuesReader.readBytes();
                sourceType.writeLong(blockBuilder, getTimestampMillis(binary));
            }
            else {
                throw new UnsupportedOperationException(format("Could not read type TIMESTAMP from %s", columnDescriptor.getPrimitiveType()));
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
            if (columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == INT64) {
                valuesReader.readLong();
            }
            else if (columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == INT96) {
                valuesReader.readBytes();
            }
            else {
                throw new UnsupportedOperationException(format("Could not skip type TIMESTAMP with %s", columnDescriptor.getPrimitiveType()));
            }
        }
    }
}
