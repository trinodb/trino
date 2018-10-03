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
import io.prestosql.spi.type.DateTimeEncoding;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;
import org.apache.parquet.schema.OriginalType;

import java.util.concurrent.TimeUnit;

public class LongColumnReader
        extends PrimitiveColumnReader
{
    public LongColumnReader(RichColumnDescriptor descriptor)
    {
        super(descriptor);
    }

    @Override
    protected void readValue(BlockBuilder blockBuilder, Type type)
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            // There is no easy way to support this for both iceberg and hive. hive does not have
            // timestamp with timezone and iceberg stores timestamp with timezone as INT_64. Presto
            // has its own way of storing timestamp with timezone and it has its own way to preserve
            // the zone where as iceberg does not preserve zone info but stores just microseconds from 1970-01-01 00:00:00.000000 UTC
            // in an 8-byte little-endian long. Given there are only 2 consumers right now we are hacking
            // this piece and converting the long to UTC millisecond and writing it in presto timestamp with timezone's long format
            // so presto-iceberg can handle timestamp with timezone type correctly
            if (columnDescriptor.getPrimitiveType().getOriginalType() == OriginalType.TIMESTAMP_MICROS) {
                final long utcMillis = TimeUnit.MICROSECONDS.toMillis(valuesReader.readLong());
                if (type instanceof TimestampWithTimeZoneType) {
                    final long dateTimeWithZone = DateTimeEncoding.packDateTimeWithZone(utcMillis, TimeZoneKey.UTC_KEY);
                    type.writeLong(blockBuilder, dateTimeWithZone);
                }
                else {
                    type.writeLong(blockBuilder, utcMillis);
                }
            }
            else {
                type.writeLong(blockBuilder, valuesReader.readLong());
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
