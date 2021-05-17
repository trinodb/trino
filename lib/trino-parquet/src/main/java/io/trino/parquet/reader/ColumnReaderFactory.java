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
import org.apache.parquet.schema.OriginalType;
import org.joda.time.DateTimeZone;

import java.util.Optional;

import static io.trino.parquet.ParquetTypeUtils.createDecimalType;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class ColumnReaderFactory
{
    public ColumnReader create(RichColumnDescriptor descriptor, DateTimeZone timeZone)
    {
        switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
            case BOOLEAN:
                return new BooleanColumnReader(descriptor);
            case INT32:
                return createDecimalColumnReader(descriptor).orElse(new IntColumnReader(descriptor));
            case INT64:
                if (descriptor.getPrimitiveType().getOriginalType() == OriginalType.TIME_MICROS) {
                    return new TimeMicrosColumnReader(descriptor);
                }
                if (descriptor.getPrimitiveType().getOriginalType() == OriginalType.TIMESTAMP_MICROS) {
                    return new TimestampMicrosColumnReader(descriptor);
                }
                if (descriptor.getPrimitiveType().getOriginalType() == OriginalType.TIMESTAMP_MILLIS) {
                    return new Int64TimestampMillisColumnReader(descriptor);
                }
                return createDecimalColumnReader(descriptor).orElse(new LongColumnReader(descriptor));
            case INT96:
                return new TimestampColumnReader(descriptor, timeZone);
            case FLOAT:
                return new FloatColumnReader(descriptor);
            case DOUBLE:
                return new DoubleColumnReader(descriptor);
            case BINARY:
                return createDecimalColumnReader(descriptor).orElse(new BinaryColumnReader(descriptor));
            case FIXED_LEN_BYTE_ARRAY:
                return createDecimalColumnReader(descriptor)
                        .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, " type FIXED_LEN_BYTE_ARRAY supported as DECIMAL; got " + descriptor.getPrimitiveType().getOriginalType()));
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported parquet type: " + descriptor.getPrimitiveType().getPrimitiveTypeName());
    }

    private static Optional<PrimitiveColumnReader> createDecimalColumnReader(RichColumnDescriptor descriptor)
    {
        return createDecimalType(descriptor)
                .map(decimalType -> DecimalColumnReaderFactory.createReader(descriptor, decimalType));
    }
}
