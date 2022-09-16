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
package io.trino.parquet.batchreader;

import io.trino.parquet.ColumnReader;
import io.trino.parquet.PrimitiveField;
import io.trino.spi.type.DecimalType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.joda.time.DateTimeZone;

import static org.apache.parquet.schema.OriginalType.DECIMAL;

public class BatchColumnReaderFactory
{
    private BatchColumnReaderFactory()
    {
    }

    public static ColumnReader createReader(PrimitiveField field, DateTimeZone timeZone)
    {
        final ColumnDescriptor descriptor = field.getDescriptor();
        final boolean isNested = descriptor.getPath().length > 1;
        if (isNested) {
            return null;
        }

        PrimitiveType.PrimitiveTypeName typeName = descriptor.getPrimitiveType().getPrimitiveTypeName();

        if (descriptor.getPrimitiveType().getOriginalType() == DECIMAL) {
            DecimalMetadata decimalMetadata = descriptor.getPrimitiveType().getDecimalMetadata();
            switch (typeName) {
                case INT32:
                    return new Int32DecimalBatchReader(descriptor, DecimalType.createDecimalType(decimalMetadata.getPrecision(), decimalMetadata.getScale()));
                case INT64:
                    return new Int64DecimalBatchReader(descriptor, DecimalType.createDecimalType(decimalMetadata.getPrecision(), decimalMetadata.getScale()));
            }
            return null;
        }

        switch (typeName) {
            case INT32:
                return new IntFlatBatchReader(descriptor);
            case INT64:
                if (descriptor.getPrimitiveType().getOriginalType() == OriginalType.TIME_MICROS) {
                    return new TimeMicrosFlatBatchReader(descriptor);
                }
                if (descriptor.getPrimitiveType().getOriginalType() == OriginalType.TIMESTAMP_MICROS) {
                    return new TimestampMicrosFlatBatchReader(descriptor);
                }
                if (descriptor.getPrimitiveType().getOriginalType() == OriginalType.TIMESTAMP_MILLIS) {
                    return new Int64TimestampMillisFlatBatchReader(descriptor);
                }
                if (descriptor.getPrimitiveType().getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation &&
                        ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) descriptor.getPrimitiveType().getLogicalTypeAnnotation()).getUnit() == LogicalTypeAnnotation.TimeUnit.NANOS) {
                    return new Int64TimestampNanosFlatBatchReader(descriptor);
                }
                return new LongFlatBatchReader(descriptor);
            case BOOLEAN:
                return new BooleanFlatBatchReader(descriptor);
            case FLOAT:
                return new FloatFlatBatchReader(descriptor);
            case DOUBLE:
                return new DoubleFlatBatchReader(descriptor);
        }

        return null;
    }
}
