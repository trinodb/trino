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

import io.trino.parquet.PrimitiveField;
import io.trino.spi.TrinoException;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.joda.time.DateTimeZone;

import java.util.Optional;

import static io.trino.parquet.ParquetTypeUtils.createDecimalType;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.NANOS;

public final class ColumnReaderFactory
{
    private ColumnReaderFactory() {}

    public static ColumnReader create(PrimitiveField field, DateTimeZone timeZone)
    {
        PrimitiveType primitiveType = field.getDescriptor().getPrimitiveType();
        return switch (primitiveType.getPrimitiveTypeName()) {
            case BOOLEAN -> new BooleanColumnReader(field);
            case INT32 -> createDecimalColumnReader(field).orElse(new IntColumnReader(field));
            case INT64 -> {
                if (primitiveType.getLogicalTypeAnnotation() instanceof TimeLogicalTypeAnnotation timeAnnotation) {
                    if (timeAnnotation.getUnit() == MICROS) {
                        yield new TimeMicrosColumnReader(field);
                    }
                    throw new TrinoException(NOT_SUPPORTED, "Unsupported column: " + field.getDescriptor());
                }
                if (primitiveType.getLogicalTypeAnnotation() instanceof TimestampLogicalTypeAnnotation timestampAnnotation) {
                    if (timestampAnnotation.getUnit() == MILLIS) {
                        yield new Int64TimestampMillisColumnReader(field);
                    }
                    if (timestampAnnotation.getUnit() == MICROS) {
                        yield new TimestampMicrosColumnReader(field);
                    }
                    if (timestampAnnotation.getUnit() == NANOS) {
                        yield new Int64TimestampNanosColumnReader(field);
                    }
                    throw new TrinoException(NOT_SUPPORTED, "Unsupported column: " + field.getDescriptor());
                }
                yield createDecimalColumnReader(field).orElse(new LongColumnReader(field));
            }
            case INT96 -> new TimestampColumnReader(field, timeZone);
            case FLOAT -> new FloatColumnReader(field);
            case DOUBLE -> new DoubleColumnReader(field);
            case BINARY -> createDecimalColumnReader(field).orElse(new BinaryColumnReader(field));
            case FIXED_LEN_BYTE_ARRAY -> {
                Optional<PrimitiveColumnReader> decimalColumnReader = createDecimalColumnReader(field);
                if (decimalColumnReader.isPresent()) {
                    yield decimalColumnReader.get();
                }
                if (isLogicalUuid(primitiveType)) {
                    yield new UuidColumnReader(field);
                }
                if (primitiveType.getLogicalTypeAnnotation() == null) {
                    // Iceberg 0.11.1 writes UUID as FIXED_LEN_BYTE_ARRAY without logical type annotation (see https://github.com/apache/iceberg/pull/2913)
                    // To support such files, we bet on the type to be UUID, which gets verified later, when reading the column data.
                    yield new UuidColumnReader(field);
                }
                throw new TrinoException(NOT_SUPPORTED, "Unsupported column: " + field.getDescriptor());
            }
        };
    }

    private static boolean isLogicalUuid(PrimitiveType primitiveType)
    {
        return Optional.ofNullable(primitiveType.getLogicalTypeAnnotation())
                .flatMap(logicalTypeAnnotation -> logicalTypeAnnotation.accept(new LogicalTypeAnnotationVisitor<Boolean>()
                {
                    @Override
                    public Optional<Boolean> visit(UUIDLogicalTypeAnnotation uuidLogicalType)
                    {
                        return Optional.of(TRUE);
                    }
                }))
                .orElse(FALSE);
    }

    private static Optional<PrimitiveColumnReader> createDecimalColumnReader(PrimitiveField field)
    {
        return createDecimalType(field)
                .map(decimalType -> DecimalColumnReaderFactory.createReader(field, decimalType));
    }
}
