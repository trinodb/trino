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

import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.decoders.TransformingValueDecoders;
import io.trino.parquet.reader.decoders.ValueDecoders;
import io.trino.parquet.reader.flat.FlatColumnReader;
import io.trino.spi.TrinoException;
import io.trino.spi.type.AbstractIntType;
import io.trino.spi.type.AbstractLongType;
import io.trino.spi.type.AbstractVariableWidthType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.joda.time.DateTimeZone;

import java.util.Optional;

import static io.trino.parquet.ParquetTypeUtils.createDecimalType;
import static io.trino.parquet.reader.decoders.TransformingValueDecoders.getInt96ToLongTimestampDecoder;
import static io.trino.parquet.reader.decoders.TransformingValueDecoders.getInt96ToShortTimestampDecoder;
import static io.trino.parquet.reader.flat.BinaryColumnAdapter.BINARY_ADAPTER;
import static io.trino.parquet.reader.flat.BooleanColumnAdapter.BOOLEAN_ADAPTER;
import static io.trino.parquet.reader.flat.ByteColumnAdapter.BYTE_ADAPTER;
import static io.trino.parquet.reader.flat.Int128ColumnAdapter.INT128_ADAPTER;
import static io.trino.parquet.reader.flat.Int96ColumnAdapter.INT96_ADAPTER;
import static io.trino.parquet.reader.flat.IntColumnAdapter.INT_ADAPTER;
import static io.trino.parquet.reader.flat.LongColumnAdapter.LONG_ADAPTER;
import static io.trino.parquet.reader.flat.ShortColumnAdapter.SHORT_ADAPTER;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.String.format;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.NANOS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;

public final class ColumnReaderFactory
{
    private static final int MAX_INT_DIGITS = 10;

    private ColumnReaderFactory() {}

    public static ColumnReader create(PrimitiveField field, DateTimeZone timeZone, AggregatedMemoryContext aggregatedMemoryContext, boolean useBatchedColumnReaders)
    {
        Type type = field.getType();
        PrimitiveTypeName primitiveType = field.getDescriptor().getPrimitiveType().getPrimitiveTypeName();
        LogicalTypeAnnotation annotation = field.getDescriptor().getPrimitiveType().getLogicalTypeAnnotation();
        LocalMemoryContext memoryContext = aggregatedMemoryContext.newLocalMemoryContext(ColumnReader.class.getSimpleName());
        if (useBatchedColumnReaders && field.getDescriptor().getPath().length == 1) {
            if (BOOLEAN.equals(type) && primitiveType == PrimitiveTypeName.BOOLEAN) {
                return new FlatColumnReader<>(field, ValueDecoders::getBooleanDecoder, BOOLEAN_ADAPTER, memoryContext);
            }
            if (TINYINT.equals(type) && isIntegerOrDecimalPrimitive(primitiveType)) {
                if (isZeroScaleShortDecimalAnnotation(annotation)) {
                    return new FlatColumnReader<>(field, TransformingValueDecoders::getShortDecimalToByteDecoder, BYTE_ADAPTER, memoryContext);
                }
                if (!isIntegerAnnotationAndPrimitive(annotation, primitiveType)) {
                    throw unsupportedException(type, field);
                }
                return new FlatColumnReader<>(field, ValueDecoders::getByteDecoder, BYTE_ADAPTER, memoryContext);
            }
            if (SMALLINT.equals(type) && isIntegerOrDecimalPrimitive(primitiveType)) {
                if (isZeroScaleShortDecimalAnnotation(annotation)) {
                    return new FlatColumnReader<>(field, TransformingValueDecoders::getShortDecimalToShortDecoder, SHORT_ADAPTER, memoryContext);
                }
                if (!isIntegerAnnotationAndPrimitive(annotation, primitiveType)) {
                    throw unsupportedException(type, field);
                }
                return new FlatColumnReader<>(field, ValueDecoders::getShortDecoder, SHORT_ADAPTER, memoryContext);
            }
            if (DATE.equals(type) && primitiveType == INT32) {
                if (annotation == null || annotation instanceof DateLogicalTypeAnnotation) {
                    return new FlatColumnReader<>(field, ValueDecoders::getIntDecoder, INT_ADAPTER, memoryContext);
                }
                throw unsupportedException(type, field);
            }
            if (type instanceof AbstractIntType && isIntegerOrDecimalPrimitive(primitiveType)) {
                if (isZeroScaleShortDecimalAnnotation(annotation)) {
                    return new FlatColumnReader<>(field, TransformingValueDecoders::getShortDecimalToIntDecoder, INT_ADAPTER, memoryContext);
                }
                if (!isIntegerAnnotationAndPrimitive(annotation, primitiveType)) {
                    throw unsupportedException(type, field);
                }
                return new FlatColumnReader<>(field, ValueDecoders::getIntDecoder, INT_ADAPTER, memoryContext);
            }
            if (type instanceof TimeType && primitiveType == INT64) {
                if (annotation instanceof TimeLogicalTypeAnnotation timeAnnotation && timeAnnotation.getUnit() == MICROS) {
                    return new FlatColumnReader<>(field, TransformingValueDecoders::getTimeMicrosDecoder, LONG_ADAPTER, memoryContext);
                }
                throw unsupportedException(type, field);
            }
            if (BIGINT.equals(type) && primitiveType == INT64 && annotation instanceof TimestampLogicalTypeAnnotation) {
                return new FlatColumnReader<>(field, ValueDecoders::getLongDecoder, LONG_ADAPTER, memoryContext);
            }
            if (type instanceof AbstractLongType && isIntegerOrDecimalPrimitive(primitiveType)) {
                if (isZeroScaleShortDecimalAnnotation(annotation)) {
                    return new FlatColumnReader<>(field, ValueDecoders::getShortDecimalDecoder, LONG_ADAPTER, memoryContext);
                }
                if (!isIntegerAnnotationAndPrimitive(annotation, primitiveType)) {
                    throw unsupportedException(type, field);
                }
                if (primitiveType == INT32) {
                    return new FlatColumnReader<>(field, TransformingValueDecoders::getInt32ToLongDecoder, LONG_ADAPTER, memoryContext);
                }
                if (primitiveType == INT64) {
                    return new FlatColumnReader<>(field, ValueDecoders::getLongDecoder, LONG_ADAPTER, memoryContext);
                }
            }
            if (REAL.equals(type) && primitiveType == FLOAT) {
                return new FlatColumnReader<>(field, ValueDecoders::getRealDecoder, INT_ADAPTER, memoryContext);
            }
            if (DOUBLE.equals(type)) {
                if (primitiveType == PrimitiveTypeName.DOUBLE) {
                    return new FlatColumnReader<>(field, ValueDecoders::getDoubleDecoder, LONG_ADAPTER, memoryContext);
                }
                if (primitiveType == FLOAT) {
                    return new FlatColumnReader<>(field, TransformingValueDecoders::getFloatToDoubleDecoder, LONG_ADAPTER, memoryContext);
                }
            }
            if (type instanceof TimestampType timestampType && primitiveType == INT96) {
                if (timestampType.isShort()) {
                    return new FlatColumnReader<>(
                            field,
                            (encoding, primitiveField) -> getInt96ToShortTimestampDecoder(encoding, primitiveField, timeZone),
                            LONG_ADAPTER,
                            memoryContext);
                }
                return new FlatColumnReader<>(
                        field,
                        (encoding, primitiveField) -> getInt96ToLongTimestampDecoder(encoding, primitiveField, timeZone),
                        INT96_ADAPTER,
                        memoryContext);
            }
            if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType && primitiveType == INT96) {
                if (timestampWithTimeZoneType.isShort()) {
                    return new FlatColumnReader<>(field, TransformingValueDecoders::getInt96ToShortTimestampWithTimeZoneDecoder, LONG_ADAPTER, memoryContext);
                }
                throw unsupportedException(type, field);
            }
            if (type instanceof TimestampType timestampType && primitiveType == INT64) {
                if (!(annotation instanceof TimestampLogicalTypeAnnotation timestampAnnotation)) {
                    throw unsupportedException(type, field);
                }
                if (timestampType.isShort()) {
                    return switch (timestampAnnotation.getUnit()) {
                        case MILLIS -> new FlatColumnReader<>(field, TransformingValueDecoders::getInt64TimestampMillsToShortTimestampDecoder, LONG_ADAPTER, memoryContext);
                        case MICROS -> new FlatColumnReader<>(field, TransformingValueDecoders::getInt64TimestampMicrosToShortTimestampDecoder, LONG_ADAPTER, memoryContext);
                        case NANOS -> new FlatColumnReader<>(field, TransformingValueDecoders::getInt64TimestampNanosToShortTimestampDecoder, LONG_ADAPTER, memoryContext);
                    };
                }
                return switch (timestampAnnotation.getUnit()) {
                    case MILLIS -> new FlatColumnReader<>(field, TransformingValueDecoders::getInt64TimestampMillisToLongTimestampDecoder, INT96_ADAPTER, memoryContext);
                    case MICROS -> new FlatColumnReader<>(field, TransformingValueDecoders::getInt64TimestampMicrosToLongTimestampDecoder, INT96_ADAPTER, memoryContext);
                    case NANOS -> new FlatColumnReader<>(field, TransformingValueDecoders::getInt64TimestampNanosToLongTimestampDecoder, INT96_ADAPTER, memoryContext);
                };
            }
            if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType && primitiveType == INT64) {
                if (!(annotation instanceof TimestampLogicalTypeAnnotation timestampAnnotation)) {
                    throw unsupportedException(type, field);
                }
                if (timestampWithTimeZoneType.isShort()) {
                    return switch (timestampAnnotation.getUnit()) {
                        case MILLIS -> new FlatColumnReader<>(field, TransformingValueDecoders::getInt64TimestampMillsToShortTimestampWithTimeZoneDecoder, LONG_ADAPTER, memoryContext);
                        case MICROS -> new FlatColumnReader<>(field, TransformingValueDecoders::getInt64TimestampMicrosToShortTimestampWithTimeZoneDecoder, LONG_ADAPTER, memoryContext);
                        case NANOS -> throw unsupportedException(type, field);
                    };
                }
                return switch (timestampAnnotation.getUnit()) {
                    case MILLIS, NANOS -> throw unsupportedException(type, field);
                    case MICROS -> new FlatColumnReader<>(field, TransformingValueDecoders::getInt64TimestampMicrosToLongTimestampWithTimeZoneDecoder, INT96_ADAPTER, memoryContext);
                };
            }
            if (type instanceof DecimalType decimalType && decimalType.isShort()
                    && isIntegerOrDecimalPrimitive(primitiveType)) {
                if (decimalType.getScale() == 0 && decimalType.getPrecision() >= MAX_INT_DIGITS
                        && primitiveType == INT32
                        && isIntegerAnnotation(annotation)) {
                    return new FlatColumnReader<>(field, TransformingValueDecoders::getInt32ToLongDecoder, LONG_ADAPTER, memoryContext);
                }
                if (!(annotation instanceof DecimalLogicalTypeAnnotation decimalAnnotation)) {
                    throw unsupportedException(type, field);
                }
                if (isDecimalRescaled(decimalAnnotation, decimalType)) {
                    return new FlatColumnReader<>(field, TransformingValueDecoders::getRescaledShortDecimalDecoder, LONG_ADAPTER, memoryContext);
                }
                return new FlatColumnReader<>(field, ValueDecoders::getShortDecimalDecoder, LONG_ADAPTER, memoryContext);
            }
            if (type instanceof DecimalType decimalType && !decimalType.isShort()
                    && isIntegerOrDecimalPrimitive(primitiveType)) {
                if (!(annotation instanceof DecimalLogicalTypeAnnotation decimalAnnotation)) {
                    throw unsupportedException(type, field);
                }
                if (isDecimalRescaled(decimalAnnotation, decimalType)) {
                    return new FlatColumnReader<>(field, TransformingValueDecoders::getRescaledLongDecimalDecoder, INT128_ADAPTER, memoryContext);
                }
                return new FlatColumnReader<>(field, ValueDecoders::getLongDecimalDecoder, INT128_ADAPTER, memoryContext);
            }
            if (type instanceof VarcharType varcharType && !varcharType.isUnbounded() && primitiveType == BINARY) {
                return new FlatColumnReader<>(field, ValueDecoders::getBoundedVarcharBinaryDecoder, BINARY_ADAPTER, memoryContext);
            }
            if (type instanceof CharType && primitiveType == BINARY) {
                return new FlatColumnReader<>(field, ValueDecoders::getCharBinaryDecoder, BINARY_ADAPTER, memoryContext);
            }
            if (type instanceof AbstractVariableWidthType && primitiveType == BINARY) {
                return new FlatColumnReader<>(field, ValueDecoders::getBinaryDecoder, BINARY_ADAPTER, memoryContext);
            }
            if (UUID.equals(type) && primitiveType == FIXED_LEN_BYTE_ARRAY) {
                // Iceberg 0.11.1 writes UUID as FIXED_LEN_BYTE_ARRAY without logical type annotation (see https://github.com/apache/iceberg/pull/2913)
                // To support such files, we bet on the logical type to be UUID based on the Trino UUID type check.
                if (annotation == null || isLogicalUuid(annotation)) {
                    return new FlatColumnReader<>(field, ValueDecoders::getUuidDecoder, INT128_ADAPTER, memoryContext);
                }
                throw unsupportedException(type, field);
            }
            throw new TrinoException(
                    NOT_SUPPORTED,
                    format("Reading Trino column (%s) from Parquet column (%s) is not supported by optimized parquet reader", type, field.getDescriptor()));
        }

        return switch (primitiveType) {
            case BOOLEAN -> new BooleanColumnReader(field);
            case INT32 -> createDecimalColumnReader(field).orElse(new IntColumnReader(field));
            case INT64 -> {
                if (annotation instanceof TimeLogicalTypeAnnotation timeAnnotation) {
                    if (timeAnnotation.getUnit() == MICROS) {
                        yield new TimeMicrosColumnReader(field);
                    }
                    throw unsupportedException(type, field);
                }
                if (annotation instanceof TimestampLogicalTypeAnnotation timestampAnnotation) {
                    if (timestampAnnotation.getUnit() == MILLIS) {
                        yield new Int64TimestampMillisColumnReader(field);
                    }
                    if (timestampAnnotation.getUnit() == MICROS) {
                        yield new TimestampMicrosColumnReader(field);
                    }
                    if (timestampAnnotation.getUnit() == NANOS) {
                        yield new Int64TimestampNanosColumnReader(field);
                    }
                    throw unsupportedException(type, field);
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
                if (isLogicalUuid(annotation)) {
                    yield new UuidColumnReader(field);
                }
                if (annotation == null) {
                    // Iceberg 0.11.1 writes UUID as FIXED_LEN_BYTE_ARRAY without logical type annotation (see https://github.com/apache/iceberg/pull/2913)
                    // To support such files, we bet on the type to be UUID, which gets verified later, when reading the column data.
                    yield new UuidColumnReader(field);
                }
                throw unsupportedException(type, field);
            }
        };
    }

    private static boolean isLogicalUuid(LogicalTypeAnnotation annotation)
    {
        return Optional.ofNullable(annotation)
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

    private static boolean isDecimalRescaled(DecimalLogicalTypeAnnotation decimalAnnotation, DecimalType trinoType)
    {
        return decimalAnnotation.getPrecision() != trinoType.getPrecision()
                || decimalAnnotation.getScale() != trinoType.getScale();
    }

    private static boolean isIntegerAnnotation(LogicalTypeAnnotation typeAnnotation)
    {
        return typeAnnotation == null || typeAnnotation instanceof IntLogicalTypeAnnotation;
    }

    private static boolean isZeroScaleShortDecimalAnnotation(LogicalTypeAnnotation typeAnnotation)
    {
        return typeAnnotation instanceof DecimalLogicalTypeAnnotation decimalAnnotation
                && decimalAnnotation.getScale() == 0
                && decimalAnnotation.getPrecision() <= Decimals.MAX_SHORT_PRECISION;
    }

    private static boolean isIntegerOrDecimalPrimitive(PrimitiveTypeName primitiveType)
    {
        // Integers may be stored in INT32 or INT64
        // Decimals may be stored as INT32, INT64, BINARY or FIXED_LEN_BYTE_ARRAY
        // Short decimals with zero scale in parquet files may be read as integers in Trino
        return primitiveType == INT32 || primitiveType == INT64 || primitiveType == BINARY || primitiveType == FIXED_LEN_BYTE_ARRAY;
    }

    private static boolean isIntegerAnnotationAndPrimitive(LogicalTypeAnnotation typeAnnotation, PrimitiveTypeName primitiveType)
    {
        return isIntegerAnnotation(typeAnnotation) && (primitiveType == INT32 || primitiveType == INT64);
    }

    private static TrinoException unsupportedException(Type type, PrimitiveField field)
    {
        return new TrinoException(NOT_SUPPORTED, format("Unsupported Trino column type (%s) for Parquet column (%s)", type, field.getDescriptor()));
    }
}
