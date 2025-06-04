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
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.decoders.ValueDecoders;
import io.trino.parquet.reader.flat.ColumnAdapter;
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

import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.reader.decoders.ValueDecoder.ValueDecodersProvider;
import static io.trino.parquet.reader.decoders.ValueDecoder.createLevelsDecoder;
import static io.trino.parquet.reader.flat.BinaryColumnAdapter.BINARY_ADAPTER;
import static io.trino.parquet.reader.flat.ByteColumnAdapter.BYTE_ADAPTER;
import static io.trino.parquet.reader.flat.DictionaryDecoder.DictionaryDecoderProvider;
import static io.trino.parquet.reader.flat.DictionaryDecoder.getDictionaryDecoder;
import static io.trino.parquet.reader.flat.Fixed12ColumnAdapter.FIXED12_ADAPTER;
import static io.trino.parquet.reader.flat.FlatDefinitionLevelDecoder.getFlatDefinitionLevelDecoder;
import static io.trino.parquet.reader.flat.Int128ColumnAdapter.INT128_ADAPTER;
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
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;

public final class ColumnReaderFactory
{
    private static final int PREFERRED_BIT_WIDTH = getVectorBitSize();

    private final DateTimeZone timeZone;
    private final boolean vectorizedDecodingEnabled;

    public ColumnReaderFactory(DateTimeZone timeZone, ParquetReaderOptions readerOptions)
    {
        this.timeZone = requireNonNull(timeZone, "dateTimeZone is null");
        this.vectorizedDecodingEnabled = readerOptions.isVectorizedDecodingEnabled() && isVectorizedDecodingSupported();
    }

    public ColumnReader create(PrimitiveField field, AggregatedMemoryContext aggregatedMemoryContext)
    {
        Type type = field.getType();
        PrimitiveTypeName primitiveType = field.getDescriptor().getPrimitiveType().getPrimitiveTypeName();
        LogicalTypeAnnotation annotation = field.getDescriptor().getPrimitiveType().getLogicalTypeAnnotation();
        LocalMemoryContext memoryContext = aggregatedMemoryContext.newLocalMemoryContext(ColumnReader.class.getSimpleName());
        ValueDecoders valueDecoders = new ValueDecoders(field, vectorizedDecodingEnabled);
        if (BOOLEAN.equals(type) && primitiveType == PrimitiveTypeName.BOOLEAN) {
            return createColumnReader(field, valueDecoders::getBooleanDecoder, BYTE_ADAPTER, memoryContext);
        }
        if (TINYINT.equals(type) && isIntegerOrDecimalPrimitive(primitiveType)) {
            if (isZeroScaleShortDecimalAnnotation(annotation)) {
                return createColumnReader(field, valueDecoders::getShortDecimalToByteDecoder, BYTE_ADAPTER, memoryContext);
            }
            if (!isIntegerAnnotationAndPrimitive(annotation, primitiveType)) {
                throw unsupportedException(type, field);
            }
            return createColumnReader(field, valueDecoders::getByteDecoder, BYTE_ADAPTER, memoryContext);
        }
        if (SMALLINT.equals(type) && isIntegerOrDecimalPrimitive(primitiveType)) {
            if (isZeroScaleShortDecimalAnnotation(annotation)) {
                return createColumnReader(field, valueDecoders::getShortDecimalToShortDecoder, SHORT_ADAPTER, memoryContext);
            }
            if (!isIntegerAnnotationAndPrimitive(annotation, primitiveType)) {
                throw unsupportedException(type, field);
            }
            return createColumnReader(field, valueDecoders::getShortDecoder, SHORT_ADAPTER, memoryContext);
        }
        if (DATE.equals(type) && primitiveType == INT32) {
            if (isIntegerAnnotation(annotation) || annotation instanceof DateLogicalTypeAnnotation) {
                return createColumnReader(field, valueDecoders::getIntDecoder, INT_ADAPTER, memoryContext);
            }
            throw unsupportedException(type, field);
        }
        if (type instanceof AbstractIntType && isIntegerOrDecimalPrimitive(primitiveType)) {
            if (isZeroScaleShortDecimalAnnotation(annotation)) {
                return createColumnReader(field, valueDecoders::getShortDecimalToIntDecoder, INT_ADAPTER, memoryContext);
            }
            if (!isIntegerAnnotationAndPrimitive(annotation, primitiveType)) {
                throw unsupportedException(type, field);
            }
            return createColumnReader(field, valueDecoders::getIntDecoder, INT_ADAPTER, memoryContext);
        }
        if (type instanceof TimeType) {
            if (!(annotation instanceof TimeLogicalTypeAnnotation timeAnnotation)) {
                throw unsupportedException(type, field);
            }
            if (primitiveType == INT64 && timeAnnotation.getUnit() == MICROS) {
                return createColumnReader(field, valueDecoders::getTimeMicrosDecoder, LONG_ADAPTER, memoryContext);
            }
            if (primitiveType == INT32 && timeAnnotation.getUnit() == MILLIS) {
                return createColumnReader(field, valueDecoders::getTimeMillisDecoder, LONG_ADAPTER, memoryContext);
            }
            throw unsupportedException(type, field);
        }
        if (BIGINT.equals(type) && primitiveType == INT64
                && (annotation instanceof TimestampLogicalTypeAnnotation || annotation instanceof TimeLogicalTypeAnnotation)) {
            return createColumnReader(field, valueDecoders::getLongDecoder, LONG_ADAPTER, memoryContext);
        }
        if (type instanceof AbstractLongType && isIntegerOrDecimalPrimitive(primitiveType)) {
            if (isZeroScaleShortDecimalAnnotation(annotation)) {
                return createColumnReader(field, valueDecoders::getShortDecimalDecoder, LONG_ADAPTER, memoryContext);
            }
            if (!isIntegerAnnotationAndPrimitive(annotation, primitiveType)) {
                throw unsupportedException(type, field);
            }
            if (primitiveType == INT32) {
                return createColumnReader(field, valueDecoders::getInt32ToLongDecoder, LONG_ADAPTER, memoryContext);
            }
            if (primitiveType == INT64) {
                return createColumnReader(field, valueDecoders::getLongDecoder, LONG_ADAPTER, memoryContext);
            }
        }
        if (REAL.equals(type) && primitiveType == FLOAT) {
            return createColumnReader(field, valueDecoders::getRealDecoder, INT_ADAPTER, memoryContext);
        }
        if (DOUBLE.equals(type)) {
            if (primitiveType == PrimitiveTypeName.DOUBLE) {
                return createColumnReader(field, valueDecoders::getDoubleDecoder, LONG_ADAPTER, memoryContext);
            }
            if (primitiveType == FLOAT) {
                return createColumnReader(field, valueDecoders::getFloatToDoubleDecoder, LONG_ADAPTER, memoryContext);
            }
        }
        if (type instanceof TimestampType timestampType && primitiveType == INT96) {
            if (timestampType.isShort()) {
                return createColumnReader(
                        field,
                        (encoding) -> valueDecoders.getInt96ToShortTimestampDecoder(encoding, timeZone),
                        LONG_ADAPTER,
                        memoryContext);
            }
            return createColumnReader(
                    field,
                    (encoding) -> valueDecoders.getInt96ToLongTimestampDecoder(encoding, timeZone),
                    FIXED12_ADAPTER,
                    memoryContext);
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType && primitiveType == INT96) {
            if (timestampWithTimeZoneType.isShort()) {
                return createColumnReader(field, valueDecoders::getInt96ToShortTimestampWithTimeZoneDecoder, LONG_ADAPTER, memoryContext);
            }
            return createColumnReader(field, valueDecoders::getInt96ToLongTimestampWithTimeZoneDecoder, FIXED12_ADAPTER, memoryContext);
        }
        if (type instanceof TimestampType timestampType && primitiveType == INT64) {
            if (!(annotation instanceof TimestampLogicalTypeAnnotation timestampAnnotation)) {
                throw unsupportedException(type, field);
            }
            DateTimeZone readTimeZone = timestampAnnotation.isAdjustedToUTC() ? timeZone : DateTimeZone.UTC;
            if (timestampType.isShort()) {
                return switch (timestampAnnotation.getUnit()) {
                    case MILLIS -> createColumnReader(field, encoding -> valueDecoders.getInt64TimestampMillisToShortTimestampDecoder(encoding, readTimeZone), LONG_ADAPTER, memoryContext);
                    case MICROS -> createColumnReader(field, encoding -> valueDecoders.getInt64TimestampMicrosToShortTimestampDecoder(encoding, readTimeZone), LONG_ADAPTER, memoryContext);
                    case NANOS -> createColumnReader(field, encoding -> valueDecoders.getInt64TimestampNanosToShortTimestampDecoder(encoding, readTimeZone), LONG_ADAPTER, memoryContext);
                };
            }
            return switch (timestampAnnotation.getUnit()) {
                case MILLIS -> createColumnReader(field, encoding -> valueDecoders.getInt64TimestampMillisToLongTimestampDecoder(encoding, readTimeZone), FIXED12_ADAPTER, memoryContext);
                case MICROS -> createColumnReader(field, encoding -> valueDecoders.getInt64TimestampMicrosToLongTimestampDecoder(encoding, readTimeZone), FIXED12_ADAPTER, memoryContext);
                case NANOS -> createColumnReader(field, encoding -> valueDecoders.getInt64TimestampNanosToLongTimestampDecoder(encoding, readTimeZone), FIXED12_ADAPTER, memoryContext);
            };
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType && primitiveType == INT64) {
            if (!(annotation instanceof TimestampLogicalTypeAnnotation timestampAnnotation)) {
                throw unsupportedException(type, field);
            }
            if (timestampWithTimeZoneType.isShort()) {
                return switch (timestampAnnotation.getUnit()) {
                    case MILLIS -> createColumnReader(field, valueDecoders::getInt64TimestampMillsToShortTimestampWithTimeZoneDecoder, LONG_ADAPTER, memoryContext);
                    case MICROS -> createColumnReader(field, valueDecoders::getInt64TimestampMicrosToShortTimestampWithTimeZoneDecoder, LONG_ADAPTER, memoryContext);
                    case NANOS -> throw unsupportedException(type, field);
                };
            }
            return switch (timestampAnnotation.getUnit()) {
                case MILLIS, NANOS -> throw unsupportedException(type, field);
                case MICROS -> createColumnReader(field, valueDecoders::getInt64TimestampMicrosToLongTimestampWithTimeZoneDecoder, FIXED12_ADAPTER, memoryContext);
            };
        }
        if (type instanceof DecimalType decimalType && decimalType.isShort()
                && isIntegerOrDecimalPrimitive(primitiveType)) {
            if (primitiveType == INT32 && isIntegerAnnotation(annotation)) {
                return createColumnReader(field, valueDecoders::getInt32ToShortDecimalDecoder, LONG_ADAPTER, memoryContext);
            }
            if (!(annotation instanceof DecimalLogicalTypeAnnotation decimalAnnotation)) {
                throw unsupportedException(type, field);
            }
            if (isDecimalRescaled(decimalAnnotation, decimalType)) {
                return createColumnReader(field, valueDecoders::getRescaledShortDecimalDecoder, LONG_ADAPTER, memoryContext);
            }
            return createColumnReader(field, valueDecoders::getShortDecimalDecoder, LONG_ADAPTER, memoryContext);
        }
        if (type instanceof DecimalType decimalType && !decimalType.isShort()
                && isIntegerOrDecimalPrimitive(primitiveType)) {
            if (!(annotation instanceof DecimalLogicalTypeAnnotation decimalAnnotation)) {
                throw unsupportedException(type, field);
            }
            if (isDecimalRescaled(decimalAnnotation, decimalType)) {
                return createColumnReader(field, valueDecoders::getRescaledLongDecimalDecoder, INT128_ADAPTER, memoryContext);
            }
            return createColumnReader(field, valueDecoders::getLongDecimalDecoder, INT128_ADAPTER, memoryContext);
        }
        if (type instanceof VarcharType varcharType && !varcharType.isUnbounded() && primitiveType == BINARY) {
            return createColumnReader(field, valueDecoders::getBoundedVarcharBinaryDecoder, BINARY_ADAPTER, memoryContext);
        }
        if (type instanceof CharType && primitiveType == BINARY) {
            return createColumnReader(field, valueDecoders::getCharBinaryDecoder, BINARY_ADAPTER, memoryContext);
        }
        if (type instanceof AbstractVariableWidthType && primitiveType == BINARY) {
            return createColumnReader(field, valueDecoders::getBinaryDecoder, BINARY_ADAPTER, memoryContext);
        }
        if ((VARBINARY.equals(type) || VARCHAR.equals(type)) && primitiveType == FIXED_LEN_BYTE_ARRAY) {
            if (annotation instanceof DecimalLogicalTypeAnnotation) {
                throw unsupportedException(type, field);
            }
            return createColumnReader(field, valueDecoders::getFixedWidthBinaryDecoder, BINARY_ADAPTER, memoryContext);
        }
        if (UUID.equals(type) && primitiveType == FIXED_LEN_BYTE_ARRAY) {
            // Iceberg 0.11.1 writes UUID as FIXED_LEN_BYTE_ARRAY without logical type annotation (see https://github.com/apache/iceberg/pull/2913)
            // To support such files, we bet on the logical type to be UUID based on the Trino UUID type check.
            if (annotation == null || isLogicalUuid(annotation)) {
                return createColumnReader(field, valueDecoders::getUuidDecoder, INT128_ADAPTER, memoryContext);
            }
        }
        throw unsupportedException(type, field);
    }

    private <T> ColumnReader createColumnReader(
            PrimitiveField field,
            ValueDecodersProvider<T> decodersProvider,
            ColumnAdapter<T> columnAdapter,
            LocalMemoryContext memoryContext)
    {
        DictionaryDecoderProvider<T> dictionaryDecoderProvider = (dictionaryPage, isNonNull) -> getDictionaryDecoder(
                dictionaryPage,
                columnAdapter,
                decodersProvider.create(PLAIN),
                isNonNull,
                vectorizedDecodingEnabled);
        if (isFlatColumn(field)) {
            return new FlatColumnReader<>(
                    field,
                    decodersProvider,
                    maxDefinitionLevel -> getFlatDefinitionLevelDecoder(maxDefinitionLevel, vectorizedDecodingEnabled),
                    dictionaryDecoderProvider,
                    columnAdapter,
                    memoryContext);
        }
        return new NestedColumnReader<>(
                field,
                decodersProvider,
                maxLevel -> createLevelsDecoder(maxLevel, vectorizedDecodingEnabled),
                dictionaryDecoderProvider,
                columnAdapter,
                memoryContext);
    }

    private static boolean isFlatColumn(PrimitiveField field)
    {
        return field.getDescriptor().getPath().length == 1 && field.getRepetitionLevel() == 0;
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

    public static boolean isIntegerAnnotationAndPrimitive(LogicalTypeAnnotation typeAnnotation, PrimitiveTypeName primitiveType)
    {
        return isIntegerAnnotation(typeAnnotation) && (primitiveType == INT32 || primitiveType == INT64);
    }

    private static TrinoException unsupportedException(Type type, PrimitiveField field)
    {
        return new TrinoException(NOT_SUPPORTED, format("Unsupported Trino column type (%s) for Parquet column (%s)", type, field.getDescriptor()));
    }

    private static boolean isVectorizedDecodingSupported()
    {
        // Performance gains with vectorized decoding are validated only when the hardware platform provides at least 256 bit width registers
        // Graviton 2 machines return false here, whereas x86 and Graviton 3 machines return true
        return PREFERRED_BIT_WIDTH >= 256;
    }

    // get VectorShape bit size via reflection to avoid requiring the preview feature is enabled
    private static int getVectorBitSize()
    {
        try {
            Class<?> clazz = Class.forName("jdk.incubator.vector.VectorShape");
            return (int) clazz.getMethod("vectorBitSize").invoke(clazz.getMethod("preferredShape").invoke(null));
        }
        catch (Throwable e) {
            return -1;
        }
    }
}
