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
package io.trino.parquet.reader.decoders;

import io.airlift.slice.Slice;
import io.trino.parquet.ParquetEncoding;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.SimpleSliceInputStream;
import io.trino.parquet.reader.flat.BinaryBuffer;
import io.trino.spi.TrinoException;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalConversions;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.joda.time.DateTimeZone;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.ParquetEncoding.DELTA_BYTE_ARRAY;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetReaderUtils.toByteExact;
import static io.trino.parquet.ParquetReaderUtils.toShortExact;
import static io.trino.parquet.ParquetTypeUtils.checkBytesFitInShortDecimal;
import static io.trino.parquet.ParquetTypeUtils.getShortDecimalValue;
import static io.trino.parquet.ValuesType.VALUES;
import static io.trino.parquet.reader.decoders.ApacheParquetValueDecoders.BooleanApacheParquetValueDecoder;
import static io.trino.parquet.reader.decoders.DeltaBinaryPackedDecoders.DeltaBinaryPackedByteDecoder;
import static io.trino.parquet.reader.decoders.DeltaBinaryPackedDecoders.DeltaBinaryPackedIntDecoder;
import static io.trino.parquet.reader.decoders.DeltaBinaryPackedDecoders.DeltaBinaryPackedLongDecoder;
import static io.trino.parquet.reader.decoders.DeltaBinaryPackedDecoders.DeltaBinaryPackedShortDecoder;
import static io.trino.parquet.reader.decoders.DeltaByteArrayDecoders.BinaryDeltaByteArrayDecoder;
import static io.trino.parquet.reader.decoders.DeltaByteArrayDecoders.BoundedVarcharDeltaByteArrayDecoder;
import static io.trino.parquet.reader.decoders.DeltaByteArrayDecoders.CharDeltaByteArrayDecoder;
import static io.trino.parquet.reader.decoders.DeltaLengthByteArrayDecoders.BinaryDeltaLengthDecoder;
import static io.trino.parquet.reader.decoders.DeltaLengthByteArrayDecoders.BoundedVarcharDeltaLengthDecoder;
import static io.trino.parquet.reader.decoders.DeltaLengthByteArrayDecoders.CharDeltaLengthDecoder;
import static io.trino.parquet.reader.decoders.PlainByteArrayDecoders.BinaryPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainByteArrayDecoders.BoundedVarcharPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainByteArrayDecoders.CharPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.BooleanPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.FixedLengthPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.Int96TimestampPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.IntPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.IntToBytePlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.IntToShortPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.LongDecimalPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.LongPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.ShortDecimalFixedLengthByteArrayDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.UuidPlainValueDecoder;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.block.Fixed12Block.decodeFixed12First;
import static io.trino.spi.block.Fixed12Block.decodeFixed12Second;
import static io.trino.spi.block.Fixed12Block.encodeFixed12;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.Decimals.longTenToNth;
import static io.trino.spi.type.Decimals.overflows;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;

/**
 * This class provides API for creating value decoders for given fields and encodings.
 * <p>
 * This class is to replace most of the logic contained in ParquetEncoding enum
 */
public final class ValueDecoders
{
    private final PrimitiveField field;

    public ValueDecoders(PrimitiveField field)
    {
        this.field = requireNonNull(field, "field is null");
    }

    public ValueDecoder<long[]> getDoubleDecoder(ParquetEncoding encoding)
    {
        if (PLAIN.equals(encoding)) {
            return new LongPlainValueDecoder();
        }
        throw wrongEncoding(encoding);
    }

    public ValueDecoder<int[]> getRealDecoder(ParquetEncoding encoding)
    {
        if (PLAIN.equals(encoding)) {
            return new IntPlainValueDecoder();
        }
        throw wrongEncoding(encoding);
    }

    public ValueDecoder<long[]> getShortDecimalDecoder(ParquetEncoding encoding)
    {
        PrimitiveType primitiveType = field.getDescriptor().getPrimitiveType();
        checkArgument(
                primitiveType.getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation,
                "Column %s is not annotated as a decimal",
                field);
        return switch (primitiveType.getPrimitiveTypeName()) {
            case INT64 -> getLongDecoder(encoding);
            case INT32 -> getInt32ToLongDecoder(encoding);
            case FIXED_LEN_BYTE_ARRAY -> getFixedWidthShortDecimalDecoder(encoding);
            case BINARY -> getBinaryShortDecimalDecoder(encoding);
            default -> throw wrongEncoding(encoding);
        };
    }

    public ValueDecoder<long[]> getLongDecimalDecoder(ParquetEncoding encoding)
    {
        return switch (field.getDescriptor().getPrimitiveType().getPrimitiveTypeName()) {
            case FIXED_LEN_BYTE_ARRAY -> getFixedWidthLongDecimalDecoder(encoding);
            case BINARY -> getBinaryLongDecimalDecoder(encoding);
            default -> throw wrongEncoding(encoding);
        };
    }

    public ValueDecoder<long[]> getUuidDecoder(ParquetEncoding encoding)
    {
        return switch (encoding) {
            case PLAIN -> new UuidPlainValueDecoder();
            case DELTA_BYTE_ARRAY -> getDeltaUuidDecoder(encoding);
            default -> throw wrongEncoding(encoding);
        };
    }

    public ValueDecoder<long[]> getLongDecoder(ParquetEncoding encoding)
    {
        return switch (encoding) {
            case PLAIN -> new LongPlainValueDecoder();
            case DELTA_BINARY_PACKED -> new DeltaBinaryPackedLongDecoder();
            default -> throw wrongEncoding(encoding);
        };
    }

    public ValueDecoder<int[]> getIntDecoder(ParquetEncoding encoding)
    {
        return switch (field.getDescriptor().getPrimitiveType().getPrimitiveTypeName()) {
            case INT64 -> getInt64ToIntDecoder(encoding);
            case INT32 -> getInt32Decoder(encoding);
            default -> throw wrongEncoding(encoding);
        };
    }

    public ValueDecoder<short[]> getShortDecoder(ParquetEncoding encoding)
    {
        return switch (field.getDescriptor().getPrimitiveType().getPrimitiveTypeName()) {
            case INT64 -> getInt64ToShortDecoder(encoding);
            case INT32 -> getInt32ToShortDecoder(encoding);
            default -> throw wrongEncoding(encoding);
        };
    }

    public ValueDecoder<byte[]> getByteDecoder(ParquetEncoding encoding)
    {
        return switch (field.getDescriptor().getPrimitiveType().getPrimitiveTypeName()) {
            case INT64 -> getInt64ToByteDecoder(encoding);
            case INT32 -> getInt32ToByteDecoder(encoding);
            default -> throw wrongEncoding(encoding);
        };
    }

    public ValueDecoder<byte[]> getBooleanDecoder(ParquetEncoding encoding)
    {
        return switch (encoding) {
            case PLAIN -> new BooleanPlainValueDecoder();
            case RLE -> new RleBitPackingHybridBooleanDecoder();
            // BIT_PACKED is a deprecated encoding which should not be used anymore as per
            // https://github.com/apache/parquet-format/blob/master/Encodings.md#bit-packed-deprecated-bit_packed--4
            // An unoptimized decoder for this encoding is provided here for compatibility with old files or non-compliant writers
            case BIT_PACKED -> new BooleanApacheParquetValueDecoder(getApacheParquetReader(encoding));
            default -> throw wrongEncoding(encoding);
        };
    }

    public ValueDecoder<int[]> getInt96TimestampDecoder(ParquetEncoding encoding)
    {
        if (PLAIN.equals(encoding)) {
            // INT96 type has been deprecated as per https://github.com/apache/parquet-format/blob/master/Encodings.md#plain-plain--0
            // However, this encoding is still commonly encountered in parquet files.
            return new Int96TimestampPlainValueDecoder();
        }
        throw wrongEncoding(encoding);
    }

    public ValueDecoder<long[]> getFixedWidthShortDecimalDecoder(ParquetEncoding encoding)
    {
        return switch (encoding) {
            case PLAIN -> new ShortDecimalFixedLengthByteArrayDecoder(field.getDescriptor());
            case DELTA_BYTE_ARRAY -> getDeltaFixedWidthShortDecimalDecoder(encoding);
            default -> throw wrongEncoding(encoding);
        };
    }

    public ValueDecoder<long[]> getFixedWidthLongDecimalDecoder(ParquetEncoding encoding)
    {
        return switch (encoding) {
            case PLAIN -> new LongDecimalPlainValueDecoder(field.getDescriptor().getPrimitiveType().getTypeLength());
            case DELTA_BYTE_ARRAY -> getDeltaFixedWidthLongDecimalDecoder(encoding);
            default -> throw wrongEncoding(encoding);
        };
    }

    public ValueDecoder<BinaryBuffer> getFixedWidthBinaryDecoder(ParquetEncoding encoding)
    {
        return switch (encoding) {
            case PLAIN -> new FixedLengthPlainValueDecoder(field.getDescriptor().getPrimitiveType().getTypeLength());
            case DELTA_BYTE_ARRAY -> new BinaryDeltaByteArrayDecoder();
            default -> throw wrongEncoding(encoding);
        };
    }

    public ValueDecoder<BinaryBuffer> getBoundedVarcharBinaryDecoder(ParquetEncoding encoding)
    {
        Type trinoType = field.getType();
        checkArgument(
                trinoType instanceof VarcharType varcharType && !varcharType.isUnbounded(),
                "Trino type %s is not a bounded varchar",
                trinoType);
        return switch (encoding) {
            case PLAIN -> new BoundedVarcharPlainValueDecoder((VarcharType) trinoType);
            case DELTA_LENGTH_BYTE_ARRAY -> new BoundedVarcharDeltaLengthDecoder((VarcharType) trinoType);
            case DELTA_BYTE_ARRAY -> new BoundedVarcharDeltaByteArrayDecoder((VarcharType) trinoType);
            default -> throw wrongEncoding(encoding);
        };
    }

    public ValueDecoder<BinaryBuffer> getCharBinaryDecoder(ParquetEncoding encoding)
    {
        Type trinoType = field.getType();
        checkArgument(
                trinoType instanceof CharType,
                "Trino type %s is not a char",
                trinoType);
        return switch (encoding) {
            case PLAIN -> new CharPlainValueDecoder((CharType) trinoType);
            case DELTA_LENGTH_BYTE_ARRAY -> new CharDeltaLengthDecoder((CharType) trinoType);
            case DELTA_BYTE_ARRAY -> new CharDeltaByteArrayDecoder((CharType) trinoType);
            default -> throw wrongEncoding(encoding);
        };
    }

    public ValueDecoder<BinaryBuffer> getBinaryDecoder(ParquetEncoding encoding)
    {
        return switch (encoding) {
            case PLAIN -> new BinaryPlainValueDecoder();
            case DELTA_LENGTH_BYTE_ARRAY -> new BinaryDeltaLengthDecoder();
            case DELTA_BYTE_ARRAY -> new BinaryDeltaByteArrayDecoder();
            default -> throw wrongEncoding(encoding);
        };
    }

    public ValueDecoder<int[]> getInt32Decoder(ParquetEncoding encoding)
    {
        return switch (encoding) {
            case PLAIN -> new IntPlainValueDecoder();
            case DELTA_BINARY_PACKED -> new DeltaBinaryPackedIntDecoder();
            default -> throw wrongEncoding(encoding);
        };
    }

    private ValueDecoder<short[]> getInt32ToShortDecoder(ParquetEncoding encoding)
    {
        return switch (encoding) {
            case PLAIN -> new IntToShortPlainValueDecoder();
            case DELTA_BINARY_PACKED -> new DeltaBinaryPackedShortDecoder();
            default -> throw wrongEncoding(encoding);
        };
    }

    private ValueDecoder<byte[]> getInt32ToByteDecoder(ParquetEncoding encoding)
    {
        return switch (encoding) {
            case PLAIN -> new IntToBytePlainValueDecoder();
            case DELTA_BINARY_PACKED -> new DeltaBinaryPackedByteDecoder();
            default -> throw wrongEncoding(encoding);
        };
    }

    public ValueDecoder<long[]> getTimeMicrosDecoder(ParquetEncoding encoding)
    {
        return new InlineTransformDecoder<>(
                getLongDecoder(encoding),
                (values, offset, length) -> {
                    for (int i = offset; i < offset + length; i++) {
                        values[i] = values[i] * PICOSECONDS_PER_MICROSECOND;
                    }
                });
    }

    public ValueDecoder<long[]> getTimeMillisDecoder(ParquetEncoding encoding)
    {
        int precision = ((TimeType) field.getType()).getPrecision();
        if (precision < 3) {
            return new InlineTransformDecoder<>(
                    getInt32ToLongDecoder(encoding),
                    (values, offset, length) -> {
                        // decoded values are millis, round to lower precision and convert to picos
                        // modulo PICOSECONDS_PER_DAY is applied for the case when a value is rounded up to PICOSECONDS_PER_DAY
                        for (int i = offset; i < offset + length; i++) {
                            values[i] = (round(values[i], 3 - precision) * PICOSECONDS_PER_MILLISECOND) % PICOSECONDS_PER_DAY;
                        }
                    });
        }
        return new InlineTransformDecoder<>(
                getInt32ToLongDecoder(encoding),
                (values, offset, length) -> {
                    for (int i = offset; i < offset + length; i++) {
                        values[i] = values[i] * PICOSECONDS_PER_MILLISECOND;
                    }
                });
    }

    public ValueDecoder<long[]> getInt96ToShortTimestampDecoder(ParquetEncoding encoding, DateTimeZone timeZone)
    {
        checkArgument(
                field.getType() instanceof TimestampType timestampType && timestampType.isShort(),
                "Trino type %s is not a short timestamp",
                field.getType());
        int precision = ((TimestampType) field.getType()).getPrecision();
        ValueDecoder<int[]> delegate = getInt96TimestampDecoder(encoding);
        return new ValueDecoder<>()
        {
            @Override
            public void init(SimpleSliceInputStream input)
            {
                delegate.init(input);
            }

            @Override
            public void read(long[] values, int offset, int length)
            {
                int[] int96Buffer = new int[length * 3];
                delegate.read(int96Buffer, 0, length);
                for (int i = 0; i < length; i++) {
                    long epochSeconds = decodeFixed12First(int96Buffer, i);
                    long epochMicros;
                    if (timeZone == DateTimeZone.UTC) {
                        epochMicros = epochSeconds * MICROSECONDS_PER_SECOND;
                    }
                    else {
                        epochMicros = timeZone.convertUTCToLocal(epochSeconds * MILLISECONDS_PER_SECOND) * MICROSECONDS_PER_MILLISECOND;
                    }
                    int nanosOfSecond = (int) round(decodeFixed12Second(int96Buffer, i), 9 - precision);
                    values[offset + i] = epochMicros + nanosOfSecond / NANOSECONDS_PER_MICROSECOND;
                }
            }

            @Override
            public void skip(int n)
            {
                delegate.skip(n);
            }
        };
    }

    public ValueDecoder<int[]> getInt96ToLongTimestampDecoder(ParquetEncoding encoding, DateTimeZone timeZone)
    {
        checkArgument(
                field.getType() instanceof TimestampType timestampType && !timestampType.isShort(),
                "Trino type %s is not a long timestamp",
                field.getType());
        int precision = ((TimestampType) field.getType()).getPrecision();
        return new InlineTransformDecoder<>(
                getInt96TimestampDecoder(encoding),
                (values, offset, length) -> {
                    for (int i = offset; i < offset + length; i++) {
                        long epochSeconds = decodeFixed12First(values, i);
                        long nanosOfSecond = decodeFixed12Second(values, i);
                        if (timeZone != DateTimeZone.UTC) {
                            epochSeconds = timeZone.convertUTCToLocal(epochSeconds * MILLISECONDS_PER_SECOND) / MILLISECONDS_PER_SECOND;
                        }
                        if (precision < 9) {
                            nanosOfSecond = (int) round(nanosOfSecond, 9 - precision);
                        }
                        // epochMicros
                        encodeFixed12(
                                epochSeconds * MICROSECONDS_PER_SECOND + (nanosOfSecond / NANOSECONDS_PER_MICROSECOND),
                                (int) ((nanosOfSecond * PICOSECONDS_PER_NANOSECOND) % PICOSECONDS_PER_MICROSECOND),
                                values,
                                i);
                    }
                });
    }

    public ValueDecoder<long[]> getInt96ToShortTimestampWithTimeZoneDecoder(ParquetEncoding encoding)
    {
        checkArgument(
                field.getType() instanceof TimestampWithTimeZoneType timestampWithTimeZoneType && timestampWithTimeZoneType.isShort(),
                "Trino type %s is not a short timestamp with timezone",
                field.getType());
        ValueDecoder<int[]> delegate = getInt96TimestampDecoder(encoding);
        return new ValueDecoder<>()
        {
            @Override
            public void init(SimpleSliceInputStream input)
            {
                delegate.init(input);
            }

            @Override
            public void read(long[] values, int offset, int length)
            {
                int[] int96Buffer = new int[length * 3];
                delegate.read(int96Buffer, 0, length);
                for (int i = 0; i < length; i++) {
                    long epochSeconds = decodeFixed12First(int96Buffer, i);
                    int nanosOfSecond = decodeFixed12Second(int96Buffer, i);
                    long utcMillis = epochSeconds * MILLISECONDS_PER_SECOND + (nanosOfSecond / NANOSECONDS_PER_MILLISECOND);
                    values[offset + i] = packDateTimeWithZone(utcMillis, UTC_KEY);
                }
            }

            @Override
            public void skip(int n)
            {
                delegate.skip(n);
            }
        };
    }

    public ValueDecoder<long[]> getInt64TimestampMillsToShortTimestampDecoder(ParquetEncoding encoding)
    {
        checkArgument(
                field.getType() instanceof TimestampType timestampType && timestampType.isShort(),
                "Trino type %s is not a short timestamp",
                field.getType());
        int precision = ((TimestampType) field.getType()).getPrecision();
        ValueDecoder<long[]> valueDecoder = getLongDecoder(encoding);
        if (precision < 3) {
            return new InlineTransformDecoder<>(
                    valueDecoder,
                    (values, offset, length) -> {
                        // decoded values are epochMillis, round to lower precision and convert to epochMicros
                        for (int i = offset; i < offset + length; i++) {
                            values[i] = round(values[i], 3 - precision) * MICROSECONDS_PER_MILLISECOND;
                        }
                    });
        }
        return new InlineTransformDecoder<>(
                valueDecoder,
                (values, offset, length) -> {
                    // decoded values are epochMillis, convert to epochMicros
                    for (int i = offset; i < offset + length; i++) {
                        values[i] = values[i] * MICROSECONDS_PER_MILLISECOND;
                    }
                });
    }

    public ValueDecoder<long[]> getInt64TimestampMillsToShortTimestampWithTimeZoneDecoder(ParquetEncoding encoding)
    {
        checkArgument(
                field.getType() instanceof TimestampWithTimeZoneType timestampWithTimeZoneType && timestampWithTimeZoneType.isShort(),
                "Trino type %s is not a short timestamp",
                field.getType());
        int precision = ((TimestampWithTimeZoneType) field.getType()).getPrecision();
        ValueDecoder<long[]> valueDecoder = getLongDecoder(encoding);
        if (precision < 3) {
            return new InlineTransformDecoder<>(
                    valueDecoder,
                    (values, offset, length) -> {
                        // decoded values are epochMillis, round to lower precision and convert to packed millis utc value
                        for (int i = offset; i < offset + length; i++) {
                            values[i] = packDateTimeWithZone(round(values[i], 3 - precision), UTC_KEY);
                        }
                    });
        }
        return new InlineTransformDecoder<>(
                valueDecoder,
                (values, offset, length) -> {
                    // decoded values are epochMillis, convert to packed millis utc value
                    for (int i = offset; i < offset + length; i++) {
                        values[i] = packDateTimeWithZone(values[i], UTC_KEY);
                    }
                });
    }

    public ValueDecoder<long[]> getInt64TimestampMicrosToShortTimestampDecoder(ParquetEncoding encoding)
    {
        checkArgument(
                field.getType() instanceof TimestampType timestampType && timestampType.isShort(),
                "Trino type %s is not a short timestamp",
                field.getType());
        int precision = ((TimestampType) field.getType()).getPrecision();
        ValueDecoder<long[]> valueDecoder = getLongDecoder(encoding);
        if (precision == 6) {
            return valueDecoder;
        }
        return new InlineTransformDecoder<>(
                valueDecoder,
                (values, offset, length) -> {
                    // decoded values are epochMicros, round to lower precision
                    for (int i = offset; i < offset + length; i++) {
                        values[i] = round(values[i], 6 - precision);
                    }
                });
    }

    public ValueDecoder<long[]> getInt64TimestampMicrosToShortTimestampWithTimeZoneDecoder(ParquetEncoding encoding)
    {
        checkArgument(
                field.getType() instanceof TimestampWithTimeZoneType timestampWithTimeZoneType && timestampWithTimeZoneType.isShort(),
                "Trino type %s is not a short timestamp",
                field.getType());
        int precision = ((TimestampWithTimeZoneType) field.getType()).getPrecision();
        return new InlineTransformDecoder<>(
                getLongDecoder(encoding),
                (values, offset, length) -> {
                    // decoded values are epochMicros, round to lower precision and convert to packed millis utc value
                    for (int i = offset; i < offset + length; i++) {
                        values[i] = packDateTimeWithZone(round(values[i], 6 - precision) / MICROSECONDS_PER_MILLISECOND, UTC_KEY);
                    }
                });
    }

    public ValueDecoder<long[]> getInt64TimestampNanosToShortTimestampDecoder(ParquetEncoding encoding)
    {
        checkArgument(
                field.getType() instanceof TimestampType timestampType && timestampType.isShort(),
                "Trino type %s is not a short timestamp",
                field.getType());
        int precision = ((TimestampType) field.getType()).getPrecision();
        return new InlineTransformDecoder<>(
                getLongDecoder(encoding),
                (values, offset, length) -> {
                    // decoded values are epochNanos, round to lower precision and convert to epochMicros
                    for (int i = offset; i < offset + length; i++) {
                        values[i] = round(values[i], 9 - precision) / NANOSECONDS_PER_MICROSECOND;
                    }
                });
    }

    public ValueDecoder<int[]> getInt64TimestampMillisToLongTimestampDecoder(ParquetEncoding encoding)
    {
        ValueDecoder<long[]> delegate = getLongDecoder(encoding);
        return new ValueDecoder<>()
        {
            @Override
            public void init(SimpleSliceInputStream input)
            {
                delegate.init(input);
            }

            @Override
            public void read(int[] values, int offset, int length)
            {
                long[] buffer = new long[length];
                delegate.read(buffer, 0, length);
                // decoded values are epochMillis, convert to epochMicros
                for (int i = 0; i < length; i++) {
                    encodeFixed12(buffer[i] * MICROSECONDS_PER_MILLISECOND, 0, values, i + offset);
                }
            }

            @Override
            public void skip(int n)
            {
                delegate.skip(n);
            }
        };
    }

    public ValueDecoder<int[]> getInt64TimestampMicrosToLongTimestampDecoder(ParquetEncoding encoding)
    {
        ValueDecoder<long[]> delegate = getLongDecoder(encoding);
        return new ValueDecoder<>()
        {
            @Override
            public void init(SimpleSliceInputStream input)
            {
                delegate.init(input);
            }

            @Override
            public void read(int[] values, int offset, int length)
            {
                long[] buffer = new long[length];
                delegate.read(buffer, 0, length);
                // decoded values are epochMicros
                for (int i = 0; i < length; i++) {
                    encodeFixed12(buffer[i], 0, values, i + offset);
                }
            }

            @Override
            public void skip(int n)
            {
                delegate.skip(n);
            }
        };
    }

    public ValueDecoder<int[]> getInt64TimestampMicrosToLongTimestampWithTimeZoneDecoder(ParquetEncoding encoding)
    {
        ValueDecoder<long[]> delegate = getLongDecoder(encoding);
        return new ValueDecoder<>()
        {
            @Override
            public void init(SimpleSliceInputStream input)
            {
                delegate.init(input);
            }

            @Override
            public void read(int[] values, int offset, int length)
            {
                long[] buffer = new long[length];
                delegate.read(buffer, 0, length);
                // decoded values are epochMicros, convert to (packed epochMillisUtc, picosOfMilli)
                for (int i = 0; i < length; i++) {
                    long epochMicros = buffer[i];
                    encodeFixed12(
                            packDateTimeWithZone(floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND), UTC_KEY),
                            floorMod(epochMicros, MICROSECONDS_PER_MILLISECOND) * PICOSECONDS_PER_MICROSECOND,
                            values,
                            i + offset);
                }
            }

            @Override
            public void skip(int n)
            {
                delegate.skip(n);
            }
        };
    }

    public ValueDecoder<int[]> getInt64TimestampNanosToLongTimestampDecoder(ParquetEncoding encoding)
    {
        ValueDecoder<long[]> delegate = getLongDecoder(encoding);
        return new ValueDecoder<>()
        {
            @Override
            public void init(SimpleSliceInputStream input)
            {
                delegate.init(input);
            }

            @Override
            public void read(int[] values, int offset, int length)
            {
                long[] buffer = new long[length];
                delegate.read(buffer, 0, length);
                // decoded values are epochNanos, convert to (epochMicros, picosOfMicro)
                for (int i = 0; i < length; i++) {
                    long epochNanos = buffer[i];
                    encodeFixed12(
                            floorDiv(epochNanos, NANOSECONDS_PER_MICROSECOND),
                            floorMod(epochNanos, NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND,
                            values,
                            i + offset);
                }
            }

            @Override
            public void skip(int n)
            {
                delegate.skip(n);
            }
        };
    }

    public ValueDecoder<long[]> getFloatToDoubleDecoder(ParquetEncoding encoding)
    {
        ValueDecoder<int[]> delegate = getRealDecoder(encoding);
        return new ValueDecoder<>()
        {
            @Override
            public void init(SimpleSliceInputStream input)
            {
                delegate.init(input);
            }

            @Override
            public void read(long[] values, int offset, int length)
            {
                int[] buffer = new int[length];
                delegate.read(buffer, 0, length);
                for (int i = 0; i < length; i++) {
                    values[offset + i] = Double.doubleToLongBits(Float.intBitsToFloat(buffer[i]));
                }
            }

            @Override
            public void skip(int n)
            {
                delegate.skip(n);
            }
        };
    }

    public ValueDecoder<long[]> getBinaryLongDecimalDecoder(ParquetEncoding encoding)
    {
        return new BinaryToLongDecimalTransformDecoder(getBinaryDecoder(encoding));
    }

    public ValueDecoder<long[]> getDeltaFixedWidthLongDecimalDecoder(ParquetEncoding encoding)
    {
        checkArgument(encoding.equals(DELTA_BYTE_ARRAY), "encoding %s is not DELTA_BYTE_ARRAY", encoding);
        ColumnDescriptor descriptor = field.getDescriptor();
        LogicalTypeAnnotation logicalTypeAnnotation = descriptor.getPrimitiveType().getLogicalTypeAnnotation();
        checkArgument(
                logicalTypeAnnotation instanceof DecimalLogicalTypeAnnotation decimalAnnotation
                        && decimalAnnotation.getPrecision() > Decimals.MAX_SHORT_PRECISION,
                "Column %s is not a long decimal",
                descriptor);
        return new BinaryToLongDecimalTransformDecoder(new BinaryDeltaByteArrayDecoder());
    }

    public ValueDecoder<long[]> getBinaryShortDecimalDecoder(ParquetEncoding encoding)
    {
        ValueDecoder<BinaryBuffer> delegate = getBinaryDecoder(encoding);
        return new ValueDecoder<>()
        {
            @Override
            public void init(SimpleSliceInputStream input)
            {
                delegate.init(input);
            }

            @Override
            public void read(long[] values, int offset, int length)
            {
                BinaryBuffer buffer = new BinaryBuffer(length);
                delegate.read(buffer, 0, length);
                int[] offsets = buffer.getOffsets();
                byte[] inputBytes = buffer.asSlice().byteArray();

                for (int i = 0; i < length; i++) {
                    int positionOffset = offsets[i];
                    int positionLength = offsets[i + 1] - positionOffset;
                    if (positionLength > 8) {
                        throw new ParquetDecodingException("Unable to read BINARY type decimal of size " + positionLength + " as a short decimal");
                    }
                    // No need for checkBytesFitInShortDecimal as the standard requires variable binary decimals
                    // to be stored in minimum possible number of bytes
                    values[offset + i] = getShortDecimalValue(inputBytes, positionOffset, positionLength);
                }
            }

            @Override
            public void skip(int n)
            {
                delegate.skip(n);
            }
        };
    }

    public ValueDecoder<long[]> getDeltaFixedWidthShortDecimalDecoder(ParquetEncoding encoding)
    {
        checkArgument(encoding.equals(DELTA_BYTE_ARRAY), "encoding %s is not DELTA_BYTE_ARRAY", encoding);
        ColumnDescriptor descriptor = field.getDescriptor();
        LogicalTypeAnnotation logicalTypeAnnotation = descriptor.getPrimitiveType().getLogicalTypeAnnotation();
        checkArgument(
                logicalTypeAnnotation instanceof DecimalLogicalTypeAnnotation decimalAnnotation
                        && decimalAnnotation.getPrecision() <= Decimals.MAX_SHORT_PRECISION,
                "Column %s is not a short decimal",
                descriptor);
        int typeLength = descriptor.getPrimitiveType().getTypeLength();
        checkArgument(typeLength > 0 && typeLength <= 16, "Expected column %s to have type length in range (1-16)", descriptor);
        return new ValueDecoder<>()
        {
            private final ValueDecoder<BinaryBuffer> delegate = new BinaryDeltaByteArrayDecoder();

            @Override
            public void init(SimpleSliceInputStream input)
            {
                delegate.init(input);
            }

            @Override
            public void read(long[] values, int offset, int length)
            {
                BinaryBuffer buffer = new BinaryBuffer(length);
                delegate.read(buffer, 0, length);

                // Each position in FIXED_LEN_BYTE_ARRAY has fixed length
                int bytesOffset = 0;
                int bytesLength = typeLength;
                if (typeLength > Long.BYTES) {
                    bytesOffset = typeLength - Long.BYTES;
                    bytesLength = Long.BYTES;
                }

                byte[] inputBytes = buffer.asSlice().byteArray();
                int[] offsets = buffer.getOffsets();
                for (int i = 0; i < length; i++) {
                    int inputOffset = offsets[i];
                    checkBytesFitInShortDecimal(inputBytes, inputOffset, bytesOffset, descriptor);
                    values[offset + i] = getShortDecimalValue(inputBytes, inputOffset + bytesOffset, bytesLength);
                }
            }

            @Override
            public void skip(int n)
            {
                delegate.skip(n);
            }
        };
    }

    public ValueDecoder<long[]> getRescaledLongDecimalDecoder(ParquetEncoding encoding)
    {
        DecimalType decimalType = (DecimalType) field.getType();
        DecimalLogicalTypeAnnotation decimalAnnotation = (DecimalLogicalTypeAnnotation) field.getDescriptor().getPrimitiveType().getLogicalTypeAnnotation();
        if (decimalAnnotation.getPrecision() <= Decimals.MAX_SHORT_PRECISION) {
            ValueDecoder<long[]> delegate = getShortDecimalDecoder(encoding);
            return new ValueDecoder<>()
            {
                @Override
                public void init(SimpleSliceInputStream input)
                {
                    delegate.init(input);
                }

                @Override
                public void read(long[] values, int offset, int length)
                {
                    long[] buffer = new long[length];
                    delegate.read(buffer, 0, length);
                    for (int i = 0; i < length; i++) {
                        Int128 rescaled = DecimalConversions.shortToLongCast(
                                buffer[i],
                                decimalAnnotation.getPrecision(),
                                decimalAnnotation.getScale(),
                                decimalType.getPrecision(),
                                decimalType.getScale());

                        values[2 * (offset + i)] = rescaled.getHigh();
                        values[2 * (offset + i) + 1] = rescaled.getLow();
                    }
                }

                @Override
                public void skip(int n)
                {
                    delegate.skip(n);
                }
            };
        }
        return new InlineTransformDecoder<>(
                getLongDecimalDecoder(encoding),
                (values, offset, length) -> {
                    int endOffset = (offset + length) * 2;
                    for (int currentOffset = offset * 2; currentOffset < endOffset; currentOffset += 2) {
                        Int128 rescaled = DecimalConversions.longToLongCast(
                                Int128.valueOf(values[currentOffset], values[currentOffset + 1]),
                                decimalAnnotation.getPrecision(),
                                decimalAnnotation.getScale(),
                                decimalType.getPrecision(),
                                decimalType.getScale());

                        values[currentOffset] = rescaled.getHigh();
                        values[currentOffset + 1] = rescaled.getLow();
                    }
                });
    }

    public ValueDecoder<long[]> getRescaledShortDecimalDecoder(ParquetEncoding encoding)
    {
        DecimalType decimalType = (DecimalType) field.getType();
        DecimalLogicalTypeAnnotation decimalAnnotation = (DecimalLogicalTypeAnnotation) field.getDescriptor().getPrimitiveType().getLogicalTypeAnnotation();
        if (decimalAnnotation.getPrecision() <= Decimals.MAX_SHORT_PRECISION) {
            long rescale = longTenToNth(Math.abs(decimalType.getScale() - decimalAnnotation.getScale()));
            return new InlineTransformDecoder<>(
                    getShortDecimalDecoder(encoding),
                    (values, offset, length) -> {
                        for (int i = offset; i < offset + length; i++) {
                            values[i] = DecimalConversions.shortToShortCast(
                                    values[i],
                                    decimalAnnotation.getPrecision(),
                                    decimalAnnotation.getScale(),
                                    decimalType.getPrecision(),
                                    decimalType.getScale(),
                                    rescale,
                                    rescale / 2);
                        }
                    });
        }
        ValueDecoder<long[]> delegate = getLongDecimalDecoder(encoding);
        return new ValueDecoder<>()
        {
            @Override
            public void init(SimpleSliceInputStream input)
            {
                delegate.init(input);
            }

            @Override
            public void read(long[] values, int offset, int length)
            {
                long[] buffer = new long[2 * length];
                delegate.read(buffer, 0, length);
                for (int i = 0; i < length; i++) {
                    values[offset + i] = DecimalConversions.longToShortCast(
                            Int128.valueOf(buffer[2 * i], buffer[2 * i + 1]),
                            decimalAnnotation.getPrecision(),
                            decimalAnnotation.getScale(),
                            decimalType.getPrecision(),
                            decimalType.getScale());
                }
            }

            @Override
            public void skip(int n)
            {
                delegate.skip(n);
            }
        };
    }

    public ValueDecoder<long[]> getInt32ToShortDecimalDecoder(ParquetEncoding encoding)
    {
        DecimalType decimalType = (DecimalType) field.getType();
        ValueDecoder<int[]> delegate = getInt32Decoder(encoding);
        return new ValueDecoder<>()
        {
            @Override
            public void init(SimpleSliceInputStream input)
            {
                delegate.init(input);
            }

            @Override
            public void read(long[] values, int offset, int length)
            {
                int[] buffer = new int[length];
                delegate.read(buffer, 0, length);
                for (int i = 0; i < length; i++) {
                    if (overflows(buffer[i], decimalType.getPrecision())) {
                        throw new TrinoException(
                                INVALID_CAST_ARGUMENT,
                                format("Cannot read parquet INT32 value '%s' as DECIMAL(%s, %s)", buffer[i], decimalType.getPrecision(), decimalType.getScale()));
                    }
                    values[i + offset] = buffer[i];
                }
            }

            @Override
            public void skip(int n)
            {
                delegate.skip(n);
            }
        };
    }

    public ValueDecoder<long[]> getInt32ToLongDecoder(ParquetEncoding encoding)
    {
        ValueDecoder<int[]> delegate = getInt32Decoder(encoding);
        return new ValueDecoder<>()
        {
            @Override
            public void init(SimpleSliceInputStream input)
            {
                delegate.init(input);
            }

            @Override
            public void read(long[] values, int offset, int length)
            {
                int[] buffer = new int[length];
                delegate.read(buffer, 0, length);
                for (int i = 0; i < length; i++) {
                    values[i + offset] = buffer[i];
                }
            }

            @Override
            public void skip(int n)
            {
                delegate.skip(n);
            }
        };
    }

    public ValueDecoder<int[]> getInt64ToIntDecoder(ParquetEncoding encoding)
    {
        return new LongToIntTransformDecoder(getLongDecoder(encoding));
    }

    public ValueDecoder<int[]> getShortDecimalToIntDecoder(ParquetEncoding encoding)
    {
        return new LongToIntTransformDecoder(getShortDecimalDecoder(encoding));
    }

    public ValueDecoder<short[]> getInt64ToShortDecoder(ParquetEncoding encoding)
    {
        return new LongToShortTransformDecoder(getLongDecoder(encoding));
    }

    public ValueDecoder<short[]> getShortDecimalToShortDecoder(ParquetEncoding encoding)
    {
        return new LongToShortTransformDecoder(getShortDecimalDecoder(encoding));
    }

    public ValueDecoder<byte[]> getInt64ToByteDecoder(ParquetEncoding encoding)
    {
        return new LongToByteTransformDecoder(getLongDecoder(encoding));
    }

    public ValueDecoder<byte[]> getShortDecimalToByteDecoder(ParquetEncoding encoding)
    {
        return new LongToByteTransformDecoder(getShortDecimalDecoder(encoding));
    }

    public ValueDecoder<long[]> getDeltaUuidDecoder(ParquetEncoding encoding)
    {
        checkArgument(encoding.equals(DELTA_BYTE_ARRAY), "encoding %s is not DELTA_BYTE_ARRAY", encoding);
        ValueDecoder<BinaryBuffer> delegate = new BinaryDeltaByteArrayDecoder();
        return new ValueDecoder<>()
        {
            @Override
            public void init(SimpleSliceInputStream input)
            {
                delegate.init(input);
            }

            @Override
            public void read(long[] values, int offset, int length)
            {
                BinaryBuffer buffer = new BinaryBuffer(length);
                delegate.read(buffer, 0, length);
                SimpleSliceInputStream binaryInput = new SimpleSliceInputStream(buffer.asSlice());

                int endOffset = (offset + length) * 2;
                for (int outputOffset = offset * 2; outputOffset < endOffset; outputOffset += 2) {
                    values[outputOffset] = binaryInput.readLong();
                    values[outputOffset + 1] = binaryInput.readLong();
                }
            }

            @Override
            public void skip(int n)
            {
                delegate.skip(n);
            }
        };
    }

    private static class LongToIntTransformDecoder
            implements ValueDecoder<int[]>
    {
        private final ValueDecoder<long[]> delegate;

        private LongToIntTransformDecoder(ValueDecoder<long[]> delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            delegate.init(input);
        }

        @Override
        public void read(int[] values, int offset, int length)
        {
            long[] buffer = new long[length];
            delegate.read(buffer, 0, length);
            for (int i = 0; i < length; i++) {
                values[offset + i] = toIntExact(buffer[i]);
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }

    private static class LongToShortTransformDecoder
            implements ValueDecoder<short[]>
    {
        private final ValueDecoder<long[]> delegate;

        private LongToShortTransformDecoder(ValueDecoder<long[]> delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            delegate.init(input);
        }

        @Override
        public void read(short[] values, int offset, int length)
        {
            long[] buffer = new long[length];
            delegate.read(buffer, 0, length);
            for (int i = 0; i < length; i++) {
                values[offset + i] = toShortExact(buffer[i]);
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }

    private static class LongToByteTransformDecoder
            implements ValueDecoder<byte[]>
    {
        private final ValueDecoder<long[]> delegate;

        private LongToByteTransformDecoder(ValueDecoder<long[]> delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            delegate.init(input);
        }

        @Override
        public void read(byte[] values, int offset, int length)
        {
            long[] buffer = new long[length];
            delegate.read(buffer, 0, length);
            for (int i = 0; i < length; i++) {
                values[offset + i] = toByteExact(buffer[i]);
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }

    private static class BinaryToLongDecimalTransformDecoder
            implements ValueDecoder<long[]>
    {
        private final ValueDecoder<BinaryBuffer> delegate;

        private BinaryToLongDecimalTransformDecoder(ValueDecoder<BinaryBuffer> delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            delegate.init(input);
        }

        @Override
        public void read(long[] values, int offset, int length)
        {
            BinaryBuffer buffer = new BinaryBuffer(length);
            delegate.read(buffer, 0, length);
            int[] offsets = buffer.getOffsets();
            Slice binaryInput = buffer.asSlice();

            for (int i = 0; i < length; i++) {
                int positionOffset = offsets[i];
                int positionLength = offsets[i + 1] - positionOffset;
                Int128 value = Int128.fromBigEndian(binaryInput.getBytes(positionOffset, positionLength));
                values[2 * (offset + i)] = value.getHigh();
                values[2 * (offset + i) + 1] = value.getLow();
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }

    private static class InlineTransformDecoder<T>
            implements ValueDecoder<T>
    {
        private final ValueDecoder<T> valueDecoder;
        private final TypeTransform<T> typeTransform;

        private InlineTransformDecoder(ValueDecoder<T> valueDecoder, TypeTransform<T> typeTransform)
        {
            this.valueDecoder = requireNonNull(valueDecoder, "valueDecoder is null");
            this.typeTransform = requireNonNull(typeTransform, "typeTransform is null");
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            valueDecoder.init(input);
        }

        @Override
        public void read(T values, int offset, int length)
        {
            valueDecoder.read(values, offset, length);
            typeTransform.process(values, offset, length);
        }

        @Override
        public void skip(int n)
        {
            valueDecoder.skip(n);
        }
    }

    private interface TypeTransform<T>
    {
        void process(T values, int offset, int length);
    }

    private ValuesReader getApacheParquetReader(ParquetEncoding encoding)
    {
        return encoding.getValuesReader(field.getDescriptor(), VALUES);
    }

    private IllegalArgumentException wrongEncoding(ParquetEncoding encoding)
    {
        return new IllegalArgumentException("Wrong encoding " + encoding + " for column " + field.getDescriptor());
    }
}
