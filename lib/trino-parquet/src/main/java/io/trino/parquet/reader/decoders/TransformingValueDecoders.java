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
import io.trino.spi.type.DecimalConversions;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.joda.time.DateTimeZone;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.ParquetEncoding.DELTA_BYTE_ARRAY;
import static io.trino.parquet.ParquetReaderUtils.toByteExact;
import static io.trino.parquet.ParquetReaderUtils.toShortExact;
import static io.trino.parquet.ParquetTypeUtils.checkBytesFitInShortDecimal;
import static io.trino.parquet.ParquetTypeUtils.getShortDecimalValue;
import static io.trino.parquet.reader.decoders.DeltaByteArrayDecoders.BinaryDeltaByteArrayDecoder;
import static io.trino.parquet.reader.decoders.ValueDecoders.getBinaryDecoder;
import static io.trino.parquet.reader.decoders.ValueDecoders.getInt32Decoder;
import static io.trino.parquet.reader.decoders.ValueDecoders.getInt96Decoder;
import static io.trino.parquet.reader.decoders.ValueDecoders.getLongDecimalDecoder;
import static io.trino.parquet.reader.decoders.ValueDecoders.getLongDecoder;
import static io.trino.parquet.reader.decoders.ValueDecoders.getRealDecoder;
import static io.trino.parquet.reader.decoders.ValueDecoders.getShortDecimalDecoder;
import static io.trino.parquet.reader.flat.Int96ColumnAdapter.Int96Buffer;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.Decimals.longTenToNth;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;

/**
 * {@link io.trino.parquet.reader.decoders.ValueDecoder} implementations which build on top of implementations from {@link io.trino.parquet.reader.decoders.ValueDecoders}.
 * These decoders apply transformations to the output of an underlying primitive parquet type decoder to convert it into values
 * which can be used by {@link io.trino.parquet.reader.flat.ColumnAdapter} to create Trino blocks.
 */
public class TransformingValueDecoders
{
    private TransformingValueDecoders() {}

    public static ValueDecoder<long[]> getTimeMicrosDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return new InlineTransformDecoder<>(
                getLongDecoder(encoding, field),
                (values, offset, length) -> {
                    for (int i = offset; i < offset + length; i++) {
                        values[i] = values[i] * PICOSECONDS_PER_MICROSECOND;
                    }
                });
    }

    public static ValueDecoder<long[]> getInt96ToShortTimestampDecoder(ParquetEncoding encoding, PrimitiveField field, DateTimeZone timeZone)
    {
        checkArgument(
                field.getType() instanceof TimestampType timestampType && timestampType.isShort(),
                "Trino type %s is not a short timestamp",
                field.getType());
        int precision = ((TimestampType) field.getType()).getPrecision();
        ValueDecoder<Int96Buffer> delegate = getInt96Decoder(encoding, field);
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
                Int96Buffer int96Buffer = new Int96Buffer(length);
                delegate.read(int96Buffer, 0, length);
                for (int i = 0; i < length; i++) {
                    long epochSeconds = int96Buffer.longs[i];
                    long epochMicros;
                    if (timeZone == DateTimeZone.UTC) {
                        epochMicros = epochSeconds * MICROSECONDS_PER_SECOND;
                    }
                    else {
                        epochMicros = timeZone.convertUTCToLocal(epochSeconds * MILLISECONDS_PER_SECOND) * MICROSECONDS_PER_MILLISECOND;
                    }
                    int nanosOfSecond = (int) round(int96Buffer.ints[i], 9 - precision);
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

    public static ValueDecoder<Int96Buffer> getInt96ToLongTimestampDecoder(ParquetEncoding encoding, PrimitiveField field, DateTimeZone timeZone)
    {
        checkArgument(
                field.getType() instanceof TimestampType timestampType && !timestampType.isShort(),
                "Trino type %s is not a long timestamp",
                field.getType());
        int precision = ((TimestampType) field.getType()).getPrecision();
        return new InlineTransformDecoder<>(
                getInt96Decoder(encoding, field),
                (values, offset, length) -> {
                    for (int i = offset; i < offset + length; i++) {
                        long epochSeconds = values.longs[i];
                        long nanosOfSecond = values.ints[i];
                        if (timeZone != DateTimeZone.UTC) {
                            epochSeconds = timeZone.convertUTCToLocal(epochSeconds * MILLISECONDS_PER_SECOND) / MILLISECONDS_PER_SECOND;
                        }
                        if (precision < 9) {
                            nanosOfSecond = (int) round(nanosOfSecond, 9 - precision);
                        }
                        // epochMicros
                        values.longs[i] = epochSeconds * MICROSECONDS_PER_SECOND + (nanosOfSecond / NANOSECONDS_PER_MICROSECOND);
                        // picosOfMicro
                        values.ints[i] = (int) ((nanosOfSecond * PICOSECONDS_PER_NANOSECOND) % PICOSECONDS_PER_MICROSECOND);
                    }
                });
    }

    public static ValueDecoder<long[]> getInt96ToShortTimestampWithTimeZoneDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        checkArgument(
                field.getType() instanceof TimestampWithTimeZoneType timestampWithTimeZoneType && timestampWithTimeZoneType.isShort(),
                "Trino type %s is not a short timestamp with timezone",
                field.getType());
        ValueDecoder<Int96Buffer> delegate = getInt96Decoder(encoding, field);
        return new ValueDecoder<>() {
            @Override
            public void init(SimpleSliceInputStream input)
            {
                delegate.init(input);
            }

            @Override
            public void read(long[] values, int offset, int length)
            {
                Int96Buffer int96Buffer = new Int96Buffer(length);
                delegate.read(int96Buffer, 0, length);
                for (int i = 0; i < length; i++) {
                    long epochSeconds = int96Buffer.longs[i];
                    int nanosOfSecond = int96Buffer.ints[i];
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

    public static ValueDecoder<long[]> getInt64TimestampMillsToShortTimestampDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        checkArgument(
                field.getType() instanceof TimestampType timestampType && timestampType.isShort(),
                "Trino type %s is not a short timestamp",
                field.getType());
        int precision = ((TimestampType) field.getType()).getPrecision();
        ValueDecoder<long[]> valueDecoder = getLongDecoder(encoding, field);
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

    public static ValueDecoder<long[]> getInt64TimestampMillsToShortTimestampWithTimeZoneDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        checkArgument(
                field.getType() instanceof TimestampWithTimeZoneType timestampWithTimeZoneType && timestampWithTimeZoneType.isShort(),
                "Trino type %s is not a short timestamp",
                field.getType());
        int precision = ((TimestampWithTimeZoneType) field.getType()).getPrecision();
        ValueDecoder<long[]> valueDecoder = getLongDecoder(encoding, field);
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

    public static ValueDecoder<long[]> getInt64TimestampMicrosToShortTimestampDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        checkArgument(
                field.getType() instanceof TimestampType timestampType && timestampType.isShort(),
                "Trino type %s is not a short timestamp",
                field.getType());
        int precision = ((TimestampType) field.getType()).getPrecision();
        ValueDecoder<long[]> valueDecoder = getLongDecoder(encoding, field);
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

    public static ValueDecoder<long[]> getInt64TimestampMicrosToShortTimestampWithTimeZoneDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        checkArgument(
                field.getType() instanceof TimestampWithTimeZoneType timestampWithTimeZoneType && timestampWithTimeZoneType.isShort(),
                "Trino type %s is not a short timestamp",
                field.getType());
        int precision = ((TimestampWithTimeZoneType) field.getType()).getPrecision();
        return new InlineTransformDecoder<>(
                getLongDecoder(encoding, field),
                (values, offset, length) -> {
                    // decoded values are epochMicros, round to lower precision and convert to packed millis utc value
                    for (int i = offset; i < offset + length; i++) {
                        values[i] = packDateTimeWithZone(round(values[i], 6 - precision) / MICROSECONDS_PER_MILLISECOND, UTC_KEY);
                    }
                });
    }

    public static ValueDecoder<long[]> getInt64TimestampNanosToShortTimestampDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        checkArgument(
                field.getType() instanceof TimestampType timestampType && timestampType.isShort(),
                "Trino type %s is not a short timestamp",
                field.getType());
        int precision = ((TimestampType) field.getType()).getPrecision();
        return new InlineTransformDecoder<>(
                getLongDecoder(encoding, field),
                (values, offset, length) -> {
                    // decoded values are epochNanos, round to lower precision and convert to epochMicros
                    for (int i = offset; i < offset + length; i++) {
                        values[i] = round(values[i], 9 - precision) / NANOSECONDS_PER_MICROSECOND;
                    }
                });
    }

    public static ValueDecoder<Int96Buffer> getInt64TimestampMillisToLongTimestampDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        ValueDecoder<long[]> delegate = getLongDecoder(encoding, field);
        return new ValueDecoder<>()
        {
            @Override
            public void init(SimpleSliceInputStream input)
            {
                delegate.init(input);
            }

            @Override
            public void read(Int96Buffer values, int offset, int length)
            {
                delegate.read(values.longs, offset, length);
                // decoded values are epochMillis, convert to epochMicros
                for (int i = offset; i < offset + length; i++) {
                    values.longs[i] = values.longs[i] * MICROSECONDS_PER_MILLISECOND;
                }
            }

            @Override
            public void skip(int n)
            {
                delegate.skip(n);
            }
        };
    }

    public static ValueDecoder<Int96Buffer> getInt64TimestampMicrosToLongTimestampDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        ValueDecoder<long[]> delegate = getLongDecoder(encoding, field);
        return new ValueDecoder<>()
        {
            @Override
            public void init(SimpleSliceInputStream input)
            {
                delegate.init(input);
            }

            @Override
            public void read(Int96Buffer values, int offset, int length)
            {
                // decoded values are epochMicros
                delegate.read(values.longs, offset, length);
            }

            @Override
            public void skip(int n)
            {
                delegate.skip(n);
            }
        };
    }

    public static ValueDecoder<Int96Buffer> getInt64TimestampMicrosToLongTimestampWithTimeZoneDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        ValueDecoder<long[]> delegate = getLongDecoder(encoding, field);
        return new ValueDecoder<>()
        {
            @Override
            public void init(SimpleSliceInputStream input)
            {
                delegate.init(input);
            }

            @Override
            public void read(Int96Buffer values, int offset, int length)
            {
                delegate.read(values.longs, offset, length);
                // decoded values are epochMicros, convert to (packed epochMillisUtc, picosOfMilli)
                for (int i = offset; i < offset + length; i++) {
                    long epochMicros = values.longs[i];
                    values.longs[i] = packDateTimeWithZone(floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND), UTC_KEY);
                    values.ints[i] = floorMod(epochMicros, MICROSECONDS_PER_MILLISECOND) * PICOSECONDS_PER_MICROSECOND;
                }
            }

            @Override
            public void skip(int n)
            {
                delegate.skip(n);
            }
        };
    }

    public static ValueDecoder<Int96Buffer> getInt64TimestampNanosToLongTimestampDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        ValueDecoder<long[]> delegate = getLongDecoder(encoding, field);
        return new ValueDecoder<>()
        {
            @Override
            public void init(SimpleSliceInputStream input)
            {
                delegate.init(input);
            }

            @Override
            public void read(Int96Buffer values, int offset, int length)
            {
                delegate.read(values.longs, offset, length);
                // decoded values are epochNanos, convert to (epochMicros, picosOfMicro)
                for (int i = offset; i < offset + length; i++) {
                    long epochNanos = values.longs[i];
                    values.longs[i] = floorDiv(epochNanos, NANOSECONDS_PER_MICROSECOND);
                    values.ints[i] = floorMod(epochNanos, NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND;
                }
            }

            @Override
            public void skip(int n)
            {
                delegate.skip(n);
            }
        };
    }

    public static ValueDecoder<long[]> getFloatToDoubleDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        ValueDecoder<int[]> delegate = getRealDecoder(encoding, field);
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

    public static ValueDecoder<long[]> getBinaryLongDecimalDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return new BinaryToLongDecimalTransformDecoder(getBinaryDecoder(encoding, field));
    }

    public static ValueDecoder<long[]> getDeltaFixedWidthLongDecimalDecoder(ParquetEncoding encoding, PrimitiveField field)
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

    public static ValueDecoder<long[]> getBinaryShortDecimalDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        ValueDecoder<BinaryBuffer> delegate = getBinaryDecoder(encoding, field);
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

    public static ValueDecoder<long[]> getDeltaFixedWidthShortDecimalDecoder(ParquetEncoding encoding, PrimitiveField field)
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

    public static ValueDecoder<long[]> getRescaledLongDecimalDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        DecimalType decimalType = (DecimalType) field.getType();
        DecimalLogicalTypeAnnotation decimalAnnotation = (DecimalLogicalTypeAnnotation) field.getDescriptor().getPrimitiveType().getLogicalTypeAnnotation();
        if (decimalAnnotation.getPrecision() <= Decimals.MAX_SHORT_PRECISION) {
            ValueDecoder<long[]> delegate = getShortDecimalDecoder(encoding, field);
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
                getLongDecimalDecoder(encoding, field),
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

    public static ValueDecoder<long[]> getRescaledShortDecimalDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        DecimalType decimalType = (DecimalType) field.getType();
        DecimalLogicalTypeAnnotation decimalAnnotation = (DecimalLogicalTypeAnnotation) field.getDescriptor().getPrimitiveType().getLogicalTypeAnnotation();
        if (decimalAnnotation.getPrecision() <= Decimals.MAX_SHORT_PRECISION) {
            long rescale = longTenToNth(Math.abs(decimalType.getScale() - decimalAnnotation.getScale()));
            return new InlineTransformDecoder<>(
                    getShortDecimalDecoder(encoding, field),
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
        ValueDecoder<long[]> delegate = getLongDecimalDecoder(encoding, field);
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

    public static ValueDecoder<long[]> getInt32ToLongDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        ValueDecoder<int[]> delegate = getInt32Decoder(encoding, field);
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

    public static ValueDecoder<int[]> getInt64ToIntDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return new LongToIntTransformDecoder(getLongDecoder(encoding, field));
    }

    public static ValueDecoder<int[]> getShortDecimalToIntDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return new LongToIntTransformDecoder(getShortDecimalDecoder(encoding, field));
    }

    public static ValueDecoder<short[]> getInt64ToShortDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return new LongToShortTransformDecoder(getLongDecoder(encoding, field));
    }

    public static ValueDecoder<short[]> getShortDecimalToShortDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return new LongToShortTransformDecoder(getShortDecimalDecoder(encoding, field));
    }

    public static ValueDecoder<byte[]> getInt64ToByteDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return new LongToByteTransformDecoder(getLongDecoder(encoding, field));
    }

    public static ValueDecoder<byte[]> getShortDecimalToByteDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return new LongToByteTransformDecoder(getShortDecimalDecoder(encoding, field));
    }

    public static ValueDecoder<long[]> getDeltaUuidDecoder(ParquetEncoding encoding)
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
}
