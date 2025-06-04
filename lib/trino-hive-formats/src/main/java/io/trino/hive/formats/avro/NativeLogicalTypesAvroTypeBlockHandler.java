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
package io.trino.hive.formats.avro;

import io.trino.annotation.NotThreadSafe;
import io.trino.hive.formats.avro.model.AvroLogicalType;
import io.trino.hive.formats.avro.model.AvroLogicalType.BytesDecimalLogicalType;
import io.trino.hive.formats.avro.model.AvroLogicalType.DateLogicalType;
import io.trino.hive.formats.avro.model.AvroLogicalType.FixedDecimalLogicalType;
import io.trino.hive.formats.avro.model.AvroLogicalType.StringUUIDLogicalType;
import io.trino.hive.formats.avro.model.AvroLogicalType.TimeMicrosLogicalType;
import io.trino.hive.formats.avro.model.AvroLogicalType.TimeMillisLogicalType;
import io.trino.hive.formats.avro.model.AvroLogicalType.TimestampMicrosLogicalType;
import io.trino.hive.formats.avro.model.AvroLogicalType.TimestampMillisLogicalType;
import io.trino.hive.formats.avro.model.AvroReadAction;
import io.trino.hive.formats.avro.model.AvroReadAction.LongIoFunction;
import io.trino.hive.formats.avro.model.BytesRead;
import io.trino.hive.formats.avro.model.FixedRead;
import io.trino.hive.formats.avro.model.IntRead;
import io.trino.hive.formats.avro.model.LongRead;
import io.trino.hive.formats.avro.model.StringRead;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.Type;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.hive.formats.avro.BaseAvroTypeBlockHandlerImpls.MAX_ARRAY_SIZE;
import static io.trino.hive.formats.avro.BaseAvroTypeBlockHandlerImpls.baseBlockBuildingDecoderFor;
import static io.trino.hive.formats.avro.BaseAvroTypeBlockHandlerImpls.baseTypeFor;
import static io.trino.hive.formats.avro.NativeLogicalTypesAvroTypeManager.fromBigEndian;
import static io.trino.hive.formats.avro.NativeLogicalTypesAvroTypeManager.validateAndLogIssues;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static java.util.Objects.requireNonNull;

/**
 * This block handler class is designed to handle all Avro documented <a href="https://avro.apache.org/docs/1.11.1/specification/#logical-types">logical types</a>
 * and their reading as the appropriate Trino type.
 */
public class NativeLogicalTypesAvroTypeBlockHandler
        implements AvroTypeBlockHandler
{
    @Override
    public void configure(Map<String, byte[]> fileMetadata) {}

    @Override
    public Type typeFor(Schema schema)
            throws AvroTypeException
    {
        Optional<Type> avroLogicalTypeTrinoType = validateAndLogIssues(schema)
                .map(NativeLogicalTypesAvroTypeManager::getAvroLogicalTypeSpiType);
        if (avroLogicalTypeTrinoType.isPresent()) {
            return avroLogicalTypeTrinoType.get();
        }
        return baseTypeFor(schema, this);
    }

    @Override
    public BlockBuildingDecoder blockBuildingDecoderFor(AvroReadAction readAction)
            throws AvroTypeException
    {
        Optional<AvroLogicalType> avroLogicalType = validateAndLogIssues(readAction.readSchema());
        if (avroLogicalType.isPresent()) {
            return getLogicalTypeBuildingFunction(avroLogicalType.get(), readAction);
        }
        return baseBlockBuildingDecoderFor(readAction, this);
    }

    static BlockBuildingDecoder getLogicalTypeBuildingFunction(AvroLogicalType logicalType, AvroReadAction readAction)
    {
        return switch (logicalType) {
            case DateLogicalType _ -> {
                if (readAction instanceof IntRead) {
                    yield new DateBlockBuildingDecoder();
                }
                throw new IllegalStateException("Unreachable unfiltered logical type");
            }
            case BytesDecimalLogicalType bytesDecimalLogicalType -> {
                if (readAction instanceof BytesRead) {
                    yield new BytesDecimalBlockBuildingDecoder(DecimalType.createDecimalType(bytesDecimalLogicalType.precision(), bytesDecimalLogicalType.scale()));
                }
                throw new IllegalStateException("Unreachable unfiltered logical type");
            }
            case FixedDecimalLogicalType fixedDecimalLogicalType -> {
                if (readAction instanceof FixedRead) {
                    yield new FixedDecimalBlockBuildingDecoder(DecimalType.createDecimalType(fixedDecimalLogicalType.precision(), fixedDecimalLogicalType.scale()), fixedDecimalLogicalType.fixedSize());
                }
                throw new IllegalStateException("Unreachable unfiltered logical type");
            }
            case TimeMillisLogicalType _ -> {
                if (readAction instanceof IntRead) {
                    yield new TimeMillisBlockBuildingDecoder();
                }
                throw new IllegalStateException("Unreachable unfiltered logical type");
            }
            case TimeMicrosLogicalType _ -> {
                if (readAction instanceof LongRead) {
                    yield new TimeMicrosBlockBuildingDecoder();
                }
                throw new IllegalStateException("Unreachable unfiltered logical type");
            }
            case TimestampMillisLogicalType _ -> {
                if (readAction instanceof LongRead longRead) {
                    yield new TimestampMillisBlockBuildingDecoder(longRead);
                }
                throw new IllegalStateException("Unreachable unfiltered logical type");
            }
            case TimestampMicrosLogicalType _ -> {
                if (readAction instanceof LongRead longRead) {
                    yield new TimestampMicrosBlockBuildingDecoder(longRead);
                }
                throw new IllegalStateException("Unreachable unfiltered logical type");
            }
            case StringUUIDLogicalType _ -> {
                if (readAction instanceof StringRead) {
                    yield new StringUUIDBlockBuildingDecoder();
                }
                throw new IllegalStateException("Unreachable unfiltered logical type");
            }
        };
    }

    public record DateBlockBuildingDecoder()
            implements BlockBuildingDecoder
    {
        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            DATE.writeLong(builder, decoder.readInt());
        }
    }

    public record BytesDecimalBlockBuildingDecoder(DecimalType decimalType)
            implements BlockBuildingDecoder
    {
        public BytesDecimalBlockBuildingDecoder
        {
            requireNonNull(decimalType, "decimalType is null");
        }

        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            // it is only possible to read Bytes type when underlying write type is String or Bytes
            // both have the same encoding, so coercion is a no-op
            long size = decoder.readLong();
            if (size > MAX_ARRAY_SIZE) {
                throw new IOException("Unable to read avro Bytes with size greater than %s. Found Bytes size: %s".formatted(MAX_ARRAY_SIZE, size));
            }
            byte[] bytes = new byte[(int) size];
            decoder.readFixed(bytes);
            if (decimalType.isShort()) {
                decimalType.writeLong(builder, fromBigEndian(bytes));
            }
            else {
                decimalType.writeObject(builder, Int128.fromBigEndian(bytes));
            }
        }
    }

    @NotThreadSafe
    public static class FixedDecimalBlockBuildingDecoder
            implements BlockBuildingDecoder
    {
        private final DecimalType decimalType;
        private final byte[] bytes;

        public FixedDecimalBlockBuildingDecoder(DecimalType decimalType, int fixedSize)
        {
            this.decimalType = requireNonNull(decimalType, "decimalType is null");
            verify(fixedSize > 0, "fixedSize must be over 0");
            bytes = new byte[fixedSize];
        }

        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            decoder.readFixed(bytes);
            if (decimalType.isShort()) {
                decimalType.writeLong(builder, fromBigEndian(bytes));
            }
            else {
                decimalType.writeObject(builder, Int128.fromBigEndian(bytes));
            }
        }
    }

    public record TimeMillisBlockBuildingDecoder()
            implements BlockBuildingDecoder
    {
        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            TIME_MILLIS.writeLong(builder, ((long) decoder.readInt()) * Timestamps.PICOSECONDS_PER_MILLISECOND);
        }
    }

    public record TimeMicrosBlockBuildingDecoder()
            implements BlockBuildingDecoder
    {
        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            TIME_MICROS.writeLong(builder, decoder.readLong() * Timestamps.PICOSECONDS_PER_MICROSECOND);
        }
    }

    public static class TimestampMillisBlockBuildingDecoder
            implements BlockBuildingDecoder
    {
        private final LongIoFunction<Decoder> longDecoder;

        public TimestampMillisBlockBuildingDecoder(LongRead longRead)
        {
            longDecoder = requireNonNull(longRead, "longRead is null").getLongDecoder();
        }

        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            TIMESTAMP_MILLIS.writeLong(builder, longDecoder.apply(decoder) * Timestamps.MICROSECONDS_PER_MILLISECOND);
        }
    }

    public static class TimestampMicrosBlockBuildingDecoder
            implements BlockBuildingDecoder
    {
        private final LongIoFunction<Decoder> longDecoder;

        public TimestampMicrosBlockBuildingDecoder(LongRead longRead)
        {
            longDecoder = requireNonNull(longRead, "longRead is null").getLongDecoder();
        }

        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            TIMESTAMP_MICROS.writeLong(builder, longDecoder.apply(decoder));
        }
    }

    public record StringUUIDBlockBuildingDecoder()
            implements BlockBuildingDecoder
    {
        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            // it is only possible to read String type when underlying write type is String or Bytes
            // both have the same encoding, so coercion is a no-op
            long size = decoder.readLong();
            if (size > MAX_ARRAY_SIZE) {
                throw new IOException("Unable to read avro String with size greater than %s. Found String size: %s".formatted(MAX_ARRAY_SIZE, size));
            }
            // these bytes represent the string representation of the UUID, not the UUID bytes themselves
            byte[] bytes = new byte[(int) size];
            decoder.readFixed(bytes);
            UUID.writeSlice(builder, javaUuidToTrinoUuid(java.util.UUID.fromString(new String(bytes, StandardCharsets.UTF_8))));
        }
    }
}
