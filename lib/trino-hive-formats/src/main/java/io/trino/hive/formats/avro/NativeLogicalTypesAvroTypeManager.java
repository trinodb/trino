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

import com.google.common.base.VerifyException;
import com.google.common.primitives.Longs;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static java.util.Objects.requireNonNull;
import static org.apache.avro.LogicalTypes.fromSchemaIgnoreInvalid;

/**
 * An implementation that translates Avro Standard Logical types into Trino SPI types
 */
public class NativeLogicalTypesAvroTypeManager
        implements AvroTypeManager
{
    private static final Logger log = Logger.get(NativeLogicalTypesAvroTypeManager.class);

    public static final Schema TIMESTAMP_MILLIS_SCHEMA;
    public static final Schema TIMESTAMP_MICROS_SCHEMA;
    public static final Schema DATE_SCHEMA;
    public static final Schema TIME_MILLIS_SCHEMA;
    public static final Schema TIME_MICROS_SCHEMA;
    public static final Schema UUID_SCHEMA;

    // Copied from org.apache.avro.LogicalTypes
    protected static final String DECIMAL = "decimal";
    protected static final String UUID = "uuid";
    protected static final String DATE = "date";
    protected static final String TIME_MILLIS = "time-millis";
    protected static final String TIME_MICROS = "time-micros";
    protected static final String TIMESTAMP_MILLIS = "timestamp-millis";
    protected static final String TIMESTAMP_MICROS = "timestamp-micros";
    protected static final String LOCAL_TIMESTAMP_MILLIS = "local-timestamp-millis";
    protected static final String LOCAL_TIMESTAMP_MICROS = "local-timestamp-micros";

    static {
        TIMESTAMP_MILLIS_SCHEMA = SchemaBuilder.builder().longType();
        LogicalTypes.timestampMillis().addToSchema(TIMESTAMP_MILLIS_SCHEMA);
        TIMESTAMP_MICROS_SCHEMA = SchemaBuilder.builder().longType();
        LogicalTypes.timestampMicros().addToSchema(TIMESTAMP_MICROS_SCHEMA);
        DATE_SCHEMA = Schema.create(Schema.Type.INT);
        LogicalTypes.date().addToSchema(DATE_SCHEMA);
        TIME_MILLIS_SCHEMA = Schema.create(Schema.Type.INT);
        LogicalTypes.timeMillis().addToSchema(TIME_MILLIS_SCHEMA);
        TIME_MICROS_SCHEMA = Schema.create(Schema.Type.LONG);
        LogicalTypes.timeMicros().addToSchema(TIME_MICROS_SCHEMA);
        UUID_SCHEMA = Schema.create(Schema.Type.STRING);
        LogicalTypes.uuid().addToSchema(UUID_SCHEMA);
    }

    @Override
    public void configure(Map<String, byte[]> fileMetadata) {}

    @Override
    public Optional<Type> overrideTypeForSchema(Schema schema)
            throws AvroTypeException
    {
        return validateAndLogIssues(schema).map(NativeLogicalTypesAvroTypeManager::getAvroLogicalTypeSpiType);
    }

    @Override
    public Optional<BiConsumer<BlockBuilder, Object>> overrideBuildingFunctionForSchema(Schema schema)
            throws AvroTypeException
    {
        return validateAndLogIssues(schema).map(logicalType -> getLogicalTypeBuildingFunction(logicalType, schema));
    }

    @Override
    public Optional<BiFunction<Block, Integer, Object>> overrideBlockToAvroObject(Schema schema, Type type)
            throws AvroTypeException
    {
        Optional<LogicalType> logicalType = validateAndLogIssues(schema);
        if (logicalType.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(getAvroFunction(logicalType.get(), schema, type));
    }

    private static Type getAvroLogicalTypeSpiType(LogicalType logicalType)
    {
        return switch (logicalType.getName()) {
            case TIMESTAMP_MILLIS -> TimestampType.TIMESTAMP_MILLIS;
            case TIMESTAMP_MICROS -> TimestampType.TIMESTAMP_MICROS;
            case DECIMAL -> {
                LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
                yield DecimalType.createDecimalType(decimal.getPrecision(), decimal.getScale());
            }
            case DATE -> DateType.DATE;
            case TIME_MILLIS -> TimeType.TIME_MILLIS;
            case TIME_MICROS -> TimeType.TIME_MICROS;
            case UUID -> UuidType.UUID;
            default -> throw new IllegalStateException("Unreachable unfiltered logical type");
        };
    }

    private static BiConsumer<BlockBuilder, Object> getLogicalTypeBuildingFunction(LogicalType logicalType, Schema schema)
    {
        return switch (logicalType.getName()) {
            case TIMESTAMP_MILLIS -> {
                if (schema.getType() == Schema.Type.LONG) {
                    yield (builder, obj) -> {
                        Long l = (Long) obj;
                        TimestampType.TIMESTAMP_MILLIS.writeLong(builder, l * Timestamps.MICROSECONDS_PER_MILLISECOND);
                    };
                }
                throw new IllegalStateException("Unreachable unfiltered logical type");
            }
            case TIMESTAMP_MICROS -> {
                if (schema.getType() == Schema.Type.LONG) {
                    yield (builder, obj) -> {
                        Long l = (Long) obj;
                        TimestampType.TIMESTAMP_MICROS.writeLong(builder, l);
                    };
                }
                throw new IllegalStateException("Unreachable unfiltered logical type");
            }
            case DECIMAL -> {
                LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
                DecimalType decimalType = DecimalType.createDecimalType(decimal.getPrecision(), decimal.getScale());
                Function<Object, byte[]> byteExtract = switch (schema.getType()) {
                    case BYTES -> // This is only safe because we don't reuse byte buffer objects which means each gets sized exactly for the bytes contained
                            (obj) -> ((ByteBuffer) obj).array();
                    case FIXED -> (obj) -> ((GenericFixed) obj).bytes();
                    default -> throw new IllegalStateException("Unreachable unfiltered logical type");
                };
                if (decimalType.isShort()) {
                    yield (builder, obj) -> decimalType.writeLong(builder, fromBigEndian(byteExtract.apply(obj)));
                }
                else {
                    yield (builder, obj) -> decimalType.writeObject(builder, Int128.fromBigEndian(byteExtract.apply(obj)));
                }
            }
            case DATE -> {
                if (schema.getType() == Schema.Type.INT) {
                    yield (builder, obj) -> {
                        Integer i = (Integer) obj;
                        DateType.DATE.writeLong(builder, i.longValue());
                    };
                }
                throw new IllegalStateException("Unreachable unfiltered logical type");
            }
            case TIME_MILLIS -> {
                if (schema.getType() == Schema.Type.INT) {
                    yield (builder, obj) -> {
                        Integer i = (Integer) obj;
                        TimeType.TIME_MILLIS.writeLong(builder, i.longValue() * Timestamps.PICOSECONDS_PER_MILLISECOND);
                    };
                }
                throw new IllegalStateException("Unreachable unfiltered logical type");
            }
            case TIME_MICROS -> {
                if (schema.getType() == Schema.Type.LONG) {
                    yield (builder, obj) -> {
                        Long i = (Long) obj;
                        TimeType.TIME_MICROS.writeLong(builder, i * Timestamps.PICOSECONDS_PER_MICROSECOND);
                    };
                }
                throw new IllegalStateException("Unreachable unfiltered logical type");
            }
            case UUID -> {
                if (schema.getType() == Schema.Type.STRING) {
                    yield (builder, obj) -> UuidType.UUID.writeSlice(builder, javaUuidToTrinoUuid(java.util.UUID.fromString(obj.toString())));
                }
                throw new IllegalStateException("Unreachable unfiltered logical type");
            }
            default -> throw new IllegalStateException("Unreachable unfiltered logical type");
        };
    }

    private static BiFunction<Block, Integer, Object> getAvroFunction(LogicalType logicalType, Schema schema, Type type)
            throws AvroTypeException
    {
        return switch (logicalType.getName()) {
            case TIMESTAMP_MILLIS -> {
                if (!(type instanceof TimestampType timestampType)) {
                    throw new AvroTypeException("Can't represent Avro logical type %s with Trino Type %s".formatted(logicalType.getName(), type));
                }
                if (timestampType.isShort()) {
                    yield (block, integer) -> timestampType.getLong(block, integer) / Timestamps.MICROSECONDS_PER_MILLISECOND;
                }
                else {
                    yield ((block, integer) ->
                    {
                        SqlTimestamp timestamp = (SqlTimestamp) timestampType.getObject(block, integer);
                        return timestamp.roundTo(3).getMillis();
                    });
                }
            }
            case TIMESTAMP_MICROS -> {
                if (!(type instanceof TimestampType timestampType)) {
                    throw new AvroTypeException("Can't represent Avro logical type %s with Trino Type %s".formatted(logicalType.getName(), type));
                }
                if (timestampType.isShort()) {
                    // Don't use method reference because it causes an NPE in errorprone
                    yield (block, position) -> timestampType.getLong(block, position);
                }
                else {
                    yield ((block, position) ->
                    {
                        SqlTimestamp timestamp = (SqlTimestamp) timestampType.getObject(block, position);
                        return timestamp.roundTo(6).getEpochMicros();
                    });
                }
            }
            case DECIMAL -> {
                DecimalType decimalType = (DecimalType) getAvroLogicalTypeSpiType(logicalType);
                Function<byte[], Object> wrapBytes = switch (schema.getType()) {
                    case BYTES -> ByteBuffer::wrap;
                    case FIXED -> bytes -> new GenericData.Fixed(schema, fitBigEndianValueToByteArraySize(bytes, schema.getFixedSize()));
                    default -> throw new VerifyException("Unreachable unfiltered logical type");
                };
                if (decimalType.isShort()) {
                    yield (block, pos) -> wrapBytes.apply(Longs.toByteArray(decimalType.getLong(block, pos)));
                }
                else {
                    yield (block, pos) -> wrapBytes.apply(((Int128) decimalType.getObject(block, pos)).toBigEndianBytes());
                }
            }
            case DATE -> {
                if (type != DateType.DATE) {
                    throw new AvroTypeException("Can't represent Avro logical type %s with Trino Type %s".formatted(logicalType.getName(), type));
                }
                yield DateType.DATE::getLong;
            }
            case TIME_MILLIS -> {
                if (!(type instanceof TimeType timeType)) {
                    throw new AvroTypeException("Can't represent Avro logical type %s with Trino Type %s".formatted(logicalType.getName(), type));
                }
                if (timeType.getPrecision() > 3) {
                    throw new AvroTypeException("Can't write out Avro logical time-millis from Trino Time Type with precision %s".formatted(timeType.getPrecision()));
                }
                yield (block, pos) -> roundDiv(timeType.getLong(block, pos), Timestamps.PICOSECONDS_PER_MILLISECOND);
            }
            case TIME_MICROS -> {
                if (!(type instanceof TimeType timeType)) {
                    throw new AvroTypeException("Can't represent Avro logical type %s with Trino Type %s".formatted(logicalType.getName(), type));
                }
                if (timeType.getPrecision() > 6) {
                    throw new AvroTypeException("Can't write out Avro logical time-millis from Trino Time Type with precision %s".formatted(timeType.getPrecision()));
                }
                yield (block, pos) -> roundDiv(timeType.getLong(block, pos), Timestamps.PICOSECONDS_PER_MICROSECOND);
            }
            case UUID -> {
                if (!(type instanceof UuidType uuidType)) {
                    throw new AvroTypeException("Can't represent Avro logical type %s with Trino Type %s".formatted(logicalType.getName(), type));
                }
                yield (block, pos) -> trinoUuidToJavaUuid((Slice) uuidType.getObject(block, pos)).toString();
            }
            default -> throw new VerifyException("Unreachable unfiltered logical type");
        };
    }

    private Optional<LogicalType> validateAndLogIssues(Schema schema)
    {
        // TODO replace with switch sealed class syntax when stable
        ValidateLogicalTypeResult logicalTypeResult = validateLogicalType(schema);
        if (logicalTypeResult instanceof NoLogicalType ignored) {
            return Optional.empty();
        }
        if (logicalTypeResult instanceof NonNativeAvroLogicalType ignored) {
            log.debug("Unrecognized logical type " + schema);
            return Optional.empty();
        }
        if (logicalTypeResult instanceof InvalidNativeAvroLogicalType invalidNativeAvroLogicalType) {
            log.debug(invalidNativeAvroLogicalType.getCause(), "Invalidly configured native Avro logical type");
            return Optional.empty();
        }
        if (logicalTypeResult instanceof ValidNativeAvroLogicalType validNativeAvroLogicalType) {
            return Optional.of(validNativeAvroLogicalType.getLogicalType());
        }
        throw new IllegalStateException("Unhandled validate logical type result");
    }

    protected static ValidateLogicalTypeResult validateLogicalType(Schema schema)
    {
        final String typeName = schema.getProp(LogicalType.LOGICAL_TYPE_PROP);
        if (typeName == null) {
            return new NoLogicalType();
        }
        LogicalType logicalType;
        switch (typeName) {
            case TIMESTAMP_MILLIS, TIMESTAMP_MICROS, DECIMAL, DATE, TIME_MILLIS, TIME_MICROS, UUID:
                logicalType = fromSchemaIgnoreInvalid(schema);
                break;
            case LOCAL_TIMESTAMP_MICROS + LOCAL_TIMESTAMP_MILLIS:
                log.debug("Logical type " + typeName + " not currently supported by by Trino");
                // fall through
            default:
                return new NonNativeAvroLogicalType(typeName);
        }
        // make sure the type is valid before returning it
        if (logicalType != null) {
            try {
                logicalType.validate(schema);
            }
            catch (RuntimeException e) {
                return new InvalidNativeAvroLogicalType(typeName, e);
            }
            return new ValidNativeAvroLogicalType(logicalType);
        }
        else {
            return new NonNativeAvroLogicalType(typeName);
        }
    }

    protected abstract static sealed class ValidateLogicalTypeResult
            permits NoLogicalType, NonNativeAvroLogicalType, InvalidNativeAvroLogicalType, ValidNativeAvroLogicalType {}

    protected static final class NoLogicalType
            extends ValidateLogicalTypeResult {}

    protected static final class NonNativeAvroLogicalType
            extends ValidateLogicalTypeResult
    {
        private final String logicalTypeName;

        public NonNativeAvroLogicalType(String logicalTypeName)
        {
            this.logicalTypeName = requireNonNull(logicalTypeName, "logicalTypeName is null");
        }

        public String getLogicalTypeName()
        {
            return logicalTypeName;
        }
    }

    protected static final class InvalidNativeAvroLogicalType
            extends ValidateLogicalTypeResult
    {
        private final String logicalTypeName;
        private final RuntimeException cause;

        public InvalidNativeAvroLogicalType(String logicalTypeName, RuntimeException cause)
        {
            this.logicalTypeName = requireNonNull(logicalTypeName, "logicalTypeName");
            this.cause = requireNonNull(cause, "cause is null");
        }

        public String getLogicalTypeName()
        {
            return logicalTypeName;
        }

        public RuntimeException getCause()
        {
            return cause;
        }
    }

    protected static final class ValidNativeAvroLogicalType
            extends ValidateLogicalTypeResult
    {
        private final LogicalType logicalType;

        public ValidNativeAvroLogicalType(LogicalType logicalType)
        {
            this.logicalType = requireNonNull(logicalType, "logicalType is null");
        }

        public LogicalType getLogicalType()
        {
            return logicalType;
        }
    }

    private static final VarHandle BIG_ENDIAN_LONG_VIEW = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    /**
     * Decode a long from the two's complement big-endian representation.
     *
     * @param bytes the two's complement big-endian encoding of the number. It must contain at least 1 byte.
     * It may contain more than 8 bytes if the leading bytes are not significant (either zeros or -1)
     * @throws ArithmeticException if the bytes represent a number outside the range [-2^63, 2^63 - 1]
     */
    // Styled from io.trino.spi.type.Int128.fromBigEndian
    public static long fromBigEndian(byte[] bytes)
    {
        if (bytes.length > 8) {
            int offset = bytes.length - Long.BYTES;
            long res = (long) BIG_ENDIAN_LONG_VIEW.get(bytes, offset);
            // verify that the significant bits above 64 bits are proper sign extension
            int expectedSignExtensionByte = (int) (res >> 63);
            for (int i = 0; i < offset; i++) {
                if (bytes[i] != expectedSignExtensionByte) {
                    throw new ArithmeticException("Overflow");
                }
            }
            return res;
        }
        if (bytes.length == 8) {
            return (long) BIG_ENDIAN_LONG_VIEW.get(bytes, 0);
        }
        long res = (bytes[0] >> 7);
        for (byte b : bytes) {
            res = (res << 8) | (b & 0xFF);
        }
        return res;
    }

    public static byte[] fitBigEndianValueToByteArraySize(long value, int byteSize)
    {
        return fitBigEndianValueToByteArraySize(Longs.toByteArray(value), byteSize);
    }

    public static byte[] fitBigEndianValueToByteArraySize(Int128 value, int byteSize)
    {
        return fitBigEndianValueToByteArraySize(value.toBigEndianBytes(), byteSize);
    }

    /**
     * Will resize big endian bytes to a desired array length while preserving the represented value.
     *
     * @throws ArithmeticException if conversion is not possible
     */
    public static byte[] fitBigEndianValueToByteArraySize(byte[] value, int byteSize)
    {
        if (value.length == byteSize) {
            return value;
        }
        if (value.length < byteSize) {
            return padBigEndianToSize(value, byteSize);
        }
        if (canBigEndianValueBeRepresentedBySmallerByteSize(value, byteSize)) {
            byte[] dest = new byte[byteSize];
            System.arraycopy(value, value.length - byteSize, dest, 0, byteSize);
            return dest;
        }
        throw new ArithmeticException("Can't resize big endian bytes %s to size %s".formatted(Arrays.toString(value), byteSize));
    }

    private static boolean canBigEndianValueBeRepresentedBySmallerByteSize(byte[] bigEndianValue, int byteSize)
    {
        verify(byteSize < bigEndianValue.length);
        // pre-req 1
        // can't represent number with 0 bytes
        if (byteSize <= 0) {
            return false;
        }
        // pre-req 2
        // these are the only padding bytes, if they aren't in the most sig bits place, then all bytes matter
        // and a down-size isn't possible
        if (bigEndianValue[0] != 0 && bigEndianValue[0] != -1) {
            return false;
        }
        // the first significant byte is either the first byte that is consistent with the sign of the padding
        // or the last padding byte when the next byte is inconsistent with the sign
        int firstSigByte = 0;
        byte padding = bigEndianValue[0];
        for (int i = 1; i < bigEndianValue.length; i++) {
            if (bigEndianValue[i] == padding) {
                firstSigByte = i;
            }
            // case 1
            else if (padding == 0 && bigEndianValue[i] < 0) {
                break;
            }
            // case 2
            else if (padding == 0 && bigEndianValue[i] > 0) {
                firstSigByte = i;
                break;
            }
            // case 3
            else if (padding == -1 && bigEndianValue[i] >= 0) {
                break;
            }
            // case 4
            else if (padding == -1 && bigEndianValue[i] < 0) {
                firstSigByte = i;
                break;
            }
        }
        return (bigEndianValue.length - firstSigByte) <= byteSize;
    }

    public static byte[] padBigEndianToSize(Int128 toPad, int byteSize)
    {
        return padBigEndianToSize(toPad.toBigEndianBytes(), byteSize);
    }

    public static byte[] padBigEndianToSize(long toPad, int byteSize)
    {
        return padBigEndianToSize(Longs.toByteArray(toPad), byteSize);
    }

    public static byte[] padBigEndianToSize(byte[] toPad, int byteSize)
    {
        int endianSize = toPad.length;
        if (byteSize < endianSize) {
            throw new ArithmeticException("Big endian bytes size must be less than or equal to the total padded size");
        }
        if (endianSize < 1) {
            throw new ArithmeticException("Cannot pad empty array");
        }
        byte[] padded = new byte[byteSize];
        System.arraycopy(toPad, 0, padded, byteSize - endianSize, endianSize);
        if (toPad[0] < 0) {
            for (int i = 0; i < byteSize - endianSize; i++) {
                padded[i] = -1;
            }
        }
        return padded;
    }
}
