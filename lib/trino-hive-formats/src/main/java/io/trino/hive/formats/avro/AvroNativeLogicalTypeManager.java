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

import io.airlift.log.Logger;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericFixed;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static java.util.Objects.requireNonNull;
import static org.apache.avro.LogicalTypes.fromSchemaIgnoreInvalid;

/**
 * An implementation that translates Avro Standard Logical types into Trino SPI types
 */
public class AvroNativeLogicalTypeManager
        extends AvroTypeManager
{
    private static final Logger log = Logger.get(AvroNativeLogicalTypeManager.class);

    public static final Schema TIMESTAMP_MILLI_SCHEMA;
    public static final Schema TIMESTAMP_MICRO_SCHEMA;
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
        TIMESTAMP_MILLI_SCHEMA = SchemaBuilder.builder().longType();
        LogicalTypes.timestampMillis().addToSchema(TIMESTAMP_MILLI_SCHEMA);
        TIMESTAMP_MICRO_SCHEMA = SchemaBuilder.builder().longType();
        LogicalTypes.timestampMicros().addToSchema(TIMESTAMP_MICRO_SCHEMA);
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
    public void configure(Map<String, byte[]> fileMetaData)
    {
    }

    // Heavily borrow from org.apache.avro.LogicalTypes#fromSchemaImpl(org.apache.avro.Schema, boolean)
    @Override
    public Optional<Type> overrideTypeForSchema(Schema schema)
    {
        return validateAndProduceFromName(schema, logicalType -> switch (logicalType.getName()) {
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
            default -> throw new IllegalStateException("Unreachable unfilted logical type");
        });
    }

    @Override
    public Optional<BiConsumer<BlockBuilder, Object>> overrideBuildingFunctionForSchema(Schema schema)
    {
        return validateAndProduceFromName(schema, logicalType -> switch (logicalType.getName()) {
            case TIMESTAMP_MILLIS -> switch (schema.getType()) {
                case LONG -> {
                    yield (builder, obj) -> {
                        Long l = (Long) obj;
                        TimestampType.TIMESTAMP_MILLIS.writeLong(builder, l * 1000);
                    };
                }
                default -> throw new IllegalStateException("Unreachable unfiltered logical type");
            };
            case TIMESTAMP_MICROS -> switch (schema.getType()) {
                case LONG -> {
                    yield (builder, obj) -> {
                        Long l = (Long) obj;
                        TimestampType.TIMESTAMP_MICROS.writeLong(builder, l);
                    };
                }
                default -> throw new IllegalStateException("Unreachable unfiltered logical type");
            };
            case DECIMAL -> {
                LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
                DecimalType decimalType = DecimalType.createDecimalType(decimal.getPrecision(), decimal.getScale());
                Function<Object, byte[]> byteExtract = switch (schema.getType()) {
                    case BYTES -> {
                        // This is only safe because we don't reuse byte buffer objects which means each gets sized exactly for the bytes contained
                        yield (obj) -> ((ByteBuffer) obj).array();
                    }
                    case FIXED -> {
                        yield (obj) -> ((GenericFixed) obj).bytes();
                    }
                    default -> throw new IllegalStateException("Unreachable unfiltered logical type");
                };
                // TODO replace with switch sealed class syntax when stable
                if (decimalType instanceof io.trino.spi.type.LongDecimalType longDecimalType) {
                    yield (builder, obj) -> {
                        longDecimalType.writeObject(builder, Int128.fromBigEndian(byteExtract.apply(obj)));
                    };
                }
                else if (decimalType instanceof io.trino.spi.type.ShortDecimalType shortDecimalType) {
                    yield (builder, obj) -> {
                        shortDecimalType.writeLong(builder, fromBigEndian(byteExtract.apply(obj)));
                    };
                }
                else {
                    throw new IllegalStateException("Unhandled decimal type");
                }
            }
            case DATE -> switch (schema.getType()) {
                case INT -> {
                    yield (builder, obj) -> {
                        Integer i = (Integer) obj;
                        DateType.DATE.writeLong(builder, i.longValue());
                    };
                }
                default -> throw new IllegalStateException("Unreachable unfiltered logical type");
            };
            case TIME_MILLIS -> switch (schema.getType()) {
                case INT -> {
                    yield (builder, obj) -> {
                        Integer i = (Integer) obj;
                        TimeType.TIME_MILLIS.writeLong(builder, i.longValue() * 1_000_000_000);
                    };
                }
                default -> throw new IllegalStateException("Unreachable unfiltered logical type");
            };
            case TIME_MICROS -> switch (schema.getType()) {
                case LONG -> {
                    yield (builder, obj) -> {
                        Long i = (Long) obj;
                        // convert to picos
                        TimeType.TIME_MICROS.writeLong(builder, i * 1_000_000);
                    };
                }
                default -> throw new IllegalStateException("Unreachable unfiltered logical type");
            };
            case UUID -> switch (schema.getType()) {
                case STRING -> {
                    yield (builder, obj) -> {
                        UuidType.UUID.writeSlice(builder, javaUuidToTrinoUuid(java.util.UUID.fromString(obj.toString())));
                    };
                }
                default -> throw new IllegalStateException("Unreachable unfiltered logical type");
            };
            default -> throw new IllegalStateException("Unreachable unfiltered logical type");
        });
    }

    protected abstract sealed class ValidateLogicalTypeResult
            permits NoLogicalType, NonNativeAvroLogicalType, InvalidNativeAvroLogicalType, ValidNativeAvroLogicalType {}

    protected final class NoLogicalType
            extends ValidateLogicalTypeResult {}

    protected final class NonNativeAvroLogicalType
            extends ValidateLogicalTypeResult
    {
        protected final String logicalTypeName;

        public NonNativeAvroLogicalType(String logicalTypeName)
        {
            this.logicalTypeName = requireNonNull(logicalTypeName, "logicalTypeName is null");
        }

        public String getLogicalTypeName()
        {
            return logicalTypeName;
        }
    }

    protected final class InvalidNativeAvroLogicalType
            extends ValidateLogicalTypeResult
    {
        protected final String logicalTypeName;
        protected final RuntimeException cause;

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

    protected final class ValidNativeAvroLogicalType
            extends ValidateLogicalTypeResult
    {
        protected final LogicalType logicalType;

        public ValidNativeAvroLogicalType(LogicalType logicalType)
        {
            this.logicalType = requireNonNull(logicalType, "logicalType is null");
        }

        public LogicalType getLogicalType()
        {
            return logicalType;
        }
    }

    private <T> Optional<T> validateAndProduceFromName(Schema schema, Function<LogicalType, T> produce)
    {
        // TODO replace with swtich sealed class syntax when stable
        ValidateLogicalTypeResult logicalTypeResult = validateLogicalType(schema);
        if (logicalTypeResult instanceof NoLogicalType ignored) {
            return Optional.empty();
        }
        if (logicalTypeResult instanceof NonNativeAvroLogicalType ignored) {
            log.warn("Unrecognized logical type " + schema);
            return Optional.empty();
        }
        if (logicalTypeResult instanceof InvalidNativeAvroLogicalType invalidNativeAvroLogicalType) {
            log.error("Invalidly configured native avro logical type", invalidNativeAvroLogicalType.cause);
            return Optional.empty();
        }
        if (logicalTypeResult instanceof ValidNativeAvroLogicalType validNativeAvroLogicalType) {
            return Optional.of(produce.apply(validNativeAvroLogicalType.logicalType));
        }
        throw new IllegalStateException("Unhandled validate logical type result");
    }

    protected ValidateLogicalTypeResult validateLogicalType(Schema schema)
    {
        final String typeName = schema.getProp(LogicalType.LOGICAL_TYPE_PROP);
        if (typeName == null) {
            return new NoLogicalType();
        }
        LogicalType logicalType = null;
        switch (typeName) {
            case TIMESTAMP_MILLIS, TIMESTAMP_MICROS, DECIMAL, DATE, TIME_MILLIS, TIME_MICROS, UUID:
                logicalType = fromSchemaIgnoreInvalid(schema);
                break;
            case LOCAL_TIMESTAMP_MICROS + LOCAL_TIMESTAMP_MILLIS:
                log.warn("Logical type " + typeName + " not currently supported by by Trino");
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

    private static final VarHandle BIG_ENDIAN_LONG_VIEW = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    /**
     * Decode a long from the two's complement big-endian representation.
     *
     * @param bytes the two's complement big-endian encoding of the number. It must contain at least 1 byte.
     * It may contain more than 8 bytes if the leading bytes are not significant (either zeros or -1)
     * @throws ArithmeticException if the bytes represent a number outside of the range [-2^63, 2^63 - 1]
     */
    // Styled from io.trino.spi.type.Int128.fromBigEndian
    // TODO put this where?
    public static long fromBigEndian(byte[] bytes)
    {
        if (bytes.length > 8) {
            int offset = bytes.length - Long.BYTES;
            long res = (long) BIG_ENDIAN_LONG_VIEW.get(bytes, offset);
            // verify that the bytes above 64 bits are simply proper sign extension
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
        for (int i = 0; i < bytes.length; i++) {
            res = (res << 8) | (bytes[i] & 0xFF);
        }
        return res;
    }
}
