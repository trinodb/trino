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
package io.trino.plugin.trino;

import io.airlift.slice.Slices;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.NumberType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longTimestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

/**
 * Owns both JDBC parameter serialization and the SQL expression surrounding a
 * renderer-emitted placeholder. Keeping these decisions together prevents a
 * value serialized as VARCHAR from being used as an untyped {@code ?}.
 */
final class TrinoParameterBindingFactory
{
    private TrinoParameterBindingFactory() {}

    static Optional<WriteMapping> createWriteMapping(Type type)
    {
        requireNonNull(type, "type is null");

        if (type == BOOLEAN) {
            return Optional.of(WriteMapping.booleanMapping("boolean", (statement, index, value) -> statement.setBoolean(index, value)));
        }
        if (type == TINYINT) {
            return Optional.of(WriteMapping.longMapping("tinyint", (statement, index, value) -> statement.setByte(index, (byte) value)));
        }
        if (type == SMALLINT) {
            return Optional.of(WriteMapping.longMapping("smallint", (statement, index, value) -> statement.setShort(index, (short) value)));
        }
        if (type == INTEGER) {
            return Optional.of(WriteMapping.longMapping("integer", (statement, index, value) -> statement.setInt(index, (int) value)));
        }
        if (type == BIGINT) {
            return Optional.of(WriteMapping.longMapping("bigint", (statement, index, value) -> statement.setLong(index, value)));
        }
        if (type == REAL) {
            return Optional.of(WriteMapping.longMapping("real", (statement, index, value) -> statement.setFloat(index, Float.intBitsToFloat((int) value))));
        }
        if (type == DOUBLE) {
            return Optional.of(WriteMapping.doubleMapping("double", (statement, index, value) -> statement.setDouble(index, value)));
        }
        if (type instanceof DecimalType decimalType) {
            String dataType = "decimal(" + decimalType.getPrecision() + "," + decimalType.getScale() + ")";
            if (decimalType.isShort()) {
                return Optional.of(WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType)));
            }
            return Optional.of(WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType)));
        }
        if (type instanceof NumberType) {
            return Optional.of(TrinoNumberCodec.numberWriteMapping());
        }
        if (type instanceof CharType || type instanceof VarcharType) {
            return Optional.of(WriteMapping.sliceMapping("varchar", (statement, index, value) -> statement.setString(index, value.toStringUtf8())));
        }
        if (type instanceof VarbinaryType) {
            return Optional.of(WriteMapping.sliceMapping("varbinary", (statement, index, value) -> statement.setBytes(index, value.getBytes())));
        }
        if (type instanceof DateType) {
            return Optional.of(WriteMapping.longMapping("date", dateWriteFunctionUsingLocalDate()));
        }
        if (type instanceof TimeType timeType) {
            return Optional.of(WriteMapping.longMapping(
                    "time(" + timeType.getPrecision() + ")",
                    TemporalTransportCodec.timeTransportWriteFunction(timeType)));
        }
        if (type instanceof TimestampType timestampType) {
            if (timestampType.isShort()) {
                return Optional.of(WriteMapping.longMapping(
                        "timestamp(" + timestampType.getPrecision() + ")",
                        timestampWriteFunction(timestampType)));
            }
            if (timestampType.getPrecision() <= 9) {
                return Optional.of(WriteMapping.objectMapping(
                        "timestamp(" + timestampType.getPrecision() + ")",
                        longTimestampWriteFunction(timestampType, timestampType.getPrecision())));
            }
            return Optional.of(TemporalTransportCodec.timestampWriteMapping(timestampType));
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            String dataType = timestampWithTimeZoneType.getDisplayName();
            if (timestampWithTimeZoneType.isShort()) {
                return Optional.of(WriteMapping.longMapping(dataType, TimestampWithTimeZoneTransport.shortPredicateWriteFunction(timestampWithTimeZoneType)));
            }
            return Optional.of(WriteMapping.objectMapping(dataType, TimestampWithTimeZoneTransport.longPredicateWriteFunction(timestampWithTimeZoneType)));
        }
        if (type instanceof TimeWithTimeZoneType timeWithTimeZoneType) {
            return Optional.of(TemporalTransportCodec.timeWithTimeZoneWriteMapping(timeWithTimeZoneType));
        }
        if (TrinoTypeClassifier.isIntervalYearToMonthType(type) || TrinoTypeClassifier.isIntervalDayToSecondType(type)) {
            return Optional.of(TemporalTransportCodec.intervalWriteMapping(type));
        }
        return Optional.empty();
    }

    static Optional<ParameterizedExpression> bindConstant(Constant constant)
    {
        requireNonNull(constant, "constant is null");

        Type type = constant.getType();
        Object value = constant.getValue();
        if (value == null) {
            return Optional.of(new ParameterizedExpression("CAST(NULL AS " + type.getDisplayName() + ")", List.of()));
        }

        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            String zoneId = timestampWithTimeZoneType.isShort()
                    ? TimestampWithTimeZoneTransport.shortZoneId((long) value)
                    : TimestampWithTimeZoneTransport.longZoneId((LongTimestampWithTimeZone) value);
            return Optional.of(new ParameterizedExpression(
                    "at_timezone(CAST(? AS " + type.getDisplayName() + "), CAST(? AS varchar))",
                    List.of(
                            new QueryParameter(type, Optional.of(value)),
                            new QueryParameter(VARCHAR, Optional.of(Slices.utf8Slice(zoneId))))));
        }

        return createWriteMapping(type)
                .map(mapping -> new ParameterizedExpression(
                        typedBindExpression(type, mapping),
                        List.of(new QueryParameter(type, Optional.of(value)))));
    }

    private static String typedBindExpression(Type type, WriteMapping mapping)
    {
        String bindExpression = mapping.getWriteFunction().getBindExpression();
        String logicalTypeCast = "CAST(? AS " + type.getDisplayName() + ")";
        if (bindExpression.equals(logicalTypeCast)) {
            return bindExpression;
        }
        return "CAST(" + bindExpression + " AS " + type.getDisplayName() + ")";
    }
}
