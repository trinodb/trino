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
package io.trino.testing.datatype;

import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static java.lang.String.format;

/**
 * A helper class aiding migration from {@link DataTypeTest} to {@link SqlDataTypeTest}.
 * Replace call to {@link DataTypeTest#create()} with {@link DataTypeTestToSqlDataTypeTestConverter#create()} to have your test code generated.
 */
public final class DataTypeTestToSqlDataTypeTestConverter
{
    public static DataTypeTestToSqlDataTypeTestConverter create()
    {
        return new DataTypeTestToSqlDataTypeTestConverter();
    }

    private StringBuilder output = new StringBuilder();

    private DataTypeTestToSqlDataTypeTestConverter()
    {
        output.append("SqlDataTypeTest.create()\n");
    }

    public <T> DataTypeTestToSqlDataTypeTestConverter addRoundTrip(DataType<T> dataType, T value)
    {
        output.append(format(
                " .addRoundTrip(%s, %s, %s, %s)",
                toJavaLiteral(dataType.getInsertType()),
                toJavaLiteral(dataType.toLiteral(value)),
                typeConstructor(dataType.getTrinoResultType()),
                toJavaLiteral(dataType.toTrinoLiteral(value))));

        Object expected = dataType.toTrinoQueryResult(value);
        if (expected != value) {
            output.append(format(" // TODO non-identity toTrinoQueryResult function was used: %s vs %s", value, expected));
        }

        output.append("\n");

        return this;
    }

    private String typeConstructor(Type type)
    {
        if (type instanceof DecimalType decimalType) {
            return format("createDecimalType(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
        }
        if (type instanceof CharType charType) {
            return format("createCharType(%s)", charType.getLength());
        }
        if (type instanceof VarcharType varcharType) {
            if (varcharType.isUnbounded()) {
                return "createUnboundedVarcharType()";
            }
            return format("createVarcharType(%s)", varcharType.getBoundedLength());
        }
        if (type instanceof TimeType timeType) {
            return format("createTimeType(%s)", timeType.getPrecision());
        }
        if (type instanceof TimeWithTimeZoneType timeWithTimeZoneType) {
            return format("createTimeWithTimeZoneType(%s)", timeWithTimeZoneType.getPrecision());
        }
        if (type instanceof TimestampType timestampType) {
            return format("createTimestampType(%s)", timestampType.getPrecision());
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            return format("createTimestampWithTimeZoneType(%s)", timestampWithTimeZoneType.getPrecision());
        }

        try {
            for (Field field : type.getClass().getFields()) {
                if (field.getName().matches("^[A-Z_]+$") &&
                        Modifier.isStatic(field.getModifiers()) &&
                        field.get(null) == type) {
                    return field.getName();
                }
            }
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }

        return type.getDisplayName();
    }

    public DataTypeTestToSqlDataTypeTestConverter execute(Object... args)
    {
        output.append(" .execute( ... ) // TODO\n");
        throw new RuntimeException("Code replacement:\n" + output);
    }

    private static String toJavaLiteral(String value)
    {
        if (value.contains("\\") || value.contains("\"") || value.contains("\n")) {
            throw new IllegalArgumentException(format("Unsupported value: [%s]", value));
        }
        return format("\"%s\"", value);
    }
}
