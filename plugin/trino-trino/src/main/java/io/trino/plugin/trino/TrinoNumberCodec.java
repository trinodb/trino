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

import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.spi.type.NumberType;
import io.trino.spi.type.SqlNumber;
import io.trino.spi.type.TrinoNumber;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.SQLException;

final class TrinoNumberCodec
{
    private TrinoNumberCodec() {}

    static ColumnMapping numberColumnMapping()
    {
        return ColumnMapping.objectMapping(
                NumberType.NUMBER,
                ObjectReadFunction.of(TrinoNumber.class, (rs, idx) -> parse(rs.getString(idx))),
                numberWriteFunction());
    }

    static WriteMapping numberWriteMapping()
    {
        return WriteMapping.objectMapping(NumberType.NAME, numberWriteFunction());
    }

    static TrinoNumber parse(String value)
    {
        if (value == null) {
            return null;
        }
        return switch (value) {
            case "NaN" -> TrinoNumber.from(new TrinoNumber.NotANumber());
            case "+Infinity", "Infinity" -> TrinoNumber.from(new TrinoNumber.Infinity(false));
            case "-Infinity" -> TrinoNumber.from(new TrinoNumber.Infinity(true));
            default -> TrinoNumber.from(new BigDecimal(value));
        };
    }

    static TrinoNumber toTrinoNumber(Object value)
    {
        if (value == null) {
            return null;
        }
        if (value instanceof TrinoNumber trinoNumber) {
            return trinoNumber;
        }
        if (value instanceof SqlNumber sqlNumber) {
            return TrinoNumber.from(sqlNumber.value());
        }
        if (value instanceof BigDecimal bigDecimal) {
            return TrinoNumber.from(bigDecimal);
        }
        if (value instanceof BigInteger bigInteger) {
            return TrinoNumber.from(new BigDecimal(bigInteger));
        }
        if (value instanceof Float floatValue) {
            if (floatValue.isNaN()) {
                return TrinoNumber.from(new TrinoNumber.NotANumber());
            }
            if (floatValue.isInfinite()) {
                return TrinoNumber.from(new TrinoNumber.Infinity(floatValue < 0));
            }
            return TrinoNumber.from(new BigDecimal(floatValue.toString()));
        }
        if (value instanceof Double doubleValue) {
            if (doubleValue.isNaN()) {
                return TrinoNumber.from(new TrinoNumber.NotANumber());
            }
            if (doubleValue.isInfinite()) {
                return TrinoNumber.from(new TrinoNumber.Infinity(doubleValue < 0));
            }
            return TrinoNumber.from(new BigDecimal(doubleValue.toString()));
        }
        if (value instanceof Number number) {
            return TrinoNumber.from(new BigDecimal(number.toString()));
        }
        return parse(value.toString());
    }

    static String format(TrinoNumber value)
    {
        return value.toBigDecimal().toString();
    }

    static ObjectWriteFunction numberWriteFunction()
    {
        return new ObjectWriteFunction()
        {
            @Override
            public Class<?> getJavaType()
            {
                return TrinoNumber.class;
            }

            @Override
            public String getBindExpression()
            {
                return "CAST(? AS " + NumberType.NAME + ")";
            }

            @Override
            public void set(PreparedStatement statement, int index, Object value)
                    throws SQLException
            {
                statement.setString(index, format((TrinoNumber) value));
            }
        };
    }
}
