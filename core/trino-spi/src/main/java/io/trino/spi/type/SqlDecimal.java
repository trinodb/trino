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
package io.trino.spi.type;

import com.fasterxml.jackson.annotation.JsonValue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.Objects;

public final class SqlDecimal
{
    private final BigInteger unscaledValue;
    private final int precision;
    private final int scale;

    public SqlDecimal(BigInteger unscaledValue, int precision, int scale)
    {
        this.unscaledValue = unscaledValue;
        this.precision = precision;
        this.scale = scale;
    }

    public static SqlDecimal decimal(String value, DecimalType type)
    {
        DecimalParseResult parseResult = Decimals.parse(value);
        BigInteger unscaledValue;
        if (parseResult.getType().isShort()) {
            unscaledValue = BigInteger.valueOf((Long) parseResult.getObject());
        }
        else {
            unscaledValue = ((Int128) parseResult.getObject()).toBigInteger();
        }

        return new SqlDecimal(unscaledValue, type.getPrecision(), type.getScale());
    }

    public int getPrecision()
    {
        return precision;
    }

    public int getScale()
    {
        return scale;
    }

    public static SqlDecimal of(long unscaledValue, int precision, int scale)
    {
        return new SqlDecimal(BigInteger.valueOf(unscaledValue), precision, scale);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SqlDecimal that = (SqlDecimal) o;
        return precision == that.precision && scale == that.scale && unscaledValue.equals(that.unscaledValue);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(unscaledValue, precision, scale);
    }

    @JsonValue
    @Override
    public String toString()
    {
        return Decimals.toString(unscaledValue, scale);
    }

    public BigDecimal toBigDecimal()
    {
        return new BigDecimal(unscaledValue, scale, new MathContext(precision));
    }

    public BigInteger getUnscaledValue()
    {
        return unscaledValue;
    }
}
