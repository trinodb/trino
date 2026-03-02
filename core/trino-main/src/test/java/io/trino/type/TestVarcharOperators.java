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
package io.trino.type;

import io.trino.spi.type.NumberType;
import io.trino.spi.type.SqlNumber;
import io.trino.spi.type.TrinoNumber;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.lang.reflect.Field;
import java.math.BigDecimal;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.IDENTICAL;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestVarcharOperators
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testLiteral()
    {
        assertThat(assertions.expression("'foo'"))
                .hasType(createVarcharType(3))
                .isEqualTo("foo");

        assertThat(assertions.expression("'bar'"))
                .hasType(createVarcharType(3))
                .isEqualTo("bar");

        assertThat(assertions.expression("''"))
                .hasType(createVarcharType(0))
                .isEqualTo("");
    }

    @Test
    public void testTypeConstructor()
    {
        assertThat(assertions.expression("VARCHAR 'foo'"))
                .hasType(VARCHAR)
                .isEqualTo("foo");

        assertThat(assertions.expression("VARCHAR 'bar'"))
                .hasType(VARCHAR)
                .isEqualTo("bar");

        assertThat(assertions.expression("VARCHAR ''"))
                .hasType(VARCHAR)
                .isEqualTo("");
    }

    @Test
    public void testVarcharCast()
    {
        assertThat(assertions.expression("CAST(VARCHAR '1.0' AS DOUBLE)"))
                .hasType(DOUBLE)
                .isEqualTo(1.0d);

        assertThat(assertions.expression("CAST(VARCHAR ' 1.0 ' AS DOUBLE)"))
                .hasType(DOUBLE)
                .isEqualTo(1.0d);

        assertThat(assertions.expression("CAST(VARCHAR '13.37' AS REAL)"))
                .hasType(REAL)
                .isEqualTo(13.37f);

        assertThat(assertions.expression("CAST(VARCHAR ' 13.37 ' AS REAL)"))
                .hasType(REAL)
                .isEqualTo(13.37f);

        assertThat(assertions.expression("CAST(VARCHAR '1337' AS BIGINT)"))
                .hasType(BIGINT)
                .isEqualTo(1337L);

        assertThat(assertions.expression("CAST(VARCHAR ' 1337 ' AS BIGINT)"))
                .hasType(BIGINT)
                .isEqualTo(1337L);

        assertThat(assertions.expression("CAST(VARCHAR '1337' AS INTEGER)"))
                .hasType(INTEGER)
                .isEqualTo(1337);

        assertThat(assertions.expression("CAST(VARCHAR ' 1337 ' AS INTEGER)"))
                .hasType(INTEGER)
                .isEqualTo(1337);

        assertThat(assertions.expression("CAST(VARCHAR '1337' AS SMALLINT)"))
                .hasType(SMALLINT)
                .isEqualTo((short) 1337);

        assertThat(assertions.expression("CAST(VARCHAR ' 1337 ' AS SMALLINT)"))
                .hasType(SMALLINT)
                .isEqualTo((short) 1337);

        assertThat(assertions.expression("CAST(VARCHAR '21' AS TINYINT)"))
                .hasType(TINYINT)
                .isEqualTo((byte) 21);

        assertThat(assertions.expression("CAST(VARCHAR ' 21 ' AS TINYINT)"))
                .hasType(TINYINT)
                .isEqualTo((byte) 21);

        assertThat(assertions.expression("CAST(VARCHAR '13.37' AS NUMBER)"))
                .hasType(NUMBER)
                .isEqualTo(new SqlNumber("13.37"));

        assertThat(assertions.expression("CAST(VARCHAR ' 13.37 ' AS NUMBER)"))
                .hasType(NUMBER)
                .isEqualTo(new SqlNumber("13.37"));
    }

    @Test
    public void testAdd()
    {
        // TODO change expected return type to createVarcharType(6) when function resolving is fixed
        assertThat(assertions.expression("a || b")
                .binding("a", "'foo'")
                .binding("b", "'foo'"))
                .hasType(VARCHAR)
                .isEqualTo("foofoo");

        assertThat(assertions.expression("a || b")
                .binding("a", "'foo'")
                .binding("b", "'bar'"))
                .hasType(VARCHAR)
                .isEqualTo("foobar");

        assertThat(assertions.expression("a || b")
                .binding("a", "'bar'")
                .binding("b", "'foo'"))
                .hasType(VARCHAR)
                .isEqualTo("barfoo");

        assertThat(assertions.expression("a || b")
                .binding("a", "'bar'")
                .binding("b", "'bar'"))
                .hasType(VARCHAR)
                .isEqualTo("barbar");

        assertThat(assertions.expression("a || b")
                .binding("a", "'bar'")
                .binding("b", "'barbaz'"))
                .hasType(VARCHAR)
                .isEqualTo("barbarbaz");
    }

    @Test
    public void testEqual()
    {
        assertThat(assertions.operator(EQUAL, "'foo'", "'foo'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "'foo'", "'bar'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "'bar'", "'foo'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "'bar'", "'bar'"))
                .isEqualTo(true);
    }

    @Test
    public void testNotEqual()
    {
        assertThat(assertions.expression("a <> b")
                .binding("a", "'foo'")
                .binding("b", "'foo'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "'foo'")
                .binding("b", "'bar'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "'bar'")
                .binding("b", "'foo'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "'bar'")
                .binding("b", "'bar'"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThan()
    {
        assertThat(assertions.operator(LESS_THAN, "'foo'", "'foo'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "'foo'", "'bar'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "'bar'", "'foo'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "'bar'", "'bar'"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "'foo'", "'foo'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "'foo'", "'bar'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "'bar'", "'foo'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "'bar'", "'bar'"))
                .isEqualTo(true);
    }

    @Test
    public void testGreaterThan()
    {
        assertThat(assertions.expression("a > b")
                .binding("a", "'foo'")
                .binding("b", "'foo'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "'foo'")
                .binding("b", "'bar'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "'bar'")
                .binding("b", "'foo'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "'bar'")
                .binding("b", "'bar'"))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertThat(assertions.expression("a >= b")
                .binding("a", "'foo'")
                .binding("b", "'foo'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "'foo'")
                .binding("b", "'bar'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "'bar'")
                .binding("b", "'foo'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "'bar'")
                .binding("b", "'bar'"))
                .isEqualTo(true);
    }

    @Test
    public void testBetween()
    {
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "'foo'")
                .binding("low", "'foo'")
                .binding("high", "'foo'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "'foo'")
                .binding("low", "'foo'")
                .binding("high", "'bar'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "'foo'")
                .binding("low", "'bar'")
                .binding("high", "'foo'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "'foo'")
                .binding("low", "'bar'")
                .binding("high", "'bar'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "'bar'")
                .binding("low", "'foo'")
                .binding("high", "'foo'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "'bar'")
                .binding("low", "'foo'")
                .binding("high", "'bar'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "'bar'")
                .binding("low", "'bar'")
                .binding("high", "'foo'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "'bar'")
                .binding("low", "'bar'")
                .binding("high", "'bar'"))
                .isEqualTo(true);
    }

    @Test
    public void testIdentical()
    {
        assertThat(assertions.operator(IDENTICAL, "CAST(NULL AS VARCHAR)", "CAST(NULL AS VARCHAR)"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "'foo'", "'foo'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "'foo'", "'fo0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "NULL", "'foo'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "'foo'", "NULL"))
                .isEqualTo(false);
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "cast(null as varchar)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "'foo'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "cast(123456 as varchar)"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "cast(12345.0123 as varchar)"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "cast(true as varchar)"))
                .isEqualTo(false);
    }

    /**
     * Test {@link VarcharOperators#castToNumber}. More coverage is provided also by {@link TestNumberOperators}.
     */
    @Test
    public void testCastToNumber()
    {
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "CAST(NULL AS varchar)"))
                .isNull(NUMBER);
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "'0'"))
                .isEqualTo(new SqlNumber("0"));
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "'1'"))
                .isEqualTo(new SqlNumber("1"));
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "'3.14159265358979'"))
                .isEqualTo(new SqlNumber("3.14159265358979"));
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "'20050910133100123'"))
                .isEqualTo(new SqlNumber("20050910133100123"));
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "'20050910.133100123'"))
                .isEqualTo(new SqlNumber("20050910.133100123"));
        // leading and trailing zeros
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "'000000001.3000000000'"))
                .isEqualTo(new SqlNumber("1.3"));
        // leading and trailing zeros after decimal point
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "'0.0000000013000000000'"))
                .isEqualTo(new SqlNumber("1.3E-9"));
        // leading and trailing zeros before decimal point
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "'0000000013000000000.0000000000'"))
                .isEqualTo(new SqlNumber("1.3E+10"));
        // leading and trailing whitespace
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "' \t\n\f 20050910.133100123  \t\n\f '"))
                .isEqualTo(new SqlNumber("20050910.133100123"));

        // max representable value
        String maxValue = "9".repeat(getNumberMaxDecimalPrecision()) + "e" + -getNumberScaleMinValue();
        BigDecimal bigDecimalMaxValue = new BigDecimal(maxValue);
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "'" + maxValue + "'"))
                .isEqualTo(new SqlNumber(bigDecimalMaxValue));
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "'" + bigDecimalMaxValue.toString() + "'"))
                .isEqualTo(new SqlNumber(bigDecimalMaxValue));
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "'" + bigDecimalMaxValue.toPlainString() + "'"))
                .isEqualTo(new SqlNumber(bigDecimalMaxValue));
        // Rounding due to max precision reached
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "'" + bigDecimalMaxValue.add(BigDecimal.ONE) + "'"))
                .isEqualTo(new SqlNumber(bigDecimalMaxValue));

        assertTrinoExceptionThrownBy(assertions.expression("CAST(a AS number)")
                .binding("a", "'1e" + (-getNumberScaleMaxValue() - 1) + "'")::evaluate)
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Cannot cast '1e-16384' to NUMBER");

        // min representable value
        String minValue = "-" + maxValue;
        BigDecimal bigDecimalMinValue = new BigDecimal(minValue);
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "'" + minValue + "'"))
                .isEqualTo(new SqlNumber(bigDecimalMinValue));
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "'" + bigDecimalMinValue.toString() + "'"))
                .isEqualTo(new SqlNumber(bigDecimalMinValue));
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "'" + bigDecimalMinValue.toPlainString() + "'"))
                .isEqualTo(new SqlNumber(bigDecimalMinValue));

        // min positive representable value
        String minPositiveValue = "1e" + -getNumberScaleMinValue();
        BigDecimal bigDecimalMinPositiveValue = new BigDecimal(minPositiveValue);
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "'" + minPositiveValue + "'"))
                .isEqualTo(new SqlNumber(bigDecimalMinPositiveValue));
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "'" + bigDecimalMinPositiveValue.toString() + "'"))
                .isEqualTo(new SqlNumber(bigDecimalMinPositiveValue));
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "'" + bigDecimalMinPositiveValue.toPlainString() + "'"))
                .isEqualTo(new SqlNumber(bigDecimalMinPositiveValue));

        // max negative representable value
        String maxNegativeValue = "-" + minPositiveValue;
        BigDecimal bigDecimalMaxNegativeValue = new BigDecimal(maxNegativeValue);
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "'" + maxNegativeValue + "'"))
                .isEqualTo(new SqlNumber(bigDecimalMaxNegativeValue));
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "'" + bigDecimalMaxNegativeValue.toString() + "'"))
                .isEqualTo(new SqlNumber(bigDecimalMaxNegativeValue));
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "'" + bigDecimalMaxNegativeValue.toPlainString() + "'"))
                .isEqualTo(new SqlNumber(bigDecimalMaxNegativeValue));

        // NaN
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "' \t\n\f NaN \t\n\f '"))
                .isEqualTo(new SqlNumber(new TrinoNumber.NotANumber()));
        // NaN with sign
        assertTrinoExceptionThrownBy(assertions.expression("CAST(a AS number)")
                .binding("a", "'+NaN'")::evaluate)
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Cannot cast '+NaN' to NUMBER");
        assertTrinoExceptionThrownBy(assertions.expression("CAST(a AS number)")
                .binding("a", "'-NaN'")::evaluate)
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Cannot cast '-NaN' to NUMBER");

        // ±Infinity
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "' \t\n\f Infinity \t\n\f '"))
                .isEqualTo(new SqlNumber(new TrinoNumber.Infinity(false)));
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "' \t\n\f +Infinity \t\n\f '"))
                .isEqualTo(new SqlNumber(new TrinoNumber.Infinity(false)));
        assertThat(assertions.expression("CAST(a AS number)")
                .binding("a", "' \t\n\f -Infinity \t\n\f '"))
                .isEqualTo(new SqlNumber(new TrinoNumber.Infinity(true)));

        // Space between sign and Infinity
        assertTrinoExceptionThrownBy(assertions.expression("CAST(a AS number)")
                .binding("a", "'+ Infinity'")::evaluate)
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Cannot cast '+ Infinity' to NUMBER");
    }

    private static int getNumberMaxDecimalPrecision()
    {
        try {
            Field field = NumberType.class.getDeclaredField("MAX_DECIMAL_PRECISION");
            field.setAccessible(true);
            return (int) field.get(null);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static int getNumberScaleMaxValue()
    {
        try {
            Field field = TrinoNumber.class.getDeclaredField("MAX_SCALE");
            field.setAccessible(true);
            return (int) field.get(null);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static int getNumberScaleMinValue()
    {
        try {
            Field field = TrinoNumber.class.getDeclaredField("MIN_SCALE");
            field.setAccessible(true);
            return (int) field.get(null);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
