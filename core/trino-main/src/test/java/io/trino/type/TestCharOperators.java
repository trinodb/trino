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

import io.trino.spi.type.SqlNumber;
import io.trino.spi.type.TrinoNumber;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.IDENTICAL;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestCharOperators
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
    public void testEqual()
    {
        assertThat(assertions.operator(EQUAL, "cast('foo' as char(3))", "cast('foo' as char(5))"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "cast('foo' as char(3))", "cast('foo' as char(3))"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "cast('foo' as char(3))", "cast('bar' as char(3))"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "cast('bar' as char(3))", "cast('foo' as char(3))"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "cast('bar' as char(5))", "'bar'"))
                .isEqualTo(true);

        // the char is coerced to varchar by trimming, so the trailing spaces in the varchar are significant
        assertThat(assertions.operator(EQUAL, "cast('bar' as char(5))", "'bar   '"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "cast('a' as char(2))", "cast('a ' as char(2))"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "cast('a ' as char(2))", "cast('a' as char(2))"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "cast('a' as char(3))", "cast('a' as char(2))"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "cast('' as char(3))", "cast('' as char(2))"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "cast('' as char(2))", "cast('' as char(2))"))
                .isEqualTo(true);
    }

    @Test
    public void testNotEqual()
    {
        assertThat(assertions.expression("a <> b")
                .binding("a", "cast('foo' as char(3))")
                .binding("b", "cast('foo' as char(5))"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "cast('foo' as char(3))")
                .binding("b", "cast('foo' as char(3))"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "cast('foo' as char(3))")
                .binding("b", "cast('bar' as char(3))"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "cast('bar' as char(3))")
                .binding("b", "cast('foo' as char(3))"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "cast('bar' as char(5))")
                .binding("b", "'bar'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "cast('bar' as char(5))")
                .binding("b", "'bar   '"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "cast('a' as char(2))")
                .binding("b", "cast('a ' as char(2))"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "cast('a ' as char(2))")
                .binding("b", "cast('a' as char(2))"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "cast('a' as char(3))")
                .binding("b", "cast('a' as char(2))"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "cast('' as char(3))")
                .binding("b", "cast('' as char(2))"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "cast('' as char(2))")
                .binding("b", "cast('' as char(2))"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThan()
    {
        assertThat(assertions.operator(LESS_THAN, "cast('\0' as char(1))", "cast(' ' as char(1))"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "cast('bar' as char(5))", "cast('foo' as char(3))"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "cast('foo' as char(5))", "cast('bar' as char(3))"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "cast('bar' as char(3))", "cast('foo' as char(5))"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "cast('foo' as char(3))", "cast('bar' as char(5))"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "cast('foo' as char(3))", "cast('foo' as char(3))"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "cast('foo' as char(3))", "cast('foo' as char(5))"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "cast('foo' as char(5))", "cast('foo' as char(3))"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "cast('foo' as char(3))", "cast('bar' as char(3))"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "cast('bar' as char(3))", "cast('foo' as char(3))"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "cast('foobar' as char(6))", "cast('foobaz' as char(6))"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "cast('foob r' as char(6))", "cast('foobar' as char(6))"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "cast('\0' as char(1))", "cast(' ' as char(1))"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "cast('\0' as char(1))", "cast('' as char(0))"))
                .isEqualTo(true);

        // 'abc' is implicitly padded with spaces -> 'abc' is greater
        assertThat(assertions.operator(LESS_THAN, "cast('abc\0' as char(4))", "cast('abc' as char(4))"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "cast('\0' as char(1))", "cast('\0 ' as char(2))"))
                .isEqualTo(false);

        // '\0' is implicitly padded with spaces -> both are equal
        assertThat(assertions.operator(LESS_THAN, "cast('\0' as char(2))", "cast('\0 ' as char(2))"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "cast('\0 a' as char(3))", "cast('\0' as char(3))"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "cast('bar' as char(5))", "cast('foo' as char(3))"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "cast('foo' as char(5))", "cast('bar' as char(3))"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "cast('bar' as char(3))", "cast('foo' as char(5))"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "cast('foo' as char(3))", "cast('bar' as char(5))"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "cast('foo' as char(3))", "cast('foo' as char(3))"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "cast('foo' as char(3))", "cast('foo' as char(5))"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "cast('foo' as char(5))", "cast('foo' as char(3))"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "cast('foo' as char(3))", "cast('bar' as char(3))"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "cast('bar' as char(3))", "cast('foo' as char(3))"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "cast('foobar' as char(6))", "cast('foobaz' as char(6))"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "cast('foob r' as char(6))", "cast('foobar' as char(6))"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "cast('\0' as char(1))", "cast(' ' as char(1))"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "cast('\0' as char(1))", "cast('' as char(0))"))
                .isEqualTo(true);

        // 'abc' is implicitly padded with spaces -> 'abc' is greater
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "cast('abc\0' as char(4))", "cast('abc' as char(4))"))
                .isEqualTo(true);

        // length mismatch, coercion to VARCHAR applies
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "cast('\0' as char(1))", "cast('\0 ' as char(2))"))
                .isEqualTo(true);

        // '\0' is implicitly padded with spaces -> both are equal
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "cast('\0' as char(2))", "cast('\0 ' as char(2))"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "cast('\0 a' as char(3))", "cast('\0' as char(3))"))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterThan()
    {
        assertThat(assertions.expression("a > b")
                .binding("a", "cast('bar' as char(5))")
                .binding("b", "cast('foo' as char(3))"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "cast('foo' as char(5))")
                .binding("b", "cast('bar' as char(3))"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "cast('bar' as char(3))")
                .binding("b", "cast('foo' as char(5))"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "cast('foo' as char(3))")
                .binding("b", "cast('bar' as char(5))"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "cast('foo' as char(3))")
                .binding("b", "cast('foo' as char(3))"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "cast('foo' as char(3))")
                .binding("b", "cast('foo' as char(5))"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "cast('foo' as char(5))")
                .binding("b", "cast('foo' as char(3))"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "cast('foo' as char(3))")
                .binding("b", "cast('bar' as char(3))"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "cast('bar' as char(3))")
                .binding("b", "cast('foo' as char(3))"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "cast('foobar' as char(6))")
                .binding("b", "cast('foobaz' as char(6))"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "cast('foob r' as char(6))")
                .binding("b", "cast('foobar' as char(6))"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "cast(' ' as char(1))")
                .binding("b", "cast('\0' as char(1))"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "cast('' as char(0))")
                .binding("b", "cast('\0' as char(1))"))
                .isEqualTo(true);

        // 'abc' is implicitly padded with spaces -> 'abc' is greater
        assertThat(assertions.expression("a > b")
                .binding("a", "cast('abc' as char(4))")
                .binding("b", "cast('abc\0' as char(4))"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "cast('\0 ' as char(2))")
                .binding("b", "cast('\0' as char(1))"))
                .isEqualTo(false);

        // '\0' is implicitly padded with spaces -> both are equal
        assertThat(assertions.expression("a > b")
                .binding("a", "cast('\0 ' as char(2))")
                .binding("b", "cast('\0' as char(2))"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "cast('\0 a' as char(3))")
                .binding("b", "cast('\0' as char(3))"))
                .isEqualTo(true);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertThat(assertions.expression("a >= b")
                .binding("a", "cast('bar' as char(5))")
                .binding("b", "cast('foo' as char(3))"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "cast('foo' as char(5))")
                .binding("b", "cast('bar' as char(3))"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "cast('bar' as char(3))")
                .binding("b", "cast('foo' as char(5))"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "cast('foo' as char(3))")
                .binding("b", "cast('bar' as char(5))"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "cast('foo' as char(3))")
                .binding("b", "cast('foo' as char(3))"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "cast('foo' as char(3))")
                .binding("b", "cast('foo' as char(5))"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "cast('foo' as char(5))")
                .binding("b", "cast('foo' as char(3))"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "cast('foo' as char(3))")
                .binding("b", "cast('bar' as char(3))"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "cast('bar' as char(3))")
                .binding("b", "cast('foo' as char(3))"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "cast('foobar' as char(6))")
                .binding("b", "cast('foobaz' as char(6))"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "cast('foob r' as char(6))")
                .binding("b", "cast('foobar' as char(6))"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "cast(' ' as char(1))")
                .binding("b", "cast('\0' as char(1))"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "cast('' as char(0))")
                .binding("b", "cast('\0' as char(1))"))
                .isEqualTo(true);

        // 'abc' is implicitly padded with spaces -> 'abc' is greater
        assertThat(assertions.expression("a >= b")
                .binding("a", "cast('abc' as char(4))")
                .binding("b", "cast('abc\0' as char(4))"))
                .isEqualTo(true);

        // length mismatch, coercion to VARCHAR applies
        assertThat(assertions.expression("a >= b")
                .binding("a", "cast('\0 ' as char(2))")
                .binding("b", "cast('\0' as char(1))"))
                .isEqualTo(true);

        // '\0' is implicitly padded with spaces -> both are equal
        assertThat(assertions.expression("a >= b")
                .binding("a", "cast('\0 ' as char(2))")
                .binding("b", "cast('\0' as char(2))"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "cast('\0 a' as char(3))")
                .binding("b", "cast('\0' as char(3))"))
                .isEqualTo(true);
    }

    @Test
    public void testBetween()
    {
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "cast('bbb' as char(3))")
                .binding("low", "cast('aaa' as char(3))")
                .binding("high", "cast('ccc' as char(3))"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "cast('foo' as char(3))")
                .binding("low", "cast('foo' as char(3))")
                .binding("high", "cast('foo' as char(3))"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "cast('foo' as char(3))")
                .binding("low", "cast('foo' as char(3))")
                .binding("high", "cast('bar' as char(3))"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "cast('foo' as char(3))")
                .binding("low", "cast('zzz' as char(3))")
                .binding("high", "cast('foo' as char(3))"))
                .isEqualTo(false);

        assertThat(assertions.expression("value NOT BETWEEN low AND high")
                .binding("value", "cast('foo' as char(3))")
                .binding("low", "cast('zzz' as char(3))")
                .binding("high", "cast('foo' as char(3))"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "cast('foo' as char(3))")
                .binding("low", "cast('bar' as char(3))")
                .binding("high", "cast('foo' as char(3))"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "cast('foo' as char(3))")
                .binding("low", "cast('bar' as char(3))")
                .binding("high", "cast('bar' as char(3))"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "cast('bar' as char(3))")
                .binding("low", "cast('foo' as char(3))")
                .binding("high", "cast('foo' as char(3))"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "cast('bar' as char(3))")
                .binding("low", "cast('foo' as char(3))")
                .binding("high", "cast('bar' as char(3))"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "cast('bar' as char(3))")
                .binding("low", "cast('bar' as char(3))")
                .binding("high", "cast('foo' as char(3))"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "cast('bar' as char(3))")
                .binding("low", "cast('bar' as char(3))")
                .binding("high", "cast('bar' as char(3))"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "cast('\0 a' as char(3))")
                .binding("low", "cast('\0' as char(3))")
                .binding("high", "cast('\0a' as char(3))"))
                .isEqualTo(true);

        // length based comparison
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "cast('bar' as char(4))")
                .binding("low", "cast('bar' as char(3))")
                .binding("high", "cast('bar' as char(5))"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "cast('bar' as char(4))")
                .binding("low", "cast('bar' as char(5))")
                .binding("high", "cast('bar' as char(7))"))
                .isEqualTo(true);
    }

    @Test
    public void testIdentical()
    {
        assertThat(assertions.operator(IDENTICAL, "cast(NULL as char(3))", "cast(NULL as char(3))"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "cast(NULL as char(3))", "cast(NULL as char(5))"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "cast('foo' as char(3))", "cast('foo' as char(5))"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "cast('foo' as char(3))", "cast('foo' as char(3))"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "cast('foo' as char(3))", "cast('bar' as char(3))"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "cast('bar' as char(3))", "cast('foo' as char(3))"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "cast('foo' as char(3))", "NULL"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "cast('bar' as char(5))", "'bar'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "cast('bar' as char(5))", "'bar   '"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "NULL", "cast('foo' as char(3))"))
                .isEqualTo(false);
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "CAST(null AS CHAR(3))"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "CHAR '123'"))
                .isEqualTo(false);
    }

    @Test
    public void testCharCast()
    {
        assertThat(assertions.expression("CAST(CHAR '1337' AS BIGINT)"))
                .hasType(BIGINT)
                .isEqualTo(1337L);

        assertThat(assertions.expression("CAST(CHAR ' 1337 ' AS BIGINT)"))
                .hasType(BIGINT)
                .isEqualTo(1337L);

        assertThat(assertions.expression("CAST(CHAR '1337' AS INTEGER)"))
                .hasType(INTEGER)
                .isEqualTo(1337);

        assertThat(assertions.expression("CAST(CHAR ' 1337 ' AS INTEGER)"))
                .hasType(INTEGER)
                .isEqualTo(1337);

        assertThat(assertions.expression("CAST(CHAR '1337' AS SMALLINT)"))
                .hasType(SMALLINT)
                .isEqualTo((short) 1337);

        assertThat(assertions.expression("CAST(CHAR ' 1337 ' AS SMALLINT)"))
                .hasType(SMALLINT)
                .isEqualTo((short) 1337);

        assertThat(assertions.expression("CAST(CHAR '21' AS TINYINT)"))
                .hasType(TINYINT)
                .isEqualTo((byte) 21);

        assertThat(assertions.expression("CAST(CHAR ' 21 ' AS TINYINT)"))
                .hasType(TINYINT)
                .isEqualTo((byte) 21);

        assertThat(assertions.expression("CAST(CHAR '1.0' AS DOUBLE)"))
                .hasType(DOUBLE)
                .isEqualTo(1.0d);

        assertThat(assertions.expression("CAST(CHAR ' 1.0 ' AS DOUBLE)"))
                .hasType(DOUBLE)
                .isEqualTo(1.0d);

        assertThat(assertions.expression("CAST(CHAR '13.37' AS REAL)"))
                .hasType(REAL)
                .isEqualTo(13.37f);

        assertThat(assertions.expression("CAST(CHAR ' 13.37 ' AS REAL)"))
                .hasType(REAL)
                .isEqualTo(13.37f);

        // cast to boolean
        assertThat(assertions.expression("CAST(CHAR 'true' AS BOOLEAN)"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("CAST(CHAR 'false' AS BOOLEAN)"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("CAST(CHAR '1' AS BOOLEAN)"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.expression("CAST(CHAR '0' AS BOOLEAN)"))
                .hasType(BOOLEAN)
                .isEqualTo(false);

        assertThat(assertions.expression("CAST(CHAR 'abc' AS VARBINARY)"))
                .hasType(VARBINARY)
                .matches("X'616263'");

        assertThat(assertions.expression("CAST(CAST('abc' AS char(5)) AS VARBINARY)"))
                .hasType(VARBINARY)
                .matches("X'6162632020'");

        // cast to number
        assertThat(assertions.expression("CAST(CHAR '13.37' AS NUMBER)"))
                .hasType(NUMBER)
                .isEqualTo(new SqlNumber("13.37"));

        assertThat(assertions.expression("CAST(CHAR ' 13.37 ' AS NUMBER)"))
                .hasType(NUMBER)
                .isEqualTo(new SqlNumber("13.37"));

        assertThat(assertions.expression("CAST(CHAR 'NaN' AS NUMBER)"))
                .hasType(NUMBER)
                .isEqualTo(new SqlNumber(new TrinoNumber.NotANumber()));

        assertThat(assertions.expression("CAST(CHAR 'Infinity' AS NUMBER)"))
                .hasType(NUMBER)
                .isEqualTo(new SqlNumber(new TrinoNumber.Infinity(false)));

        assertThat(assertions.expression("CAST(CHAR '-Infinity' AS NUMBER)"))
                .hasType(NUMBER)
                .isEqualTo(new SqlNumber(new TrinoNumber.Infinity(true)));

        assertTrinoExceptionThrownBy(assertions.expression("CAST(CHAR 'abc' AS BIGINT)")::evaluate)
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.expression("CAST(CHAR 'abc' AS DOUBLE)")::evaluate)
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.expression("CAST(CHAR 'abc' AS NUMBER)")::evaluate)
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.expression("cast(a AS INTEGER)")
                .binding("a", "CHAR 'abc'")::evaluate)
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.expression("cast(a AS SMALLINT)")
                .binding("a", "CHAR 'abc'")::evaluate)
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.expression("cast(a AS TINYINT)")
                .binding("a", "CHAR 'abc'")::evaluate)
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.expression("cast(a AS REAL)")
                .binding("a", "CHAR 'abc'")::evaluate)
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.expression("cast(a AS BOOLEAN)")
                .binding("a", "CHAR 'abc'")::evaluate)
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertThat(assertions.expression("cast(a AS VARBINARY)").binding("a", "CHAR 'abc'"))
                .neverFails();
    }
}
