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

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.IDENTICAL;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
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
}
