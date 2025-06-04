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
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestBooleanOperators
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
        assertThat(assertions.expression("true"))
                .isEqualTo(true);

        assertThat(assertions.expression("false"))
                .isEqualTo(false);
    }

    @Test
    public void testTypeConstructor()
    {
        assertThat(assertions.expression("BOOLEAN 'true'"))
                .isEqualTo(true);

        assertThat(assertions.expression("BOOLEAN 'false'"))
                .isEqualTo(false);
    }

    @Test
    public void testEqual()
    {
        assertThat(assertions.operator(EQUAL, "true", "true"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "true", "false"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "false", "true"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "false", "false"))
                .isEqualTo(true);
    }

    @Test
    public void testNotEqual()
    {
        assertThat(assertions.expression("a <> b")
        .binding("a", "true")
        .binding("b", "true"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
        .binding("a", "true")
        .binding("b", "false"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
        .binding("a", "false")
        .binding("b", "true"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
        .binding("a", "false")
        .binding("b", "false"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThan()
    {
        assertThat(assertions.operator(LESS_THAN, "true", "true"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "true", "false"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "false", "true"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "false", "false"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "true", "true"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "true", "false"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "false", "true"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "false", "false"))
                .isEqualTo(true);
    }

    @Test
    public void testGreaterThan()
    {
        assertThat(assertions.expression("a > b")
                .binding("a", "true")
                .binding("b", "true"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "true")
                .binding("b", "false"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "false")
                .binding("b", "true"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "false")
                .binding("b", "false"))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertThat(assertions.expression("a >= b")
                .binding("a", "true")
                .binding("b", "true"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "true")
                .binding("b", "false"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "false")
                .binding("b", "true"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "false")
                .binding("b", "false"))
                .isEqualTo(true);
    }

    @Test
    public void testBetween()
    {
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "true")
                .binding("low", "true")
                .binding("high", "true"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "true")
                .binding("low", "true")
                .binding("high", "false"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "true")
                .binding("low", "false")
                .binding("high", "true"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "true")
                .binding("low", "false")
                .binding("high", "false"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "false")
                .binding("low", "true")
                .binding("high", "true"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "false")
                .binding("low", "true")
                .binding("high", "false"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "false")
                .binding("low", "false")
                .binding("high", "true"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "false")
                .binding("low", "false")
                .binding("high", "false"))
                .isEqualTo(true);
    }

    @Test
    public void testCastToReal()
    {
        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "true"))
                .isEqualTo(1.0f);

        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "false"))
                .isEqualTo(0.0f);
    }

    @Test
    public void testCastToVarchar()
    {
        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "true"))
                .hasType(VARCHAR)
                .isEqualTo("true");

        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "false"))
                .hasType(VARCHAR)
                .isEqualTo("false");
    }

    @Test
    public void testCastFromVarchar()
    {
        assertThat(assertions.expression("cast(a as boolean)")
                .binding("a", "'true'"))
                .isEqualTo(true);

        assertThat(assertions.expression("cast(a as boolean)")
                .binding("a", "'false'"))
                .isEqualTo(false);
    }

    @Test
    public void testIdentical()
    {
        assertThat(assertions.operator(IDENTICAL, "CAST(NULL AS BOOLEAN)", "CAST(NULL AS BOOLEAN)"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "FALSE", "FALSE"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "TRUE", "TRUE"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "FALSE", "TRUE"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "TRUE", "FALSE"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "FALSE", "NULL"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "TRUE", "NULL"))
                .isEqualTo(false);
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "cast(null AS BOOLEAN)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "true"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "false"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "true AND false"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "true OR false"))
                .isEqualTo(false);
    }
}
