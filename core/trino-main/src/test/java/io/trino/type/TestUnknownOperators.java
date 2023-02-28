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

import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.UnknownType.UNKNOWN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestUnknownOperators
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addFunctions(InternalFunctionBundle.builder()
                .scalars(TestUnknownOperators.class)
                .build());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @ScalarFunction(value = "null_function", deterministic = false)
    @SqlNullable
    @SqlType("unknown")
    public static Boolean nullFunction()
    {
        return null;
    }

    @Test
    public void testLiteral()
    {
        assertThat(assertions.expression("NULL"))
                .hasType(UNKNOWN);
    }

    @Test
    public void testEqual()
    {
        assertThat(assertions.operator(EQUAL, "NULL", "NULL"))
                .isNull(BOOLEAN);
    }

    @Test
    public void testNotEqual()
    {
        assertThat(assertions.expression("a <> b")
                .binding("a", "NULL")
                .binding("b", "NULL"))
                .isNull(BOOLEAN);
    }

    @Test
    public void testLessThan()
    {
        assertThat(assertions.operator(LESS_THAN, "NULL", "NULL"))
                .isNull(BOOLEAN);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NULL", "NULL"))
                .isNull(BOOLEAN);
    }

    @Test
    public void testGreaterThan()
    {
        assertThat(assertions.expression("a > b")
                .binding("a", "NULL")
                .binding("b", "NULL"))
                .isNull(BOOLEAN);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertThat(assertions.expression("a >= b")
                .binding("a", "NULL")
                .binding("b", "NULL"))
                .isNull(BOOLEAN);
    }

    @Test
    public void testBetween()
    {
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NULL")
                .binding("low", "NULL")
                .binding("high", "NULL"))
                .isNull(BOOLEAN);
    }

    @Test
    public void testCastToBigint()
    {
        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "NULL"))
                .isNull(BIGINT);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "null_function()"))
                .isNull(BIGINT);
    }

    @Test
    public void testCastToVarchar()
    {
        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "NULL"))
                .isNull(VARCHAR);

        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "null_function()"))
                .isNull(VARCHAR);
    }

    @Test
    public void testCastToDouble()
    {
        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "NULL"))
                .isNull(DOUBLE);

        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "null_function()"))
                .isNull(DOUBLE);
    }

    @Test
    public void testCastToBoolean()
    {
        assertThat(assertions.expression("cast(a as boolean)")
                .binding("a", "NULL"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("cast(a as boolean)")
                .binding("a", "null_function()"))
                .isNull(BOOLEAN);
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertThat(assertions.operator(IS_DISTINCT_FROM, "NULL", "NULL"))
                .isEqualTo(false);
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "null"))
                .isEqualTo(true);
    }
}
