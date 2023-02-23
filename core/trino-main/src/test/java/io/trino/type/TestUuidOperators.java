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

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.SqlVarbinaryTestingUtil.sqlVarbinaryFromHex;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestUuidOperators
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
    public void testRandomUuid()
    {
        assertThat(assertions.expression("uuid()"))
                .hasType(UUID);
    }

    @Test
    public void testVarcharToUUIDCast()
    {
        assertThat(assertions.expression("cast(a as UUID)")
                .binding("a", "'00000000-0000-0000-0000-000000000000'"))
                .hasType(UUID)
                .isEqualTo("00000000-0000-0000-0000-000000000000");

        assertThat(assertions.expression("cast(a as UUID)")
                .binding("a", "'12151fd2-7586-11e9-8f9e-2a86e4085a59'"))
                .hasType(UUID)
                .isEqualTo("12151fd2-7586-11e9-8f9e-2a86e4085a59");

        assertThat(assertions.expression("cast(a as UUID)")
                .binding("a", "'300433ad-b0a1-3b53-a977-91cab582458e'"))
                .hasType(UUID)
                .isEqualTo("300433ad-b0a1-3b53-a977-91cab582458e");

        assertThat(assertions.expression("cast(a as UUID)")
                .binding("a", "'d3074e99-de12-4b8c-a2a1-b7faf79faba6'"))
                .hasType(UUID)
                .isEqualTo("d3074e99-de12-4b8c-a2a1-b7faf79faba6");

        assertThat(assertions.expression("cast(a as UUID)")
                .binding("a", "'dfa7eaf8-6a26-5749-8d36-336025df74e8'"))
                .hasType(UUID)
                .isEqualTo("dfa7eaf8-6a26-5749-8d36-336025df74e8");

        assertThat(assertions.expression("cast(a as UUID)")
                .binding("a", "'12151FD2-7586-11E9-8F9E-2A86E4085A59'"))
                .hasType(UUID)
                .isEqualTo("12151fd2-7586-11e9-8f9e-2a86e4085a59");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as UUID)")
                .binding("a", "'1-2-3-4-1'").evaluate())
                .hasMessage("Invalid UUID string length: 9")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as UUID)")
                .binding("a", "'12151fd217586211e938f9e42a86e4085a59'").evaluate())
                .hasMessage("Cannot cast value to UUID: 12151fd217586211e938f9e42a86e4085a59")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testUUIDToVarcharCast()
    {
        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "UUID 'd3074e99-de12-4b8c-a2a1-b7faf79faba6'"))
                .hasType(VARCHAR)
                .isEqualTo("d3074e99-de12-4b8c-a2a1-b7faf79faba6");

        assertThat(assertions.expression("cast(a AS VARCHAR)")
                .binding("a", "UUID 'd3074e99-de12-4b8c-a2a1-b7faf79faba6'"))
                .hasType(VARCHAR)
                .isEqualTo("d3074e99-de12-4b8c-a2a1-b7faf79faba6");
    }

    @Test
    public void testVarbinaryToUUIDCast()
    {
        assertThat(assertions.expression("cast(a as UUID)")
                .binding("a", "x'00000000000000000000000000000000'"))
                .hasType(UUID)
                .isEqualTo("00000000-0000-0000-0000-000000000000");

        assertThat(assertions.expression("cast(a as UUID)")
                .binding("a", "x'12151fd2758611e98f9e2a86e4085a59'"))
                .hasType(UUID)
                .isEqualTo("12151fd2-7586-11e9-8f9e-2a86e4085a59");

        assertThat(assertions.expression("cast(a as UUID)")
                .binding("a", "x'300433adb0a13b53a97791cab582458e'"))
                .hasType(UUID)
                .isEqualTo("300433ad-b0a1-3b53-a977-91cab582458e");

        assertThat(assertions.expression("cast(a as UUID)")
                .binding("a", "x'd3074e99de124b8ca2a1b7faf79faba6'"))
                .hasType(UUID)
                .isEqualTo("d3074e99-de12-4b8c-a2a1-b7faf79faba6");

        assertThat(assertions.expression("cast(a as UUID)")
                .binding("a", "x'dfa7eaf86a2657498d36336025df74e8'"))
                .hasType(UUID)
                .isEqualTo("dfa7eaf8-6a26-5749-8d36-336025df74e8");

        assertThat(assertions.expression("cast(a as UUID)")
                .binding("a", "x'12151fd2758611e98f9e2a86e4085a59'"))
                .hasType(UUID)
                .isEqualTo("12151fd2-7586-11e9-8f9e-2a86e4085a59");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as UUID)")
                .binding("a", "x'f000001100'").evaluate())
                .hasMessage("Invalid UUID binary length: 5")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testUUIDToVarbinaryCast()
    {
        assertThat(assertions.expression("cast(a as VARBINARY)")
                .binding("a", "UUID '00000000-0000-0000-0000-000000000000'"))
                .isEqualTo(sqlVarbinaryFromHex("00000000000000000000000000000000"));

        assertThat(assertions.expression("cast(a as VARBINARY)")
                .binding("a", "UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0'"))
                .isEqualTo(sqlVarbinaryFromHex("6B5F5B6567E443B08EE3586CD49F58A0"));
    }

    @Test
    public void testEquals()
    {
        assertThat(assertions.operator(EQUAL, "UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0'", "UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0'", "UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0'"))
                .isEqualTo(true);
    }

    @Test
    public void testDistinctFrom()
    {
        assertThat(assertions.operator(IS_DISTINCT_FROM, "UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0'", "UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "CAST(NULL AS UUID)", "CAST(NULL AS UUID)"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0'", "UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a1'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0'", "CAST(NULL AS UUID)"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "CAST(NULL AS UUID)", "UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0'"))
                .isEqualTo(true);
    }

    @Test
    public void testNotEquals()
    {
        assertThat(assertions.expression("a != b")
                .binding("a", "UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0'")
                .binding("b", "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'")
                .binding("b", "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'"))
                .isEqualTo(false);
    }

    @Test
    public void testOrderOperators()
    {
        assertThat(assertions.operator(LESS_THAN, "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a58'", "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'", "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a58'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a58'", "UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0'", "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a58'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "UUID 'dfa7eaf8-6a26-5749-8d36-336025df74e8'")
                .binding("b", "UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0'")
                .binding("b", "UUID 'dfa7eaf8-6a26-5749-8d36-336025df74e8'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a52'")
                .binding("low", "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a50'")
                .binding("high", "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a52'")
                .binding("low", "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a54'")
                .binding("high", "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0'")
                .binding("low", "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a50'")
                .binding("high", "UUID 'dfa7eaf8-6a26-5749-8d36-336025df74e8'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a52'")
                .binding("low", "UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0'")
                .binding("high", "UUID 'dfa7eaf8-6a26-5749-8d36-336025df74e8'"))
                .isEqualTo(false);
    }

    @Test
    public void testCompare()
    {
        assertThat(assertions.operator(COMPARISON_UNORDERED_LAST, "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a58'", "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'"))
                .isEqualTo(-1);
        assertThat(assertions.operator(COMPARISON_UNORDERED_LAST, "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a58'", "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a58'"))
                .isEqualTo(0);
        assertThat(assertions.operator(COMPARISON_UNORDERED_LAST, "UUID 'dfa7eaf8-6a26-5749-8d36-336025df74e8'", "UUID '6b5f5b65-67e4-43b0-8ee3-586cd49f58a0'"))
                .isEqualTo(1);
        assertThat(assertions.operator(COMPARISON_UNORDERED_LAST, "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a58'", "UUID 'dfa7eaf8-6a26-5749-8d36-336025df74e8'"))
                .isEqualTo(-1);
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "CAST(null AS UUID)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'"))
                .isEqualTo(false);
    }

    @Test
    public void testHash()
    {
        assertThat(assertions.operator(HASH_CODE, "CAST(null AS UUID)"))
                .isNull(BIGINT);

        assertThat(assertions.operator(HASH_CODE, "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'"))
                .isEqualTo(-6664838859954528709L);
    }
}
