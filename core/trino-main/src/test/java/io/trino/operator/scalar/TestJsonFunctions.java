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
package io.trino.operator.scalar;

import io.trino.json.Json;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.json.JsonItems.toText;
import static io.trino.operator.scalar.JsonFunctions.jsonArrayGet;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.JsonType.JSON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestJsonFunctions
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
    public void testDatetimeStringContainment()
    {
        assertThat(assertions.query(
                """
                SELECT json_array_contains(
                    json_query(JSON '["2020-01-01"]', 'lax $[*].datetime()' RETURNING JSON WITH ARRAY WRAPPER),
                    '2020-01-01')
                """))
                .matches("VALUES true");
    }

    @Test
    public void testExponentOverflowIsInvalidInput()
    {
        assertTrinoExceptionThrownBy(() -> assertions.expression("JSON '1e309'").evaluate())
                .hasErrorCode(INVALID_LITERAL);
        assertTrinoExceptionThrownBy(() -> assertions.function("json_parse", "'1e309'").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testJsonConstantIdentity()
    {
        assertThat(assertions.execute("SELECT JSON '1', JSON '1.0'").getMaterializedRows().getFirst().getFields())
                .containsExactly("1", "1.0");
        assertThat(assertions.execute("SELECT JSON '{\"a\":1,\"b\":2}', JSON '{\"b\":2,\"a\":1}'").getMaterializedRows().getFirst().getFields())
                .containsExactly("{\"a\":1,\"b\":2}", "{\"b\":2,\"a\":1}");
    }

    @Test
    public void testContainerScalarFidelity()
    {
        assertThat(assertions.execute("SELECT CAST(ARRAY[-0.0e0] AS JSON), CAST(MAP(ARRAY['x'], ARRAY[-0.0e0]) AS JSON), CAST(ROW(-0.0e0) AS JSON)")
                .getMaterializedRows().getFirst().getFields())
                .containsExactly("[-0.0]", "{\"x\":-0.0}", "{\"\":-0.0}");
        assertThat(assertions.execute("SELECT json_query(CAST(ARRAY[infinity()] AS JSON), 'lax $[0].type()' RETURNING VARCHAR)")
                .getMaterializedRows().getFirst().getField(0))
                .isEqualTo("\"number\"");
    }

    @Test
    public void testDuplicateDescendantMembers()
    {
        assertThat(assertions.execute("SELECT json_query(JSON '{\"a\":1,\"a\":2}', 'lax $..a' RETURNING VARCHAR WITH ARRAY WRAPPER)")
                .getMaterializedRows().getFirst().getField(0))
                .isEqualTo("[1,2]");
    }

    @Test
    public void testIsJsonScalar()
    {
        assertThat(assertions.function("is_json_scalar", "null"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("is_json_scalar", "JSON 'null'"))
                .isEqualTo(true);

        assertThat(assertions.function("is_json_scalar", "JSON 'true'"))
                .isEqualTo(true);

        assertThat(assertions.function("is_json_scalar", "JSON '1'"))
                .isEqualTo(true);

        assertThat(assertions.function("is_json_scalar", "JSON '\"str\"'"))
                .isEqualTo(true);

        assertThat(assertions.function("is_json_scalar", "'null'"))
                .isEqualTo(true);

        assertThat(assertions.function("is_json_scalar", "'true'"))
                .isEqualTo(true);

        assertThat(assertions.function("is_json_scalar", "'1'"))
                .isEqualTo(true);

        assertThat(assertions.function("is_json_scalar", "'\"str\"'"))
                .isEqualTo(true);

        assertThat(assertions.function("is_json_scalar", "JSON '[1, 2, 3]'"))
                .isEqualTo(false);

        assertThat(assertions.function("is_json_scalar", "JSON '{\"a\": 1, \"b\": 2}'"))
                .isEqualTo(false);

        assertThat(assertions.function("is_json_scalar", "'[1, 2, 3]'"))
                .isEqualTo(false);

        assertThat(assertions.function("is_json_scalar", "'{\"a\": 1, \"b\": 2}'"))
                .isEqualTo(false);

        assertTrinoExceptionThrownBy(assertions.function("is_json_scalar", "''")::evaluate)
                .hasMessage("Invalid JSON value: ");

        assertTrinoExceptionThrownBy(assertions.function("is_json_scalar", "'[1'")::evaluate)
                .hasMessage("Invalid JSON value: [1");

        assertTrinoExceptionThrownBy(assertions.function("is_json_scalar", "'1 trailing'")::evaluate)
                .hasMessage("Invalid JSON value: 1 trailing");

        assertTrinoExceptionThrownBy(assertions.function("is_json_scalar", "'[1, 2] trailing'")::evaluate)
                .hasMessage("Invalid JSON value: [1, 2] trailing");
    }

    @Test
    public void testJsonArrayLength()
    {
        assertThat(assertions.function("json_array_length", "'[]'"))
                .isEqualTo(0L);

        assertThat(assertions.function("json_array_length", "'[1]'"))
                .isEqualTo(1L);

        assertThat(assertions.function("json_array_length", "'[1, \"foo\", null]'"))
                .isEqualTo(3L);

        assertThat(assertions.function("json_array_length", "'[2, 4, {\"a\": [8, 9]}, [], [5], 4]'"))
                .isEqualTo(6L);

        assertThat(assertions.function("json_array_length", "JSON '[]'"))
                .isEqualTo(0L);

        assertThat(assertions.function("json_array_length", "JSON '[1]'"))
                .isEqualTo(1L);

        assertThat(assertions.function("json_array_length", "JSON '[1, \"foo\", null]'"))
                .isEqualTo(3L);

        assertThat(assertions.function("json_array_length", "JSON '[2, 4, {\"a\": [8, 9]}, [], [5], 4]'"))
                .isEqualTo(6L);

        assertThat(assertions.function("json_array_length", "null"))
                .isNull(BIGINT);
    }

    @Test
    public void testJsonArrayContainsBoolean()
    {
        assertThat(assertions.function("json_array_contains", "'[]'", "true"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "'[true]'", "true"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "'[false]'", "false"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "'[true, false]'", "false"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "'[false, true]'", "true"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "'[1]'", "true"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "'[[true]]'", "true"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "'[1, \"foo\", null, \"true\"]'", "true"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "'[2, 4, {\"a\": [8, 9]}, [], [5], false]'", "false"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "JSON '[]'", "true"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "JSON '[true]'", "true"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "JSON '[false]'", "false"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "JSON '[true, false]'", "false"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "JSON '[false, true]'", "true"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "JSON '[1]'", "true"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "JSON '[[true]]'", "true"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "JSON '[1, \"foo\", null, \"true\"]'", "true"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "JSON '[2, 4, {\"a\": [8, 9]}, [], [5], false]'", "false"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "null", "true"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("json_array_contains", "null", "null"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("json_array_contains", "'[]'", "null"))
                .isNull(BOOLEAN);
    }

    @Test
    public void testJsonArrayContainsLong()
    {
        assertThat(assertions.function("json_array_contains", "'[]'", "1"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "'[3]'", "3"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "'[-4]'", "-4"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "'[1.0]'", "1"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "'[[2]]'", "2"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "'[1, \"foo\", null, \"8\"]'", "8"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "'[2, 4, {\"a\": [8, 9]}, [], [5], 6]'", "6"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "'[92233720368547758071]'", "-9"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "JSON '[]'", "1"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "JSON '[3]'", "3"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "JSON '[-4]'", "-4"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "JSON '[1.0]'", "1"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "JSON '[[2]]'", "2"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "JSON '[1, \"foo\", null, \"8\"]'", "8"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "JSON '[2, 4, {\"a\": [8, 9]}, [], [5], 6]'", "6"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "JSON '[92233720368547758071]'", "-9"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "null", "1"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("json_array_contains", "null", "null"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("json_array_contains", "'[3]'", "null"))
                .isNull(BOOLEAN);
    }

    @Test
    public void testJsonArrayContainsDouble()
    {
        assertThat(assertions.function("json_array_contains", "'[]'", "1"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "'[1.5]'", "1.5"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "'[-9.5]'", "-9.5"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "'[1]'", "1.0"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "'[[2.5]]'", "2.5"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "'[1, \"foo\", null, \"8.2\"]'", "8.2"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "'[2, 4, {\"a\": [8, 9]}, [], [5], 6.1]'", "6.1"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "'[9.6E400]'", "4.2"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "JSON '[]'", "1"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "JSON '[1.5]'", "1.5"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "JSON '[-9.5]'", "-9.5"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "JSON '[1]'", "1.0"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "JSON '[[2.5]]'", "2.5"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "JSON '[1, \"foo\", null, \"8.2\"]'", "8.2"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "JSON '[2, 4, {\"a\": [8, 9]}, [], [5], 6.1]'", "6.1"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "CAST(ARRAY[infinity()] AS JSON)", "4.2"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "null", "1.5"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("json_array_contains", "null", "null"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("json_array_contains", "'[3.5]'", "null"))
                .isNull(BOOLEAN);
    }

    @Test
    public void testJsonArrayContainsString()
    {
        assertThat(assertions.function("json_array_contains", "'[]'", "'x'"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "'[\"foo\"]'", "'foo'"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "'[\"foo\", null]'", "cast(null as varchar)"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("json_array_contains", "'[\"8\"]'", "'8'"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "'[1, \"foo\", null]'", "'foo'"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "'[1, 5]'", "'5'"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "'[2, 4, {\"a\": [8, 9]}, [], [5], \"6\"]'", "'6'"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "JSON '[]'", "'x'"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "JSON '[\"foo\"]'", "'foo'"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "JSON '[\"foo\", null]'", "cast(null as varchar)"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("json_array_contains", "JSON '[\"8\"]'", "'8'"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "JSON '[1, \"foo\", null]'", "'foo'"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "JSON '[1, 5]'", "'5'"))
                .isEqualTo(false);

        assertThat(assertions.function("json_array_contains", "JSON '[2, 4, {\"a\": [8, 9]}, [], [5], \"6\"]'", "'6'"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "null", "'x'"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("json_array_contains", "null", "''"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("json_array_contains", "null", "null"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("json_array_contains", "'[\"\"]'", "null"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("json_array_contains", "'[\"\"]'", "''"))
                .isEqualTo(true);

        assertThat(assertions.function("json_array_contains", "'[\"\"]'", "'x'"))
                .isEqualTo(false);
    }

    @Test
    public void testJsonArrayGetLong()
    {
        assertThat(assertions.function("json_array_get", "'[1]'", "0"))
                .hasType(JSON)
                .isEqualTo("1");

        assertThat(assertions.function("json_array_get", "'[2, 7, 4]'", "1"))
                .hasType(JSON)
                .isEqualTo("7");

        assertThat(assertions.function("json_array_get", "'[2, 7, 4, 6, 8, 1, 0]'", "6"))
                .hasType(JSON)
                .isEqualTo("0");

        assertThat(assertions.function("json_array_get", "'[]'", "0"))
                .isNull(JSON);

        assertThat(assertions.function("json_array_get", "'[1, 3, 2]'", "3"))
                .isNull(JSON);

        assertThat(assertions.function("json_array_get", "'[2, 7, 4, 6, 8, 1, 0]'", "-1"))
                .hasType(JSON)
                .isEqualTo("0");

        assertThat(assertions.function("json_array_get", "'[2, 7, 4, 6, 8, 1, 0]'", "-2"))
                .hasType(JSON)
                .isEqualTo("1");

        assertThat(assertions.function("json_array_get", "'[2, 7, 4, 6, 8, 1, 0]'", "-7"))
                .hasType(JSON)
                .isEqualTo("2");

        assertThat(assertions.function("json_array_get", "'[2, 7, 4, 6, 8, 1, 0]'", "-8"))
                .isNull(JSON);

        assertThat(assertions.function("json_array_get", "JSON '[1]'", "0"))
                .hasType(JSON)
                .isEqualTo("1");

        assertThat(assertions.function("json_array_get", "JSON '[2, 7, 4]'", "1"))
                .hasType(JSON)
                .isEqualTo("7");

        assertThat(assertions.function("json_array_get", "JSON '[2, 7, 4, 6, 8, 1, 0]'", "6"))
                .hasType(JSON)
                .isEqualTo("0");

        assertThat(assertions.function("json_array_get", "JSON '[]'", "0"))
                .isNull(JSON);

        assertThat(assertions.function("json_array_get", "JSON '[1, 3, 2]'", "3"))
                .isNull(JSON);

        assertThat(assertions.function("json_array_get", "JSON '[2, 7, 4, 6, 8, 1, 0]'", "-1"))
                .hasType(JSON)
                .isEqualTo("0");

        assertThat(assertions.function("json_array_get", "JSON '[2, 7, 4, 6, 8, 1, 0]'", "-2"))
                .hasType(JSON)
                .isEqualTo("1");

        assertThat(assertions.function("json_array_get", "JSON '[2, 7, 4, 6, 8, 1, 0]'", "-7"))
                .hasType(JSON)
                .isEqualTo("2");

        assertThat(assertions.function("json_array_get", "JSON '[2, 7, 4, 6, 8, 1, 0]'", "-8"))
                .isNull(JSON);

        assertThat(assertions.function("json_array_get", "'[]'", "null"))
                .isNull(JSON);

        assertThat(assertions.function("json_array_get", "'[1]'", "null"))
                .isNull(JSON);

        assertThat(assertions.function("json_array_get", "''", "null"))
                .isNull(JSON);

        assertThat(assertions.function("json_array_get", "''", "1"))
                .isNull(JSON);

        assertThat(assertions.function("json_array_get", "''", "-1"))
                .isNull(JSON);

        assertThat(assertions.function("json_array_get", "'[1]'", "-9223372036854775807 - 1"))
                .isNull(JSON);
    }

    @Test
    public void testJsonArrayGetSkipsUnselectedValues()
    {
        for (String input : new String[] {"'[\"\\uD800\", 5]'", "'[[1e400], 5]'", "'[{\"a\": 1e400}, 5]'"}) {
            for (String index : new String[] {"1", "-1"}) {
                assertThat(assertions.function("json_array_get", input, index))
                        .hasType(JSON)
                        .isEqualTo("5");
            }
            for (String index : new String[] {"0", "-2"}) {
                assertThat(assertions.function("json_array_get", input, index))
                        .isNull(JSON);
            }
        }

        for (String index : new String[] {"1", "-1"}) {
            assertThat(assertions.function("json_array_get", "'[[1,], 5]'", index))
                    .isNull(JSON);
        }
        assertThat(assertions.function("json_array_get", "'[5, [1,]]'", "0"))
                .hasType(JSON)
                .isEqualTo("5");
        assertThat(assertions.function("json_array_get", "'[5, [1,]]'", "-2"))
                .isNull(JSON);
    }

    @Test
    public void testJsonArrayGetString()
    {
        // A JSON column holds JSON values, so a string element round-trips through
        // getObjectValue as the quoted form `"jhfa"` — a bare `jhfa` is not valid JSON.
        assertThat(assertions.function("json_array_get", "'[\"jhfa\"]'", "0"))
                .hasType(JSON)
                .isEqualTo("\"jhfa\"");

        assertThat(assertions.function("json_array_get", "'[\"jhfa\", null]'", "1"))
                .isNull(JSON);

        assertThat(assertions.function("json_array_get", "'[\"as\", \"fgs\", \"tehgf\"]'", "1"))
                .hasType(JSON)
                .isEqualTo("\"fgs\"");

        assertThat(assertions.function("json_array_get", "'[\"as\", \"fgs\", \"tehgf\", \"gjyj\", \"jut\"]'", "4"))
                .hasType(JSON)
                .isEqualTo("\"jut\"");

        assertThat(assertions.function("json_array_get", "JSON '[\"jhfa\"]'", "0"))
                .hasType(JSON)
                .isEqualTo("\"jhfa\"");

        assertThat(assertions.function("json_array_get", "JSON '[\"jhfa\", null]'", "1"))
                .isNull(JSON);

        assertThat(assertions.function("json_array_get", "JSON '[\"as\", \"fgs\", \"tehgf\"]'", "1"))
                .hasType(JSON)
                .isEqualTo("\"fgs\"");

        assertThat(assertions.function("json_array_get", "JSON '[\"as\", \"fgs\", \"tehgf\", \"gjyj\", \"jut\"]'", "4"))
                .hasType(JSON)
                .isEqualTo("\"jut\"");

        assertThat(assertions.function("json_array_get", "'[\"\"]'", "0"))
                .hasType(JSON)
                .isEqualTo("\"\"");

        assertThat(assertions.function("json_array_get", "'[]'", "0"))
                .isNull(JSON);

        assertThat(assertions.function("json_array_get", "'[null]'", "0"))
                .isNull(JSON);

        assertThat(assertions.function("json_array_get", "'[]'", "null"))
                .isNull(JSON);

        assertThat(assertions.function("json_array_get", "'[1]'", "-9223372036854775807 - 1"))
                .isNull(JSON);
    }

    @Test
    public void testJsonArrayGetDouble()
    {
        assertThat(assertions.function("json_array_get", "'[3.14]'", "0"))
                .hasType(JSON)
                .isEqualTo("3.14");

        assertThat(assertions.function("json_array_get", "'[3.14, null]'", "1"))
                .isNull(JSON);

        assertThat(assertions.function("json_array_get", "'[1.12, 3.54, 2.89]'", "1"))
                .hasType(JSON)
                .isEqualTo("3.54");

        assertThat(assertions.function("json_array_get", "'[0.58, 9.7, 7.6, 11.2, 5.02]'", "4"))
                .hasType(JSON)
                .isEqualTo("5.02");

        assertThat(assertions.function("json_array_get", "JSON '[3.14]'", "0"))
                .hasType(JSON)
                .isEqualTo("3.14");

        assertThat(assertions.function("json_array_get", "JSON '[3.14, null]'", "1"))
                .isNull(JSON);

        assertThat(assertions.function("json_array_get", "JSON '[1.12, 3.54, 2.89]'", "1"))
                .hasType(JSON)
                .isEqualTo("3.54");

        assertThat(assertions.function("json_array_get", "JSON '[0.58, 9.7, 7.6, 11.2, 5.02]'", "4"))
                .hasType(JSON)
                .isEqualTo("5.02");

        assertThat(assertions.function("json_array_get", "'[1.0]'", "-1"))
                .hasType(JSON)
                .isEqualTo("1.0");

        assertThat(assertions.function("json_array_get", "'[1.0]'", "null"))
                .isNull(JSON);
    }

    @Test
    public void testJsonArrayGetDecimal()
    {
        for (String value : List.of("9007199254740993.0", "-9007199254740993.0", "1.00", "0.12345678901234567890123456789012345678")) {
            String array = "[null, %s, null]".formatted(value);
            for (int index : new int[] {1, -2}) {
                assertThat(toText(jsonArrayGet(Json.unchecked(utf8Slice(array)), index)).toStringUtf8())
                        .isEqualTo(value);

                for (String input : List.of("'%s'", "JSON '%s'")) {
                    assertThat(assertions.expression("json_format(json_array_get(a, i))")
                            .binding("a", input.formatted(array))
                            .binding("i", Integer.toString(index)))
                            .describedAs("input: %s, index: %s", input.formatted(array), index)
                            .isEqualTo(value);

                    int scale = value.length() - value.indexOf('.') - 1;
                    assertThat(assertions.expression("CAST(json_array_get(a, i) AS DECIMAL(38, %s))".formatted(scale))
                            .binding("a", input.formatted(array))
                            .binding("i", Integer.toString(index)))
                            .describedAs("input: %s, index: %s", input.formatted(array), index)
                            .matches("CAST('%s' AS DECIMAL(38, %s))".formatted(value, scale));
                }
            }
        }
    }

    @Test
    public void testJsonArrayGetBoolean()
    {
        assertThat(assertions.function("json_array_get", "'[true]'", "0"))
                .hasType(JSON)
                .isEqualTo("true");

        assertThat(assertions.function("json_array_get", "'[true, null]'", "1"))
                .isNull(JSON);

        assertThat(assertions.function("json_array_get", "'[false, false, true]'", "1"))
                .hasType(JSON)
                .isEqualTo("false");

        assertThat(assertions.function("json_array_get", "'[true, false, false, true, true, false]'", "5"))
                .hasType(JSON)
                .isEqualTo("false");

        assertThat(assertions.function("json_array_get", "JSON '[true]'", "0"))
                .hasType(JSON)
                .isEqualTo("true");

        assertThat(assertions.function("json_array_get", "JSON '[true, null]'", "1"))
                .isNull(JSON);

        assertThat(assertions.function("json_array_get", "JSON '[false, false, true]'", "1"))
                .hasType(JSON)
                .isEqualTo("false");

        assertThat(assertions.function("json_array_get", "JSON '[true, false, false, true, true, false]'", "5"))
                .hasType(JSON)
                .isEqualTo("false");

        assertThat(assertions.function("json_array_get", "'[true]'", "-1"))
                .hasType(JSON)
                .isEqualTo("true");
    }

    @Test
    public void testJsonArrayGetNonScalar()
    {
        for (String index : List.of("0", "-1")) {
            assertThat(assertions.function("json_array_get", "'[{\"a\":1,\"a\":2}]'", index))
                    .hasType(JSON)
                    .isEqualTo("{\"a\":1,\"a\":2}");
        }

        assertThat(assertions.function("json_array_get", "'[{\"hello\":\"world\"}]'", "0"))
                .hasType(JSON)
                .isEqualTo("{\"hello\":\"world\"}");

        assertThat(assertions.function("json_array_get", "'[{\"hello\":\"world\"}, [1,2,3]]'", "1"))
                .hasType(JSON)
                .isEqualTo("[1,2,3]");

        assertThat(assertions.function("json_array_get", "'[{\"hello\":\"world\"}, [1,2, {\"x\" : 2} ]]'", "1"))
                .hasType(JSON)
                .isEqualTo("[1,2,{\"x\":2}]");

        assertThat(assertions.function("json_array_get", "'[{\"hello\":\"world\"}, {\"a\":[{\"x\":99}]}]'", "1"))
                .hasType(JSON)
                .isEqualTo("{\"a\":[{\"x\":99}]}");

        assertThat(assertions.function("json_array_get", "'[{\"hello\":\"world\"}, {\"a\":[{\"x\":99}]}]'", "-1"))
                .hasType(JSON)
                .isEqualTo("{\"a\":[{\"x\":99}]}");

        assertThat(assertions.function("json_array_get", "'[{\"hello\": null}]'", "0"))
                .hasType(JSON)
                .isEqualTo("{\"hello\":null}");

        assertThat(assertions.function("json_array_get", "'[{\"\":\"\"}]'", "0"))
                .hasType(JSON)
                .isEqualTo("{\"\":\"\"}");

        assertThat(assertions.function("json_array_get", "'[{null:null}]'", "0"))
                .isNull(JSON);

        assertThat(assertions.function("json_array_get", "'[{null:\"\"}]'", "0"))
                .isNull(JSON);

        assertThat(assertions.function("json_array_get", "'[{\"\":null}]'", "0"))
                .hasType(JSON)
                .isEqualTo("{\"\":null}");

        assertThat(assertions.function("json_array_get", "'[{\"\":null}]'", "-1"))
                .hasType(JSON)
                .isEqualTo("{\"\":null}");
    }

    @Test
    public void testJsonArrayContainsInvalid()
    {
        for (String value : new String[] {"'x'", "2.5", "8", "true", "cast(null as varchar)"}) {
            for (String array : new String[] {"", "123", "[", "[1,0,]", "[1,,0]"}) {
                assertThat(assertions.function("json_array_contains", "'%s'".formatted(array), value))
                        .isNull(BOOLEAN);
            }
        }
    }

    @Test
    public void testInvalidJsonParse()
    {
        assertTrinoExceptionThrownBy(assertions.expression("JSON 'INVALID'")::evaluate)
                .hasErrorCode(INVALID_LITERAL);

        assertTrinoExceptionThrownBy(assertions.function("json_parse", "'INVALID'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.function("json_parse", "'\"x\": 1'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.function("json_parse", "'{}{'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.function("json_parse", "'{} \"a\"'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.function("json_parse", "'{}{abc'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.function("json_parse", "'{}abc'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.function("json_parse", "''")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testJsonFormat()
    {
        assertThat(assertions.function("json_format", "JSON '[\"a\", \"b\"]'"))
                .hasType(VARCHAR)
                .isEqualTo("[\"a\",\"b\"]");
    }

    @Test
    public void testJsonFormatNestingLimit()
    {
        for (int depth : new int[] {1000, 1001, 1024}) {
            assertThat(assertions.expression("json_format(json_parse(rpad('', " + depth + ", '[') || '1' || rpad('', " + depth + ", ']')))"))
                    .isEqualTo("[".repeat(depth) + "1" + "]".repeat(depth));
        }
    }

    @Test
    public void testBigintContainsTypedDecimal()
    {
        for (String value : new String[] {"DECIMAL '1'", "CAST(1 AS DECIMAL(38, 0))"}) {
            assertThat(assertions.function("json_array_contains", "JSON_ARRAY(" + value + " RETURNING JSON)", "BIGINT '1'"))
                    .isEqualTo(true);
            assertThat(assertions.function("json_array_contains", "JSON_ARRAY(" + value + ")", "BIGINT '1'"))
                    .isEqualTo(true);
        }
        for (String value : new String[] {"DECIMAL '1.0'", "DECIMAL '1.5'", "DECIMAL '9223372036854775808'"}) {
            assertThat(assertions.function("json_array_contains", "JSON_ARRAY(" + value + " RETURNING JSON)", "BIGINT '1'"))
                    .isEqualTo(false);
        }
    }

    @Test
    public void testJsonSize()
    {
        assertThat(assertions.function("json_size", "'{\"x\": {\"a\" : 1, \"b\" : 2} }'", "'$'"))
                .isEqualTo(1L);

        assertThat(assertions.function("json_size", "'{\"x\": {\"a\" : 1, \"b\" : 2} }'", "'$.x'"))
                .isEqualTo(2L);

        assertThat(assertions.function("json_size", "'{\"x\": {\"a\" : 1, \"b\" : [1,2,3], \"c\" : {\"w\":9}} }'", "'$.x'"))
                .isEqualTo(3L);

        assertThat(assertions.function("json_size", "'{\"x\": {\"a\" : 1, \"b\" : 2} }'", "'$.x.a'"))
                .isEqualTo(0L);

        assertThat(assertions.function("json_size", "'[1,2,3]'", "'$'"))
                .isEqualTo(3L);

        assertThat(assertions.function("json_size", "'[1,2,3]'", "CHAR '$'"))
                .isEqualTo(3L);

        assertThat(assertions.function("json_size", "null", "'$'"))
                .isNull(BIGINT);

        assertThat(assertions.function("json_size", "'INVALID_JSON'", "'$'"))
                .isNull(BIGINT);

        assertThat(assertions.function("json_size", "'[1,2,3]'", "null"))
                .isNull(BIGINT);

        assertThat(assertions.function("json_size", "JSON '{\"x\": {\"a\" : 1, \"b\" : 2} }'", "'$'"))
                .isEqualTo(1L);

        assertThat(assertions.function("json_size", "JSON '{\"x\": {\"a\" : 1, \"b\" : 2} }'", "'$.x'"))
                .isEqualTo(2L);

        assertThat(assertions.function("json_size", "JSON '{\"x\": {\"a\" : 1, \"b\" : [1,2,3], \"c\" : {\"w\":9}} }'", "'$.x'"))
                .isEqualTo(3L);

        assertThat(assertions.function("json_size", "JSON '{\"x\": {\"a\" : 1, \"b\" : 2} }'", "'$.x.a'"))
                .isEqualTo(0L);

        assertThat(assertions.function("json_size", "JSON '[1,2,3]'", "'$'"))
                .isEqualTo(3L);

        assertThat(assertions.function("json_size", "null", "'%'"))
                .isNull(BIGINT);

        assertThat(assertions.function("json_size", "JSON '[1,2,3]'", "null"))
                .isNull(BIGINT);

        assertTrinoExceptionThrownBy(assertions.function("json_size", "'{\"\":\"\"}'", "''")::evaluate)
                .hasMessage("Invalid JSON path: ''");

        assertTrinoExceptionThrownBy(assertions.function("json_size", "'{\"\":\"\"}'", "CHAR ' '")::evaluate)
                .hasMessage("Invalid JSON path: ' '");

        assertTrinoExceptionThrownBy(assertions.function("json_size", "'{\"\":\"\"}'", "'.'")::evaluate)
                .hasMessage("Invalid JSON path: '.'");

        assertTrinoExceptionThrownBy(assertions.function("json_size", "'{\"\":\"\"}'", "'null'")::evaluate)
                .hasMessage("Invalid JSON path: 'null'");

        assertTrinoExceptionThrownBy(assertions.function("json_size", "'{\"\":\"\"}'", "'null'")::evaluate)
                .hasMessage("Invalid JSON path: 'null'");
    }
}
