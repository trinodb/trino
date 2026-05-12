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
package io.trino.sql.query;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.JSON_OUTPUT_CONVERSION_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
class TestJsonConstruction
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    void teardown()
    {
        assertions.close();
    }

    @Test
    void testDateInCollections()
    {
        for (String date : List.of("9999-12-31", "10000-01-01", "-10000-01-01")) {
            assertThat(assertions.query("SELECT json_format(CAST(ARRAY[DATE '" + date + "'] AS JSON))"))
                    .matches("VALUES VARCHAR '[\"" + date + "\"]'");
            assertThat(assertions.query("SELECT json_format(CAST(MAP(ARRAY['d'], ARRAY[DATE '" + date + "']) AS JSON))"))
                    .matches("VALUES VARCHAR '{\"d\":\"" + date + "\"}'");
            assertThat(assertions.query("SELECT json_format(CAST(CAST(ROW(DATE '" + date + "') AS ROW(d DATE)) AS JSON))"))
                    .matches("VALUES VARCHAR '{\"d\":\"" + date + "\"}'");
        }
    }

    @Test
    void testCollectionDepth()
    {
        for (String construction : List.of(
                "CAST(ARRAY[a] AS JSON)",
                "CAST(MAP(ARRAY['x'], ARRAY[a]) AS JSON)",
                "CAST(ROW(a) AS JSON)")) {
            assertThat(assertions.query("SELECT " + construction + " FROM (VALUES json_parse('" + nestedArray(1023) + "')) t(a)"))
                    .succeeds();
            assertThat(assertions.query("SELECT " + construction + " FROM (VALUES json_parse('" + nestedArray(1024) + "')) t(a)"))
                    .failure()
                    .hasErrorCode(INVALID_CAST_ARGUMENT)
                    .hasMessageContaining("maximum nesting depth of 1024");
        }
        assertThat(assertions.query("SELECT CAST(ARRAY[ARRAY[json_parse('" + nestedArray(1023) + "')]] AS JSON)"))
                .failure()
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    void testConstructorDepth()
    {
        for (String construction : List.of(
                "JSON_ARRAY('%s' FORMAT JSON RETURNING JSON)",
                "JSON_OBJECT('x': '%s' FORMAT JSON RETURNING JSON)")) {
            assertThat(assertions.query("SELECT " + construction.formatted(nestedArray(1023))))
                    .succeeds();
            assertThat(assertions.query("SELECT " + construction.formatted(nestedArray(1024))))
                    .failure()
                    .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                    .hasMessageContaining("maximum nesting depth of 1024");
        }
    }

    @Test
    void testQueryWrapperDepth()
    {
        String query = "SELECT JSON_QUERY('" + nestedArray(1024) + "', 'lax $' RETURNING JSON WITH UNCONDITIONAL ARRAY WRAPPER %s ON ERROR)";
        assertThat(assertions.query(query.formatted("ERROR")))
                .failure()
                .hasErrorCode(JSON_OUTPUT_CONVERSION_ERROR)
                .hasMessageContaining("maximum nesting depth of 1024");
        assertThat(assertions.query(query.formatted("NULL"))).matches("VALUES CAST(NULL AS JSON)");
        assertThat(assertions.query(query.formatted("EMPTY ARRAY"))).matches("VALUES JSON '[]'");
        assertThat(assertions.query(query.formatted("EMPTY OBJECT"))).matches("VALUES JSON '{}'");
        assertThat(assertions.query("SELECT JSON_QUERY('" + nestedArray(1023) + "', 'lax $' RETURNING JSON WITH UNCONDITIONAL ARRAY WRAPPER ERROR ON ERROR)"))
                .matches("VALUES JSON '" + nestedArray(1024) + "'");
        assertThat(assertions.query("SELECT JSON_QUERY('" + nestedArray(1024) + "', 'lax $' RETURNING JSON WITH CONDITIONAL ARRAY WRAPPER ERROR ON ERROR)"))
                .matches("VALUES JSON '" + nestedArray(1024) + "'");
    }

    private static String nestedArray(int depth)
    {
        return "[".repeat(depth) + "1" + "]".repeat(depth);
    }
}
