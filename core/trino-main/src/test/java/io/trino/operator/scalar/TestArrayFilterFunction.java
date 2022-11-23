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

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestArrayFilterFunction
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
    public void testBasic()
    {
        assertThat(assertions.expression("filter(a, x -> x = 5)")
                .binding("a", "ARRAY[5, 6]"))
                .matches("ARRAY[5]");

        assertThat(assertions.expression("filter(a, x -> x = 5)")
                .binding("a", "ARRAY[5 + random(1), 6 + random(1)]"))
                .matches("ARRAY[5]");

        assertThat(assertions.expression("filter(a, x -> nullif(x, false))")
                .binding("a", "ARRAY[true, false, true, false]"))
                .matches("ARRAY[true, true]");

        assertThat(assertions.expression("filter(a, x -> not x)")
                .binding("a", "ARRAY[true, false, null, true, false, null]"))
                .matches("ARRAY[false, false]");

        assertThat(assertions.expression("filter(a, t -> year(t) = 1111)")
                .binding("a", "ARRAY[TIMESTAMP '2020-05-10 12:34:56.123456789', TIMESTAMP '1111-05-10 12:34:56.123456789']"))
                .matches("ARRAY[TIMESTAMP '1111-05-10 12:34:56.123456789']");
    }

    @Test
    public void testEmpty()
    {
        assertThat(assertions.expression("filter(a, x -> true)")
                .binding("a", "ARRAY[]"))
                .matches("ARRAY[]");

        assertThat(assertions.expression("filter(a, x -> false)")
                .binding("a", "ARRAY[]"))
                .matches("ARRAY[]");

        assertThat(assertions.expression("filter(a, x -> CAST(null AS boolean))")
                .binding("a", "ARRAY[]"))
                .matches("ARRAY[]");

        assertThat(assertions.expression("filter(a, x -> true)")
                .binding("a", "CAST(ARRAY[] AS array(integer))"))
                .matches("CAST(ARRAY[] AS array(integer))");
    }

    @Test
    public void testNull()
    {
        assertThat(assertions.expression("filter(a, x -> x IS NULL)")
                .binding("a", "ARRAY[NULL]"))
                .matches("ARRAY[NULL]");

        assertThat(assertions.expression("filter(a, x -> x IS NOT NULL)")
                .binding("a", "ARRAY[NULL]"))
                .matches("ARRAY[]");

        assertThat(assertions.expression("filter(a, x -> x IS NULL)")
                .binding("a", "ARRAY[CAST(NULL AS integer)]"))
                .matches("CAST(ARRAY[NULL] AS array(integer))");

        assertThat(assertions.expression("filter(a, x -> x IS NULL)")
                .binding("a", "ARRAY[NULL, NULL, NULL]"))
                .matches("ARRAY[NULL, NULL, NULL]");

        assertThat(assertions.expression("filter(a, x -> x IS NOT NULL)")
                .binding("a", "ARRAY[NULL, NULL, NULL]"))
                .matches("ARRAY[]");

        assertThat(assertions.expression("filter(a, x -> x % 2 = 1 OR x IS NULL)")
                .binding("a", "ARRAY[25, 26, NULL]"))
                .matches("ARRAY[25, NULL]");

        assertThat(assertions.expression("filter(a, x -> x < 30.0E0 OR x IS NULL)")
                .binding("a", "ARRAY[25.6E0, 37.3E0, NULL]"))
                .matches("ARRAY[25.6E0, NULL]");

        assertThat(assertions.expression("filter(a, x -> NOT x OR x IS NULL)")
                .binding("a", "ARRAY[true, false, NULL]"))
                .matches("ARRAY[false, NULL]");

        assertThat(assertions.expression("filter(a, x -> substr(x, 1, 1) = 'a' OR x IS NULL)")
                .binding("a", "ARRAY['abc', 'def', NULL]"))
                .matches("ARRAY['abc', NULL]");

        assertThat(assertions.expression("filter(a, x -> x[2] IS NULL OR x IS NULL)")
                .binding("a", "ARRAY[ARRAY['abc', NULL, '123']]"))
                .matches("ARRAY[ARRAY['abc', NULL, '123']]");
    }

    @Test
    public void testTypeCombinations()
    {
        assertThat(assertions.expression("filter(a, x -> x % 2 = 1)")
                .binding("a", "ARRAY[25, 26, 27]"))
                .matches("ARRAY[25, 27]");

        assertThat(assertions.expression("filter(a, x -> x < 30.0E0)")
                .binding("a", "ARRAY[25.6E0, 37.3E0, 28.6E0]"))
                .matches("ARRAY[25.6E0, 28.6E0]");

        assertThat(assertions.expression("filter(a, x -> NOT x)")
                .binding("a", "ARRAY[true, false, true]"))
                .matches("ARRAY[false]");

        assertThat(assertions.expression("filter(a, x -> substr(x, 1, 1) = 'a' OR x IS NULL)")
                .binding("a", "ARRAY['abc', 'def', 'ayz']"))
                .matches("ARRAY['abc', 'ayz']");

        assertThat(assertions.expression("filter(a, x -> x[2] IS NULL)")
                .binding("a", "ARRAY[ARRAY['abc', NULL, '123'], ARRAY ['def', 'x', '456']]"))
                .matches("ARRAY[ARRAY['abc', NULL, '123']]");
    }
}
