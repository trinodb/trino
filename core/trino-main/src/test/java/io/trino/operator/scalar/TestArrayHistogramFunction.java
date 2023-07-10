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

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestArrayHistogramFunction
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
        assertThat(assertions.function("array_histogram", "ARRAY[42]"))
                .isEqualTo(Map.of(42, 1L));

        assertThat(assertions.function("array_histogram", "ARRAY[42]"))
                .isEqualTo(Map.of(42, 1L));

        assertThat(assertions.function("array_histogram", "ARRAY[42, 7]"))
                .isEqualTo(Map.of(42, 1L, 7, 1L));

        assertThat(assertions.function("array_histogram", "ARRAY[42, 42, 42]"))
                .isEqualTo(Map.of(42, 3L));

        assertThat(assertions.function("array_histogram", "ARRAY['a', 'b', 'a']"))
                .isEqualTo(Map.of("a", 2L, "b", 1L));
    }

    @Test
    public void testDuplicateKeys()
    {
        assertThat(assertions.function("array_histogram", "ARRAY[42, 42, 42]"))
                .isEqualTo(Map.of(42, 3L));

        assertThat(assertions.function("array_histogram", "ARRAY['a', 'b', 'a']"))
                .isEqualTo(Map.of("a", 2L, "b", 1L));
    }

    @Test
    public void testEmpty()
    {
        assertThat(assertions.function("array_histogram", "ARRAY[]"))
                .isEqualTo(Map.of());
    }

    @Test
    public void testNullsIgnored()
    {
        assertThat(assertions.function("array_histogram", "ARRAY[NULL]"))
                .isEqualTo(Map.of());

        assertThat(assertions.function("array_histogram", "ARRAY[42, NULL]"))
                .isEqualTo(Map.of(42, 1L));

        assertThat(assertions.function("array_histogram", "ARRAY[NULL, NULL, NULL, NULL, NULL]"))
                .isEqualTo(Map.of());
    }

    @Test
    public void testLargeArray()
    {
        assertThat(assertions.function("array_histogram", "ARRAY[42, 42, 42, 7, 1, 7, 1, 42, 7, 1, 1, 42, 7, 42, 1, 7, 2, 3, 7, 42, 42]"))
                .isEqualTo(Map.of(1, 5L, 2, 1L, 3, 1L, 7, 6L, 42, 8L));
    }

    @Test
    public void testEdgeCaseValues()
    {
        assertThat(assertions.function("array_histogram", "ARRAY[NULL, '', NULL, '']"))
                .matches("MAP(ARRAY[''], CAST(ARRAY[2] AS ARRAY(BIGINT)))");

        assertThat(assertions.function("array_histogram", "CAST(ARRAY[NULL, -0.1, 0, 0.1, -0.1, NULL, 0.0] AS ARRAY(DECIMAL(1,1)))"))
                .matches("MAP(CAST(ARRAY[-0.1, 0.0, 0.1] AS ARRAY(DECIMAL(1,1))), CAST(ARRAY[2, 2, 1] AS ARRAY(BIGINT)))");

        assertThat(assertions.function("array_histogram", "ARRAY[IPADDRESS '10.0.0.1', IPADDRESS '::ffff:a00:1']"))
                .matches("MAP(ARRAY[IPADDRESS '::ffff:a00:1'], CAST(ARRAY[2] AS ARRAY(BIGINT)))");

        assertThat(assertions.expression("transform_keys(array_histogram(a), (k, v) -> k AT TIME ZONE 'UTC')")
                             .binding("a", "ARRAY[TIMESTAMP '2001-01-01 01:00:00.000 UTC', TIMESTAMP '2001-01-01 02:00:00.000 +01:00']"))
                .matches("MAP(ARRAY[TIMESTAMP '2001-01-01 01:00:00.000 UTC'], CAST(ARRAY[2] AS ARRAY(BIGINT)))");
    }

    @Test
    public void testTypes()
    {
        assertThat(assertions.function("array_histogram", "ARRAY[true, false, true]"))
                .matches("MAP(ARRAY[true, false], CAST(ARRAY[2, 1] AS ARRAY(BIGINT)))");

        assertThat(assertions.function("array_histogram", "ARRAY[42, 7, 42]"))
                .matches("MAP(ARRAY[42, 7], CAST(ARRAY[2, 1] AS ARRAY(BIGINT)))");

        assertThat(assertions.function("array_histogram", "ARRAY[42.1, 7.7, 42.1]"))
                .matches("MAP(ARRAY[42.1, 7.7], CAST(ARRAY[2, 1] AS ARRAY(BIGINT)))");

        assertThat(assertions.function("array_histogram", "ARRAY[DECIMAL '42.1', DECIMAL '7.7', DECIMAL '42.1']"))
                .matches("MAP(ARRAY[DECIMAL '42.1', DECIMAL '7.7'], CAST(ARRAY[2, 1] AS ARRAY(BIGINT)))");

        assertThat(assertions.function("array_histogram", "ARRAY[X'42', X'77', X'42']"))
                .matches("MAP(ARRAY[X'42', X'77'], CAST(ARRAY[2, 1] AS ARRAY(BIGINT)))");

        assertThat(assertions.function("array_histogram", "ARRAY[json_object('k1': 42), json_object('k1': 7), json_object('k1': 42)]"))
                .matches("MAP(ARRAY[json_object('k1': 42), json_object('k1': 7)], CAST(ARRAY[2, 1] AS ARRAY(BIGINT)))");

        assertThat(assertions.function("array_histogram", "ARRAY[DATE '2023-01-01', DATE '2023-07-07', DATE '2023-01-01']"))
                .matches("MAP(ARRAY[DATE '2023-01-01', DATE '2023-07-07'], CAST(ARRAY[2, 1] AS ARRAY(BIGINT)))");

        assertThat(assertions.function("array_histogram", "ARRAY[TIMESTAMP '2023-01-01 00:00:42', TIMESTAMP '2023-07-07 00:00:07', TIMESTAMP '2023-01-01 00:00:42']"))
                .matches("MAP(ARRAY[TIMESTAMP '2023-01-01 00:00:42', TIMESTAMP '2023-07-07 00:00:07'], CAST(ARRAY[2, 1] AS ARRAY(BIGINT)))");

        assertThat(assertions.function("array_histogram", "ARRAY[ARRAY[42], ARRAY[42, 7], ARRAY[42]]"))
                .matches("MAP(ARRAY[ARRAY[42], ARRAY[42, 7]], CAST(ARRAY[2, 1] AS ARRAY(BIGINT)))");

        assertThat(assertions.function("array_histogram", "ARRAY[MAP(ARRAY[42], ARRAY[1]), MAP(ARRAY[42, 7], ARRAY[1, 1]), MAP(ARRAY[42], ARRAY[1])]"))
                .matches("MAP(ARRAY[MAP(ARRAY[42], ARRAY[1]), MAP(ARRAY[42, 7], ARRAY[1, 1])], CAST(ARRAY[2, 1] AS ARRAY(BIGINT)))");

        assertThat(assertions.function("array_histogram", "ARRAY[ROW(42, 42), ROW(7, 7), ROW(42, 42)]"))
                .matches("MAP(ARRAY[ROW(42, 42), ROW(7, 7)], CAST(ARRAY[2, 1] AS ARRAY(BIGINT)))");
    }
}
