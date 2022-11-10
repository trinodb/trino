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

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestArrayMatchFunctions
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
    public void testAllMatch()
    {
        assertThat(assertions.expression("all_match(a, x -> x % 2 = 1)")
                .binding("a", "ARRAY[5, 7, 9]"))
                .isEqualTo(true);

        assertThat(assertions.expression("all_match(a, x -> x)")
                .binding("a", "ARRAY[true, false, true]"))
                .isEqualTo(false);

        assertThat(assertions.expression("all_match(a, x -> substr(x, 1, 1) = 'a')")
                .binding("a", "ARRAY['abc', 'ade', 'afg']"))
                .isEqualTo(true);

        assertThat(assertions.expression("all_match(a, x -> true)")
                .binding("a", "ARRAY[]"))
                .isEqualTo(true);

        assertThat(assertions.expression("all_match(a, x -> x)")
                .binding("a", "ARRAY[true, true, NULL]"))
                .matches("CAST(NULL AS boolean)");

        assertThat(assertions.expression("all_match(a, x -> x)")
                .binding("a", "ARRAY[true, false, NULL]"))
                .isEqualTo(false);

        assertThat(assertions.expression("all_match(a, x -> x > 1)")
                .binding("a", "ARRAY[NULL, NULL, NULL]"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("all_match(a, x -> x IS NULL)")
                .binding("a", "ARRAY[NULL, NULL, NULL]"))
                .isEqualTo(true);

        assertThat(assertions.expression("all_match(a, x -> cardinality(x) > 1)")
                .binding("a", "ARRAY[MAP(ARRAY[1,2], ARRAY[3,4]), MAP(ARRAY[1,2,3], ARRAY[3,4,5])]"))
                .isEqualTo(true);

        assertThat(assertions.expression("all_match(a, t -> month(t) = 5)")
                .binding("a", "ARRAY[TIMESTAMP '2020-05-10 12:34:56.123456789', TIMESTAMP '1111-05-10 12:34:56.123456789']"))
                .isEqualTo(true);
    }

    @Test
    public void testAnyMatch()
    {
        assertThat(assertions.expression("any_match(a, x -> x % 2 = 1)")
                .binding("a", "ARRAY[5, 8, 10]"))
                .isEqualTo(true);

        assertThat(assertions.expression("any_match(a, x -> x)")
                .binding("a", "ARRAY[false, false, false]"))
                .isEqualTo(false);

        assertThat(assertions.expression("any_match(a, x -> substr(x, 1, 1) = 'a')")
                .binding("a", "ARRAY['abc', 'def', 'ghi']"))
                .isEqualTo(true);

        assertThat(assertions.expression("any_match(a, x -> true)")
                .binding("a", "ARRAY[]"))
                .isEqualTo(false);

        assertThat(assertions.expression("any_match(a, x -> x)")
                .binding("a", "ARRAY[false, false, NULL]"))
                .matches("CAST(NULL AS boolean)");

        assertThat(assertions.expression("any_match(a, x -> x)")
                .binding("a", "ARRAY[true, false, NULL]"))
                .isEqualTo(true);

        assertThat(assertions.expression("any_match(a, x -> x > 1)")
                .binding("a", "ARRAY[NULL, NULL, NULL]"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("any_match(a, x -> x IS NULL)")
                .binding("a", "ARRAY[true, false, NULL]"))
                .isEqualTo(true);

        assertThat(assertions.expression("any_match(a, x -> cardinality(x) > 4)")
                .binding("a", "ARRAY[MAP(ARRAY[1,2], ARRAY[3,4]), MAP(ARRAY[1,2,3], ARRAY[3,4,5])]"))
                .isEqualTo(false);

        assertThat(assertions.expression("any_match(a, t -> year(t) = 2020)")
                .binding("a", "ARRAY[TIMESTAMP '2020-05-10 12:34:56.123456789', TIMESTAMP '1111-05-10 12:34:56.123456789']"))
                .isEqualTo(true);
    }

    @Test
    public void testNoneMatch()
    {
        assertThat(assertions.expression("none_match(a, x -> x % 2 = 1)")
                .binding("a", "ARRAY[5, 8, 10]"))
                .isEqualTo(false);

        assertThat(assertions.expression("none_match(a, x -> x)")
                .binding("a", "ARRAY[false, false, false]"))
                .isEqualTo(true);

        assertThat(assertions.expression("none_match(a, x -> substr(x, 1, 1) = 'a')")
                .binding("a", "ARRAY['abc', 'def', 'ghi']"))
                .isEqualTo(false);

        assertThat(assertions.expression("none_match(a, x -> true)")
                .binding("a", "ARRAY[]"))
                .isEqualTo(true);

        assertThat(assertions.expression("none_match(a, x -> x)")
                .binding("a", "ARRAY[false, false, NULL]"))
                .matches("CAST(NULL AS boolean)");

        assertThat(assertions.expression("none_match(a, x -> x)")
                .binding("a", "ARRAY[true, false, NULL]"))
                .isEqualTo(false);

        assertThat(assertions.expression("none_match(a, x -> x > 1)")
                .binding("a", "ARRAY[NULL, NULL, NULL]"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("none_match(a, x -> x IS NULL)")
                .binding("a", "ARRAY[true, false, NULL]"))
                .isEqualTo(false);

        assertThat(assertions.expression("none_match(a, x -> cardinality(x) > 4)")
                .binding("a", "ARRAY[MAP(ARRAY[1,2], ARRAY[3,4]), MAP(ARRAY[1,2,3], ARRAY[3,4,5])]"))
                .isEqualTo(true);

        assertThat(assertions.expression("none_match(a, t -> month(t) = 10)")
                .binding("a", "ARRAY[TIMESTAMP '2020-05-10 12:34:56.123456789', TIMESTAMP '1111-05-10 12:34:56.123456789']"))
                .isEqualTo(true);
    }
}
