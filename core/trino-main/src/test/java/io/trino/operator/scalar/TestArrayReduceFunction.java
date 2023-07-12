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

import io.trino.spi.type.ArrayType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestArrayReduceFunction
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
    public void testEmpty()
    {
        assertThat(assertions.expression("reduce(a, CAST (0 AS BIGINT), (s, x) -> s + x, s -> s)")
                .binding("a", "ARRAY []"))
                .isEqualTo(0L);
    }

    @Test
    public void testBasic()
    {
        assertThat(assertions.expression("reduce(a, CAST (0 AS BIGINT), (s, x) -> s + x, s -> s)")
                .binding("a", "ARRAY [5, 20, 50]"))
                .isEqualTo(75L);
        assertThat(assertions.expression("reduce(a, CAST (0 AS BIGINT), (s, x) -> s + x, s -> s)")
                .binding("a", "ARRAY [5 + RANDOM(1), 20, 50]"))
                .isEqualTo(75L);
        assertThat(assertions.expression("reduce(a, 0.0E0, (s, x) -> s + x, s -> s)")
                .binding("a", "ARRAY [5, 6, 10, 20]"))
                .isEqualTo(41.0);
    }

    @Test
    public void testNulls()
    {
        assertThat(assertions.expression("reduce(a, CAST (0 AS BIGINT), (s, x) -> s + x, s -> s)")
                .binding("a", "ARRAY [NULL]"))
                .isNull(BIGINT);
        assertThat(assertions.expression("reduce(a, CAST (0 AS BIGINT), (s, x) -> coalesce(x, 1) + s, s -> s)")
                .binding("a", "ARRAY [NULL, NULL, NULL]"))
                .isEqualTo(3L);
        assertThat(assertions.expression("reduce(a, CAST (0 AS BIGINT), (s, x) -> s + x, s -> s)")
                .binding("a", "ARRAY [5, NULL, 50]"))
                .isNull(BIGINT);
        assertThat(assertions.expression("reduce(a, CAST (0 AS BIGINT), (s, x) -> coalesce(x, 0) + s, s -> s)")
                .binding("a", "ARRAY [5, NULL, 50]"))
                .isEqualTo(55L);

        // mimics max function
        assertThat(assertions.expression("reduce(a, CAST (NULL AS BIGINT), (s, x) -> IF(s IS NULL OR x > s, x, s), s -> s)")
                .binding("a", "ARRAY []"))
                .isNull(BIGINT);
        assertThat(assertions.expression("reduce(a, CAST (NULL AS BIGINT), (s, x) -> IF(s IS NULL OR x > s, x, s), s -> s)")
                .binding("a", "ARRAY [NULL]"))
                .isNull(BIGINT);
        assertThat(assertions.expression("reduce(a, CAST (NULL AS BIGINT), (s, x) -> IF(s IS NULL OR x > s, x, s), s -> s)")
                .binding("a", "ARRAY [NULL, NULL, NULL]"))
                .isNull(BIGINT);
        assertThat(assertions.expression("reduce(a, CAST (NULL AS BIGINT), (s, x) -> IF(s IS NULL OR x > s, x, s), s -> s)")
                .binding("a", "ARRAY [NULL, 6, 10, NULL, 8]"))
                .isEqualTo(10L);
        assertThat(assertions.expression("reduce(a, CAST (NULL AS BIGINT), (s, x) -> IF(s IS NULL OR x > s, x, s), s -> s)")
                .binding("a", "ARRAY [5, NULL, 6, 10, NULL, 8]"))
                .isEqualTo(10L);
    }

    @Test
    public void testTwoValueState()
    {
        assertThat(assertions.expression("""
                        reduce(
                            a,
                            CAST(ROW(0, 0) AS ROW(sum BIGINT, count INTEGER)),
                            (s, x) -> CAST(ROW(x + s[1], s[2] + 1) AS ROW(sum BIGINT, count INTEGER)),
                            s -> s[1] / s[2])
                        """)
                .binding("a", "ARRAY [5, 20, 50]"))
                .isEqualTo(25L);
        assertThat(assertions.expression("""
                        reduce(
                                a,
                                CAST(ROW(0.0E0, 0) AS ROW(sum DOUBLE, count INTEGER)),
                                (s, x) -> CAST(ROW(x + s[1], s[2] + 1) AS ROW(sum DOUBLE, count INTEGER)),
                                s -> s[1] / s[2])
                        """)
                .binding("a", "ARRAY [5, 6, 10, 20]"))
                .isEqualTo(10.25);
    }

    @Test
    public void testInstanceFunction()
    {
        assertThat(assertions.expression("reduce(a, CAST(ARRAY[] AS ARRAY(INTEGER)), (s, x) -> concat(s, x), s -> s)")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, 4], ARRAY[5, NULL, 7]]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(asList(1, 2, 3, 4, 5, null, 7));
    }

    @Test
    public void testCoercion()
    {
        assertThat(assertions.expression("reduce(a, 0, (s, x) -> s + coalesce(x, 0), s -> s)")
                .binding("a", "ARRAY [123456789012345, NULL, 54321]"))
                .isEqualTo(123456789066666L);

        // TODO: Support coercion of return type of lambda
        assertTrinoExceptionThrownBy(assertions.expression("reduce(ARRAY [1, NULL, 2], 0, (s, x) -> CAST (s + x AS TINYINT), s -> s)")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND);
    }
}
