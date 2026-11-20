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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestIssue32205
{
    @Test
    public void test()
    {
        try (QueryAssertions assertions = new QueryAssertions()) {
            assertThat(assertions.query(
                    """
                    SELECT x, y
                    FROM (VALUES (1, 100), (2, 10)) t(x, y)
                    ORDER BY y
                    FETCH FIRST 2 ROWS WITH TIES
                    """))
                    .ordered()
                    .matches("VALUES (2, 10), (1, 100)");

            assertThat(assertions.query(
                    """
                    SELECT x, y
                    FROM (
                        SELECT x, y
                        FROM (VALUES (1, 100), (2, 10), (3, 20)) t(x, y)
                        ORDER BY x
                        LIMIT 2
                    ) s
                    ORDER BY y
                    FETCH FIRST 2 ROWS WITH TIES
                    """))
                    .ordered()
                    .matches("VALUES (2, 10), (1, 100)");

            assertThat(assertions.query(
                    """
                    SELECT x, y
                    FROM (
                        SELECT x, y
                        FROM (VALUES (1, 100), (2, 10), (3, 20)) t(x, y)
                        ORDER BY x
                        LIMIT 2
                    ) s
                    CROSS JOIN (VALUES 1)
                    ORDER BY y
                    FETCH FIRST 2 ROWS WITH TIES
                    """))
                    .ordered()
                    .matches("VALUES (2, 10), (1, 100)");

            assertThat(assertions.query(
                    """
                    SELECT x
                    FROM (
                        SELECT x
                        FROM (VALUES 5, 4, 3, 2, 1) t(x)
                        ORDER BY x
                        FETCH FIRST 2 ROWS WITH TIES)
                    LIMIT 1
                    """))
                    .result()
                    .onlyColumnAsSet()
                    .hasSize(1)
                    .isSubsetOf(1, 2);

            assertThat(assertions.query(
                    """
                    SELECT x
                    FROM (
                        SELECT x
                        FROM (VALUES 5, 4, 3, 2, 1) t(x)
                        ORDER BY x
                        FETCH FIRST 2 ROWS WITH TIES)
                    LIMIT 3
                    """))
                    .matches("VALUES 1, 2");

            assertThat(assertions.query(
                    """
                    SELECT x
                    FROM (
                        SELECT x, y
                        FROM (VALUES (1, 100), (2, 10), (3, 20), (4, 30), (5, 40)) t(x, y)
                        ORDER BY y
                        FETCH FIRST 2 ROWS WITH TIES)
                    LIMIT 1
                    """))
                    .result()
                    .onlyColumnAsSet()
                    .hasSize(1)
                    .isSubsetOf(2, 3);

            assertThat(assertions.query(
                    """
                    SELECT x
                    FROM (
                        SELECT x, y
                        FROM (VALUES (1, 100), (2, 90), (3, 80), (4, 10), (5, 20)) t(x, y)
                        ORDER BY y
                        FETCH FIRST 2 ROWS WITH TIES)
                    LIMIT 3
                    """))
                    .matches("VALUES 4, 5");

            assertThat(assertions.query(
                    """
                    SELECT x
                    FROM (
                        SELECT x
                        FROM UNNEST(ARRAY[5, 4, 3, 2, 1]) t(x)
                        ORDER BY x
                        FETCH FIRST 2 ROWS WITH TIES)
                    LIMIT 3000000000
                    """))
                    .matches("VALUES 1, 2");

            assertThat(assertions.query(
                    """
                    SELECT x
                    FROM (
                        SELECT x, y
                        FROM UNNEST(ARRAY[1, 2, 3], ARRAY[100, 10, 20]) t(x, y)
                        ORDER BY y
                        FETCH FIRST 2 ROWS WITH TIES)
                    LIMIT 3000000000
                    """))
                    .matches("VALUES 2, 3");
        }
    }
}
