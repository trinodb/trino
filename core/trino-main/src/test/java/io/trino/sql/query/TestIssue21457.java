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

public class TestIssue21457
{
    @Test
    public void test()
    {
        try (QueryAssertions assertions = new QueryAssertions()) {
            assertThat(assertions.query(
                    """
                    WITH
                        t(k) AS (VALUES 1, 2, 3, 4, 5),
                        t1 AS (
                            SELECT k, null AS v
                            FROM t
                            GROUP BY k
                        ),
                        t2 AS (
                            SELECT k, v
                            FROM t1
                            where k = 1
                            GROUP BY k, v
                        )
                        SELECT *
                        FROM t1 JOIN t2 ON t1.k = t2.k
                    """))
                    .matches("VALUES  (1, NULL, 1, NULL)");
        }
    }
}
