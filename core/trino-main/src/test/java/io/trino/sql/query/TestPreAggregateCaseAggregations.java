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
import org.junit.jupiter.api.parallel.Execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestPreAggregateCaseAggregations
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testCastExpression()
    {
        assertThat(assertions.query("SELECT " +
                "MAX(CASE WHEN c1 = 1 THEN CAST(c2 AS int) END) AS m1, " +
                "MAX(CASE WHEN c1 = 2 THEN c2 END) AS m2, " +
                "MAX(CASE WHEN c1 = 3 THEN c2 END) AS m3, " +
                "MAX(CASE WHEN c1 = 4 THEN c2 END) AS m4 " +
                "FROM (VALUES (1, '1'), (2, '2'), (3, '3'), (4, 'd')) t(c1, c2)"))
                .matches("VALUES (1, '2', '3', 'd')");

        assertThat(assertions.query("SELECT " +
                "MAX(CAST(CASE WHEN c1 = 1 THEN C2 END AS INT)) AS m1, " +
                "MAX(CASE WHEN c1 = 2 THEN c2 END) AS m2, " +
                "MAX(CASE WHEN c1 = 3 THEN c2 END) AS m3, " +
                "MAX(CASE WHEN c1 = 4 THEN c2 END) AS m4 " +
                "FROM (VALUES (1, '1'), (2, '2'), (3, '3'), (4, 'd')) t(c1, c2)"))
                .matches("VALUES (1, '2', '3', 'd')");

        assertThat(assertions.query("SELECT " +
                "CAST(MAX(CASE WHEN c1 = 1 THEN C2 END) AS INT) AS m1, " +
                "MAX(CASE WHEN c1 = 2 THEN c2 END) AS m2, " +
                "MAX(CASE WHEN c1 = 3 THEN c2 END) AS m3, " +
                "MAX(CASE WHEN c1 = 4 THEN c2 END) AS m4 " +
                "FROM (VALUES (1, '1'), (2, '2'), (3, '3'), (4, 'd')) t(c1, c2)"))
                .matches("VALUES (1, '2', '3', 'd')");
    }

    @Test
    public void testDivisionByZero()
    {
        assertThat(assertions.query("SELECT " +
                "MAX(CASE WHEN c1 = '1' THEN 10 / c2 END) AS m1, " +
                "MAX(CASE WHEN c1 = '2' THEN 10 / c2 END) AS m2, " +
                "MAX(CASE WHEN c1 = '3' THEN c2 END) AS m3, " +
                "MAX(CASE WHEN c1 = '4' THEN c2 END) AS m4 " +
                "FROM (VALUES ('1', 1), ('2', 2), ('3', 3), ('4', 0)) t(c1, c2)"))
                .matches("VALUES (10, 5, 3, 0)");
    }

    @Test
    public void testNonDeterministicCondition()
    {
        assertThat(assertions.query(
                """
                SELECT
                    sum(CASE WHEN k = 1 AND random() < 0.5 THEN 1 END) BETWEEN 1 AND 3332,
                    sum(CASE WHEN k = 1 THEN 1 END),
                    max(CASE WHEN k = 2 THEN 1 END),
                    min(CASE WHEN k = 2 THEN 1 END)
                FROM (SELECT x % 3 AS k FROM UNNEST(sequence(1, 9999)) t(x))
                """))
                .matches("VALUES (true, BIGINT '3333', 1, 1)");
    }

    @Test
    public void testNonDeterministicResult()
    {
        assertThat(assertions.query(
                """
                SELECT
                    sum(CASE WHEN k = 1 THEN random() END) = sum(CASE WHEN k IN (1, 100) THEN random() END),
                    sum(CASE WHEN k = 0 THEN random() END) IS NOT NULL,
                    sum(CASE WHEN k = 2 THEN random() END) IS NOT NULL,
                    sum(CASE WHEN k = 3 THEN random() END) IS NULL
                FROM (SELECT x % 3 AS k FROM UNNEST(sequence(1, 9999)) t(x))
                """))
                .matches("VALUES (false, true, true, true)");
    }
}
