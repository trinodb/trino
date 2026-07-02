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
final class TestQualify
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    void teardown()
    {
        assertions.close();
    }

    @Test
    void testQualifyWithWindowRankFunction()
    {
        // keep only the highest-priced product per category
        assertThat(assertions.query(
                """
                SELECT category, product, price
                FROM (VALUES
                    ('A', 'apple',  10),
                    ('A', 'banana', 20),
                    ('B', 'cherry', 5),
                    ('B', 'date',   15)
                ) AS t(category, product, price)
                QUALIFY rank() OVER (PARTITION BY category ORDER BY price DESC) = 1
                """))
                .matches(
                        """
                        VALUES
                            (CAST('A' AS VARCHAR(1)), CAST('banana' AS VARCHAR(6)), 20),
                            (CAST('B' AS VARCHAR(1)), CAST('date' AS VARCHAR(6)),   15)
                        """);
    }

    @Test
    void testQualifyWithWindowAggregateFunction()
    {
        // keep rows where price exceeds category average
        assertThat(assertions.query(
                """
                SELECT category, price
                FROM (VALUES
                    ('A', 10),
                    ('A', 30),
                    ('B', 5),
                    ('B', 15)
                ) AS t(category, price)
                QUALIFY price > avg(price) OVER (PARTITION BY category)
                """))
                .matches(
                        """
                        VALUES
                            (CAST('A' AS VARCHAR(1)), 30),
                            (CAST('B' AS VARCHAR(1)), 15)
                        """);
    }

    @Test
    void testQualifyWithRowNumber()
    {
        // top-N per group
        assertThat(assertions.query(
                """
                SELECT category, price
                FROM (VALUES
                    ('A', 10),
                    ('A', 20),
                    ('A', 30),
                    ('B', 5),
                    ('B', 15)
                ) AS t(category, price)
                QUALIFY row_number() OVER (PARTITION BY category ORDER BY price DESC) <= 2
                """))
                .matches(
                        """
                        VALUES
                            (CAST('A' AS VARCHAR(1)), 30),
                            (CAST('A' AS VARCHAR(1)), 20),
                            (CAST('B' AS VARCHAR(1)), 15),
                            (CAST('B' AS VARCHAR(1)), 5)
                        """);
    }

    @Test
    void testQualifyWithHavingAndGroupBy()
    {
        // QUALIFY after GROUP BY + HAVING
        assertThat(assertions.query(
                """
                SELECT category, sum(price) AS total
                FROM (VALUES
                    ('A', 10),
                    ('A', 20),
                    ('B', 5),
                    ('B', 15),
                    ('C', 100)
                ) AS t(category, price)
                GROUP BY category
                HAVING sum(price) > 10
                QUALIFY rank() OVER (ORDER BY sum(price) DESC) <= 2
                """))
                .matches(
                        """
                        VALUES
                            (CAST('C' AS VARCHAR(1)), BIGINT '100'),
                            (CAST('A' AS VARCHAR(1)), BIGINT '30')
                        """);
    }

    @Test
    void testQualifyWithWhere()
    {
        // WHERE filters before window, QUALIFY filters after
        assertThat(assertions.query(
                """
                SELECT category, price
                FROM (VALUES
                    ('A', 10),
                    ('A', 20),
                    ('A', 30),
                    ('B', 5),
                    ('B', 15)
                ) AS t(category, price)
                WHERE price > 5
                QUALIFY row_number() OVER (PARTITION BY category ORDER BY price DESC) = 1
                """))
                .matches(
                        """
                        VALUES
                            (CAST('A' AS VARCHAR(1)), 30),
                            (CAST('B' AS VARCHAR(1)), 15)
                        """);
    }

    @Test
    void testQualifyTypeMismatch()
    {
        assertThat(assertions.query(
                """
                SELECT a
                FROM (VALUES 1) AS t(a)
                QUALIFY a
                """))
                .failure()
                .hasMessageContaining("QUALIFY clause must evaluate to a boolean");
    }
}
