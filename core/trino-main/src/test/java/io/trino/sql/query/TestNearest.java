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
public class TestNearest
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testCrossJoinNearest()
    {
        assertThat(assertions.query(
                """
                WITH
                    trades(symbol, ts) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:02'),
                            ('A', TIMESTAMP '2020-01-01 00:00:04'),
                            ('B', TIMESTAMP '2020-01-01 00:00:03')),
                    quotes(symbol, ts, price) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:01', 10),
                            ('A', TIMESTAMP '2020-01-01 00:00:03', 11),
                            ('B', TIMESTAMP '2020-01-01 00:00:02', 20))
                SELECT trades.symbol, trades.ts, quotes.price
                FROM trades
                CROSS JOIN NEAREST (
                    FROM quotes
                    WHERE quotes.symbol = trades.symbol
                    MATCH quotes.ts <= trades.ts
                )
                """))
                .matches("VALUES " +
                        "('A', TIMESTAMP '2020-01-01 00:00:02', 10), " +
                        "('A', TIMESTAMP '2020-01-01 00:00:04', 11), " +
                        "('B', TIMESTAMP '2020-01-01 00:00:03', 20)");
    }

    @Test
    public void testImplicitJoinNearest()
    {
        assertThat(assertions.query(
                """
                WITH
                    trades(symbol, ts) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:02'),
                            ('A', TIMESTAMP '2020-01-01 00:00:04'),
                            ('B', TIMESTAMP '2020-01-01 00:00:03')),
                    quotes(symbol, ts, price) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:01', 10),
                            ('A', TIMESTAMP '2020-01-01 00:00:03', 11),
                            ('B', TIMESTAMP '2020-01-01 00:00:02', 20))
                SELECT trades.symbol, trades.ts, quotes.price
                FROM trades,
                     NEAREST (
                         FROM quotes
                         WHERE quotes.symbol = trades.symbol
                         MATCH quotes.ts <= trades.ts
                     )
                """))
                .matches("VALUES " +
                        "('A', TIMESTAMP '2020-01-01 00:00:02', 10), " +
                        "('A', TIMESTAMP '2020-01-01 00:00:04', 11), " +
                        "('B', TIMESTAMP '2020-01-01 00:00:03', 20)");
    }

    @Test
    public void testInnerJoinNearest()
    {
        assertThat(assertions.query(
                """
                WITH
                    trades(symbol, ts) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:02'),
                            ('A', TIMESTAMP '2020-01-01 00:00:04'),
                            ('B', TIMESTAMP '2020-01-01 00:00:03')),
                    quotes(symbol, ts, price) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:01', 10),
                            ('A', TIMESTAMP '2020-01-01 00:00:03', 11),
                            ('B', TIMESTAMP '2020-01-01 00:00:02', 20))
                SELECT trades.symbol, trades.ts, quotes.price
                FROM trades
                INNER JOIN NEAREST (
                    FROM quotes
                    WHERE quotes.symbol = trades.symbol
                    MATCH quotes.ts <= trades.ts
                ) ON TRUE
                """))
                .matches("VALUES " +
                        "('A', TIMESTAMP '2020-01-01 00:00:02', 10), " +
                        "('A', TIMESTAMP '2020-01-01 00:00:04', 11), " +
                        "('B', TIMESTAMP '2020-01-01 00:00:03', 20)");
    }

    @Test
    public void testLeftJoinNearest()
    {
        assertThat(assertions.query(
                """
                WITH
                    trades(symbol, ts) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:02'),
                            ('C', TIMESTAMP '2020-01-01 00:00:01')),
                    quotes(symbol, ts, price) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:01', 10))
                SELECT trades.symbol, quotes.price
                FROM trades
                LEFT JOIN NEAREST (
                    FROM quotes
                    WHERE quotes.symbol = trades.symbol
                    MATCH quotes.ts < trades.ts
                ) ON TRUE
                """))
                .matches("VALUES ('A', 10), ('C', CAST(null AS integer))");
    }

    @Test
    public void testCrossJoinNearestForward()
    {
        assertThat(assertions.query(
                """
                WITH
                    trades(symbol, ts) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:02'),
                            ('A', TIMESTAMP '2020-01-01 00:00:04'),
                            ('B', TIMESTAMP '2020-01-01 00:00:01')),
                    quotes(symbol, ts, price) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:03', 11),
                            ('A', TIMESTAMP '2020-01-01 00:00:05', 12),
                            ('B', TIMESTAMP '2020-01-01 00:00:01', 20))
                SELECT trades.symbol, trades.ts, quotes.price
                FROM trades
                CROSS JOIN NEAREST (
                    FROM quotes
                    WHERE quotes.symbol = trades.symbol
                    MATCH quotes.ts > trades.ts
                )
                """))
                .matches("VALUES " +
                        "('A', TIMESTAMP '2020-01-01 00:00:02', 11), " +
                        "('A', TIMESTAMP '2020-01-01 00:00:04', 12)");
    }

    @Test
    public void testLeftJoinNearestForward()
    {
        assertThat(assertions.query(
                """
                WITH
                    trades(symbol, ts) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:03'),
                            ('B', TIMESTAMP '2020-01-01 00:00:01'),
                            ('C', TIMESTAMP '2020-01-01 00:00:02')),
                    quotes(symbol, ts, price) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:03', 10),
                            ('B', TIMESTAMP '2020-01-01 00:00:02', 20))
                SELECT trades.symbol, quotes.price
                FROM trades
                LEFT JOIN NEAREST (
                    FROM quotes
                    WHERE quotes.symbol = trades.symbol
                    MATCH quotes.ts >= trades.ts
                ) ON TRUE
                """))
                .matches("VALUES ('A', 10), ('B', 20), ('C', CAST(null AS integer))");
    }

    @Test
    public void testCrossJoinNearestWithConstantAnchor()
    {
        assertThat(assertions.query(
                """
                WITH
                    trades(id) AS (
                        VALUES
                            1,
                            2),
                    quotes(ts, price) AS (
                        VALUES
                            (TIMESTAMP '2020-01-01 00:00:01', 10),
                            (TIMESTAMP '2020-01-01 00:00:03', 11))
                SELECT trades.id, quotes.price
                FROM trades
                CROSS JOIN NEAREST (
                    FROM quotes
                    MATCH quotes.ts <= TIMESTAMP '2020-01-01 00:00:02'
                )
                """))
                .matches("VALUES (1, 10), (2, 10)");
    }

    @Test
    public void testImplicitJoinNearestWithConstantAnchor()
    {
        assertThat(assertions.query(
                """
                WITH
                    trades(id) AS (
                        VALUES
                            1,
                            2),
                    quotes(ts, price) AS (
                        VALUES
                            (TIMESTAMP '2020-01-01 00:00:01', 10),
                            (TIMESTAMP '2020-01-01 00:00:03', 11))
                SELECT trades.id, quotes.price
                FROM trades,
                     NEAREST (
                         FROM quotes
                         MATCH quotes.ts <= TIMESTAMP '2020-01-01 00:00:02'
                     )
                """))
                .matches("VALUES (1, 10), (2, 10)");
    }

    @Test
    public void testLeftJoinNearestWithNullMatchKeys()
    {
        assertThat(assertions.query(
                """
                WITH
                    trades(symbol, ts) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:02'),
                            ('B', CAST(NULL AS timestamp)),
                            ('C', TIMESTAMP '2020-01-01 00:00:01')),
                    quotes(symbol, ts, price) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:01', 10),
                            ('B', TIMESTAMP '2020-01-01 00:00:01', 20),
                            ('C', CAST(NULL AS timestamp), 30))
                SELECT trades.symbol, quotes.price
                FROM trades
                LEFT JOIN NEAREST (
                    FROM quotes
                    WHERE quotes.symbol = trades.symbol
                    MATCH quotes.ts <= trades.ts
                ) ON TRUE
                """))
                .matches("VALUES ('A', 10), ('B', CAST(null AS integer)), ('C', CAST(null AS integer))");
    }

    @Test
    public void testLeftJoinNearestWithNullOuterMatchKey()
    {
        assertThat(assertions.query(
                """
                WITH
                    trades(symbol, ts) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:02'),
                            ('B', CAST(NULL AS timestamp))),
                    quotes(symbol, ts, price) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:01', 10),
                            ('B', TIMESTAMP '2020-01-01 00:00:01', 20))
                SELECT trades.symbol, quotes.price
                FROM trades
                LEFT JOIN NEAREST (
                    FROM quotes
                    WHERE quotes.symbol = trades.symbol
                    MATCH quotes.ts <= trades.ts
                ) ON TRUE
                """))
                .matches("VALUES ('A', 10), ('B', CAST(null AS integer))");
    }

    @Test
    public void testLeftJoinNearestWithNullCandidateMatchKey()
    {
        assertThat(assertions.query(
                """
                WITH
                    trades(symbol, ts) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:02'),
                            ('B', TIMESTAMP '2020-01-01 00:00:02')),
                    quotes(symbol, ts, price) AS (
                        VALUES
                            ('A', CAST(NULL AS timestamp), 99),
                            ('A', TIMESTAMP '2020-01-01 00:00:01', 10),
                            ('B', CAST(NULL AS timestamp), 20))
                SELECT trades.symbol, quotes.price
                FROM trades
                LEFT JOIN NEAREST (
                    FROM quotes
                    WHERE quotes.symbol = trades.symbol
                    MATCH quotes.ts <= trades.ts
                ) ON TRUE
                """))
                .matches("VALUES ('A', 10), ('B', CAST(null AS integer))");
    }

    @Test
    public void testCrossJoinNearestWithNullMatchKeys()
    {
        assertThat(assertions.query(
                """
                WITH
                    trades(symbol, ts) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:02'),
                            ('B', CAST(NULL AS timestamp)),
                            ('C', TIMESTAMP '2020-01-01 00:00:02')),
                    quotes(symbol, ts, price) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:01', 10),
                            ('B', TIMESTAMP '2020-01-01 00:00:01', 20),
                            ('C', CAST(NULL AS timestamp), 30))
                SELECT trades.symbol, quotes.price
                FROM trades
                CROSS JOIN NEAREST (
                    FROM quotes
                    WHERE quotes.symbol = trades.symbol
                    MATCH quotes.ts <= trades.ts
                )
                """))
                .matches("VALUES ('A', 10)");
    }

    @Test
    public void testCrossJoinNearestWithNullWherePredicate()
    {
        assertThat(assertions.query(
                """
                WITH
                    trades(symbol, ts) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:02'),
                            (CAST(NULL AS varchar), TIMESTAMP '2020-01-01 00:00:02')),
                    quotes(symbol, ts, price) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:01', 10),
                            (CAST(NULL AS varchar), TIMESTAMP '2020-01-01 00:00:01', 20))
                SELECT trades.symbol, quotes.price
                FROM trades
                CROSS JOIN NEAREST (
                    FROM quotes
                    WHERE quotes.symbol = trades.symbol
                    MATCH quotes.ts <= trades.ts
                )
                """))
                .matches("VALUES (CAST('A' AS varchar), 10)");
    }

    @Test
    public void testCrossJoinNearestWithoutWhere()
    {
        assertThat(assertions.query(
                """
                WITH
                    trades(id, ts) AS (
                        VALUES
                            (1, TIMESTAMP '2020-01-01 00:00:02'),
                            (2, TIMESTAMP '2020-01-01 00:00:04')),
                    quotes(ts, price) AS (
                        VALUES
                            (TIMESTAMP '2020-01-01 00:00:01', 10),
                            (TIMESTAMP '2020-01-01 00:00:03', 11))
                SELECT trades.id, quotes.price
                FROM trades
                CROSS JOIN NEAREST (
                    FROM quotes
                    MATCH quotes.ts <= trades.ts
                )
                """))
                .matches("VALUES (1, 10), (2, 11)");
    }

    @Test
    public void testLeftJoinNearestWithoutWhere()
    {
        assertThat(assertions.query(
                """
                WITH
                    trades(id, ts) AS (
                        VALUES
                            (1, TIMESTAMP '2020-01-01 00:00:00'),
                            (2, TIMESTAMP '2020-01-01 00:00:04')),
                    quotes(ts, price) AS (
                        VALUES
                            (TIMESTAMP '2020-01-01 00:00:01', 10),
                            (TIMESTAMP '2020-01-01 00:00:03', 11))
                SELECT trades.id, quotes.price
                FROM trades
                LEFT JOIN NEAREST (
                    FROM quotes
                    MATCH quotes.ts <= trades.ts
                ) ON TRUE
                """))
                .matches("VALUES (1, CAST(null AS integer)), (2, 11)");
    }

    @Test
    public void testCrossJoinNearestWithDuplicateCandidates()
    {
        assertThat(assertions.query(
                """
                WITH
                    trades(symbol, ts) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:04'),
                            ('B', TIMESTAMP '2020-01-01 00:00:03')),
                    quotes(symbol, ts, price) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:03', 11),
                            ('A', TIMESTAMP '2020-01-01 00:00:03', 11),
                            ('B', TIMESTAMP '2020-01-01 00:00:02', 20),
                            ('B', TIMESTAMP '2020-01-01 00:00:02', 20))
                SELECT trades.symbol, quotes.price
                FROM trades
                CROSS JOIN NEAREST (
                    FROM quotes
                    WHERE quotes.symbol = trades.symbol
                    MATCH quotes.ts <= trades.ts
                )
                """))
                .matches("VALUES ('A', 11), ('B', 20)");
    }

    @Test
    public void testCrossJoinNearestWithSubqueriesInPredicates()
    {
        assertThat(assertions.query(
                """
                WITH
                    trades(symbol, ts) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:02'),
                            ('A', TIMESTAMP '2020-01-01 00:00:04')),
                    quotes(symbol, ts, price) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:01', 10),
                            ('A', TIMESTAMP '2020-01-01 00:00:03', 11))
                SELECT quotes.price
                FROM trades
                CROSS JOIN NEAREST (
                    FROM quotes
                    WHERE quotes.symbol = trades.symbol
                        AND quotes.price >= (SELECT 10)
                    MATCH quotes.ts <= date_add('second', (SELECT 0), trades.ts)
                )
                """))
                .matches("VALUES 10, 11");
    }

    @Test
    public void testCrossJoinNearestWithTies()
    {
        assertThat(assertions.query(
                """
                WITH
                    trades(symbol, ts) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:04')),
                    quotes(symbol, ts, price) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:03', 11),
                            ('A', TIMESTAMP '2020-01-01 00:00:03', 12),
                            ('A', TIMESTAMP '2020-01-01 00:00:01', 10))
                SELECT count(*), min(quotes.ts), max(quotes.ts)
                FROM trades
                CROSS JOIN NEAREST (
                    FROM quotes
                    WHERE quotes.symbol = trades.symbol
                    MATCH quotes.ts <= trades.ts
                )
                """))
                .matches("VALUES (BIGINT '1', TIMESTAMP '2020-01-01 00:00:03', TIMESTAMP '2020-01-01 00:00:03')");
    }

    @Test
    public void testLeftJoinNearestWithTies()
    {
        assertThat(assertions.query(
                """
                WITH
                    trades(symbol, ts) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:04'),
                            ('B', TIMESTAMP '2020-01-01 00:00:01')),
                    quotes(symbol, ts, price) AS (
                        VALUES
                            ('A', TIMESTAMP '2020-01-01 00:00:03', 11),
                            ('A', TIMESTAMP '2020-01-01 00:00:03', 12))
                SELECT trades.symbol, count(quotes.price), min(quotes.ts), max(quotes.ts)
                FROM trades
                LEFT JOIN NEAREST (
                    FROM quotes
                    WHERE quotes.symbol = trades.symbol
                    MATCH quotes.ts <= trades.ts
                ) ON TRUE
                GROUP BY trades.symbol
                """))
                .matches("VALUES " +
                        "('A', BIGINT '1', CAST(TIMESTAMP '2020-01-01 00:00:03' AS timestamp(0)), CAST(TIMESTAMP '2020-01-01 00:00:03' AS timestamp(0))), " +
                        "('B', BIGINT '0', CAST(null AS timestamp(0)), CAST(null AS timestamp(0)))");
    }

    @Test
    public void testJoinNearestAliasing()
    {
        assertThat(assertions.query(
                """
                WITH
                    trades(symbol, ts) AS (
                        VALUES ('A', TIMESTAMP '2020-01-01 00:00:02')),
                    quotes(symbol, ts, price) AS (
                        VALUES ('A', TIMESTAMP '2020-01-01 00:00:01', 10))
                SELECT t.symbol, q_price
                FROM trades t
                CROSS JOIN NEAREST (
                    FROM quotes
                    WHERE quotes.symbol = t.symbol
                    MATCH quotes.ts <= t.ts
                ) q(q_symbol, q_ts, q_price)
                """))
                .matches("VALUES ('A', 10)");
    }
}
