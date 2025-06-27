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
package io.trino.tests;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.NONE;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestAsofJoinQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
    {
        QueryRunner queryRunner = new io.trino.testing.StandaloneQueryRunner(testSessionBuilder()
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.toString())
                .build());

        queryRunner.installPlugin(new io.trino.plugin.tpch.TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", com.google.common.collect.ImmutableMap.of("tpch.splits-per-node", "1"));

        return queryRunner;
    }

    @org.junit.jupiter.api.Test
    public void testRightLteLeft()
    {
        assertQuery(
                """
                WITH
                  left_t(k, ts, v) AS (
                    VALUES
                      (1, DATE '1992-01-01', 10),
                      (1, DATE '1992-01-03', 20),
                      (2, DATE '1992-01-02', 30),
                      (3, DATE '1992-01-01', 40)  -- no match case
                  ),
                  right_t(k, ts, x) AS (
                    VALUES
                      (1, DATE '1992-01-01', 100),
                      (1, DATE '1992-01-02', 200),
                      (2, DATE '1992-01-01', 300)
                  )
                SELECT l.k, l.ts, r.x
                FROM left_t l ASOF JOIN right_t r
                  ON l.k = r.k AND r.ts <= l.ts
                ORDER BY l.k, l.ts""",
                """
                VALUES
                  (1, DATE '1992-01-01', 100),
                  (1, DATE '1992-01-03', 200),
                  (2, DATE '1992-01-02', 300),
                  (3, DATE '1992-01-01', CAST(NULL AS INTEGER))
                """);
    }

    @org.junit.jupiter.api.Test
    public void testLeftGteRight()
    {
        assertQuery(
                """
                WITH
                  left_t(k, ts, v) AS (
                    VALUES
                      (1, DATE '1992-01-01', 10),
                      (1, DATE '1992-01-03', 20),
                      (2, DATE '1992-01-02', 30),
                      (3, DATE '1992-01-01', 40)  -- no match case
                  ),
                  right_t(k, ts, x) AS (
                    VALUES
                      (1, DATE '1992-01-01', 100),
                      (1, DATE '1992-01-02', 200),
                      (2, DATE '1992-01-01', 300)
                  )
                SELECT l.k, l.ts, r.x
                FROM left_t l ASOF JOIN right_t r
                  ON l.k = r.k AND l.ts >= r.ts
                ORDER BY l.k, l.ts""",
                """
                VALUES
                  (1, DATE '1992-01-01', 100),
                  (1, DATE '1992-01-03', 200),
                  (2, DATE '1992-01-02', 300),
                  (3, DATE '1992-01-01', CAST(NULL AS INTEGER))
                """);
    }

    @org.junit.jupiter.api.Test
    public void testRightLtLeft()
    {
        assertQuery(
                """
                WITH
                  left_t(k, ts, v) AS (
                    VALUES
                      (1, DATE '1992-01-01', 10), -- no match case
                      (1, DATE '1992-01-03', 20),
                      (2, DATE '1992-01-02', 30),
                      (3, DATE '1992-01-01', 40)  -- no match case
                  ),
                  right_t(k, ts, x) AS (
                    VALUES
                      (1, DATE '1992-01-01', 100),
                      (1, DATE '1992-01-02', 200),
                      (2, DATE '1992-01-01', 300)
                  )
                SELECT l.k, l.ts, r.x
                FROM left_t l ASOF JOIN right_t r
                  ON l.k = r.k AND r.ts < l.ts
                ORDER BY l.k, l.ts""",
                """
                VALUES
                  (1, DATE '1992-01-01', CAST(NULL AS INTEGER)),
                  (1, DATE '1992-01-03', 200),
                  (2, DATE '1992-01-02', 300),
                  (3, DATE '1992-01-01', CAST(NULL AS INTEGER))
                """);
    }

    @org.junit.jupiter.api.Test
    public void testCompositeEquiKeys()
    {
        assertQuery(
                """
                WITH
                  left_t(k1, k2, ts, v) AS (
                    VALUES
                      (1, 10, DATE '1992-01-01', 10), -- no match case
                      (1, 10, DATE '1992-01-03', 20),
                      (2, 20, DATE '1992-01-02', 30),
                      (3, 30, DATE '1992-01-01', 40)  -- no match case
                  ),
                  right_t(k1, k2, ts, x) AS (
                    VALUES
                      (1, 10, DATE '1992-01-01', 100),
                      (1, 10, DATE '1992-01-02', 200),
                      (2, 20, DATE '1992-01-01', 300)
                  )
                SELECT l.k1, l.k2, l.ts, r.x
                FROM left_t l ASOF JOIN right_t r
                  ON l.k1 = r.k1 AND l.k2 = r.k2 AND r.ts < l.ts
                ORDER BY l.k1, l.k2, l.ts""",
                """
                VALUES
                  (1, 10, DATE '1992-01-01', CAST(NULL AS INTEGER)),
                  (1, 10, DATE '1992-01-03', 200),
                  (2, 20, DATE '1992-01-02', 300),
                  (3, 30, DATE '1992-01-01', CAST(NULL AS INTEGER))
                """);
    }
}
