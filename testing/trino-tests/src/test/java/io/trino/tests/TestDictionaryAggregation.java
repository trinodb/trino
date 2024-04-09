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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.DICTIONARY_AGGREGATION;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.plugin.tpch.TpchConnectorFactory.TPCH_SPLITS_PER_NODE;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.NONE;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestDictionaryAggregation
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
    {
        QueryRunner queryRunner = new StandaloneQueryRunner(testSessionBuilder()
                .setSystemProperty(DICTIONARY_AGGREGATION, "true")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.toString())
                .build());

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of(TPCH_SPLITS_PER_NODE, "1"));

        return queryRunner;
    }

    @Test
    public void testMixedDistinctWithFilter()
    {
        assertQuery(
                "SELECT " +
                        "     count(DISTINCT x) FILTER (WHERE x > 0), " +
                        "     sum(x) " +
                        "FROM (VALUES 0, 1, 1, 2) t(x)",
                "VALUES (2, 4)");

        assertQuery(
                "SELECT count(DISTINCT x) FILTER (where y = 1)" +
                        "FROM (VALUES (2, 1), (1, 2), (1,1)) t(x, y)",
                "VALUES 2");
    }

    @Test
    public void testAggregationOverJoin()
    {
        // Join produces DictionaryBlocks so needs special treatment in dictionary aggregation
        assertQuery(
                "SELECT to_hex(checksum(DISTINCT l.comment)) FROM tpch.sf1.lineitem l JOIN tpch.\"sf0.1\".orders USING(orderkey) WHERE orderpriority = '1-URGENT'",
                "VALUES '2D0814DA01053A47'");
    }
}
