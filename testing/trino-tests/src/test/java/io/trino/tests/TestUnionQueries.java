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
import io.trino.Session;
import io.trino.plugin.tpcds.TpcdsPlugin;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.NONE;

public class TestUnionQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder().build();
        queryRunner.installPlugin(new TpcdsPlugin());
        queryRunner.createCatalog("tpcds", "tpcds", ImmutableMap.of());
        return queryRunner;
    }

    @Test
    public void testUnionFromDifferentCatalogs()
    {
        @Language("SQL")
        String query = "SELECT count(*) FROM (SELECT nationkey FROM tpch.tiny.nation UNION ALL SELECT ss_sold_date_sk FROM tpcds.tiny.store_sales) n JOIN tpch.tiny.region r ON n.nationkey = r.regionkey";
        assertQuery(query, "VALUES(5)");
    }

    @Test
    public void testUnionAllOnConnectorPartitionedTables()
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, OptimizerConfig.JoinDistributionType.BROADCAST.name()).build();

        @Language("SQL")
        String query = "SELECT count(*) FROM ((SELECT orderkey FROM orders) union all (SELECT nationkey FROM nation)) o JOIN nation n ON o.orderkey = n.nationkey";
        assertQuery(session, query, "VALUES(32)");
    }
}
