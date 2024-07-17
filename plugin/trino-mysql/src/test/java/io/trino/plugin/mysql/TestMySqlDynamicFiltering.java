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
package io.trino.plugin.mysql;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;

@Isolated
final class TestMySqlDynamicFiltering
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = MySqlQueryRunner.builder(closeAfterClass(new TestingMySqlServer("mysql:5.7", false)))
                .setInitialTables(ImmutableList.of(NATION))
                .build();

        queryRunner.installPlugin(new JmxPlugin());
        queryRunner.createCatalog("jmx", "jmx");

        return queryRunner;
    }

    @Test
    void testDynamicFilteringStats()
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("mysql", "dynamic_filtering_wait_timeout", "1h")
                .setSystemProperty("join_reordering_strategy", "NONE")
                .setSystemProperty("join_distribution_type", "BROADCAST")
                .build();

        assertThat(query(session, "SELECT COUNT(*) FROM nation a JOIN tpch.tiny.nation b ON a.nationkey = b.nationkey AND b.name = 'INDIA'"))
                .matches("VALUES BIGINT '1'");

        String coordinatorId = (String) computeScalar("SELECT node_id FROM system.runtime.nodes WHERE coordinator = true");

        // Using assertEventually to wait for the dynamic filtering stats to be updated
        assertEventually(() -> assertThat((long) computeScalar("SELECT \"completeddynamicfilters.totalcount\" FROM jmx.current.\"io.trino.plugin.jdbc:name=mysql,type=dynamicfilteringstats\"" +
                "WHERE node = '" + coordinatorId + "'"))
                .isEqualTo(1L));
        assertEventually(() -> assertThat((long) computeScalar("SELECT \"totaldynamicfilters.totalcount\" FROM jmx.current.\"io.trino.plugin.jdbc:name=mysql,type=dynamicfilteringstats\"" +
                "WHERE node = '" + coordinatorId + "'"))
                .isEqualTo(1L));
    }
}
