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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.resourcegroups.ResourceGroupManagerPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestQueryAlreadyBegunError
        extends AbstractTestQueryFramework
{
    private static final Session REPRODUCE_ERROR_SESSION = testSessionBuilder()
            .setSystemProperty("reproduce_query_already_begun_bug", "true")
            .build();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = HiveQueryRunner.builder()
                    .setInitialTables(ImmutableList.of(NATION))
                    .build();

        queryRunner.installPlugin(new ResourceGroupManagerPlugin());
        queryRunner.getCoordinator().getResourceGroupManager().get()
                .setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_zero_concurrent_queries.json")));

        return queryRunner;
    }

    @Test
    public void testQueryAlreadyBegun()
    {
        assertThatThrownBy(() -> query(REPRODUCE_ERROR_SESSION, "SELECT count(*) FROM hive.tpch.nation WHERE n_regionkey is not null"))
                .hasMessageContaining("Query already begun");
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }
}
