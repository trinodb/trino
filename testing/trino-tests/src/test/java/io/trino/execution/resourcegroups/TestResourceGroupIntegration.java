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
package io.trino.execution.resourcegroups;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.resourcegroups.ResourceGroupManagerPlugin;
import io.trino.server.ResourceGroupInfo;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunner;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.testing.Assertions.assertLessThan;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestResourceGroupIntegration
{
    @Test
    public void testMemoryFraction()
            throws Exception
    {
        try (QueryRunner queryRunner = TpchQueryRunner.builder().build()) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            getResourceGroupManager(queryRunner).setConfigurationManager("file", ImmutableMap.of(
                    "resource-groups.config-file", getResourceFilePath("resource_groups_memory_percentage.json")));

            queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk");
            waitForGlobalResourceGroup(queryRunner);
        }
    }

    @Test
    public void testPathToRoot()
            throws Exception
    {
        try (QueryRunner queryRunner = TpchQueryRunner.builder().build()) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            InternalResourceGroupManager<?> manager = getResourceGroupManager(queryRunner);
            manager.setConfigurationManager("file", ImmutableMap.of(
                    "resource-groups.config-file", getResourceFilePath("resource_groups_config_dashboard.json")));

            queryRunner.execute(testSessionBuilder().setCatalog("tpch").setSchema("tiny").setSource("dashboard-foo").build(), "SELECT COUNT(*), clerk FROM orders GROUP BY clerk");
            List<ResourceGroupInfo> path = manager.tryGetPathToRoot(new ResourceGroupId(new ResourceGroupId(new ResourceGroupId("global"), "user-user"), "dashboard-user"))
                    .orElseThrow(() -> new IllegalStateException("Resource group not found"));
            assertThat(path.size()).isEqualTo(3);
            assertThat(path.get(1).getSubGroups()).isPresent();
            assertThat(path.get(2).getId()).isEqualTo(new ResourceGroupId("global"));
            assertThat(path.get(2).getHardConcurrencyLimit()).isEqualTo(100);
            assertThat(path.get(2).getRunningQueries()).isNotPresent();
        }
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }

    public static void waitForGlobalResourceGroup(QueryRunner queryRunner)
            throws InterruptedException
    {
        long startTime = System.nanoTime();
        while (true) {
            SECONDS.sleep(1);
            ResourceGroupInfo global = getResourceGroupManager(queryRunner).tryGetResourceGroupInfo(new ResourceGroupId("global"))
                    .orElseThrow(() -> new IllegalStateException("Resource group not found"));
            if (global.getSoftMemoryLimit().toBytes() > 0) {
                break;
            }
            assertLessThan(nanosSince(startTime).roundTo(SECONDS), 60L);
        }
    }

    private static InternalResourceGroupManager<?> getResourceGroupManager(QueryRunner queryRunner)
    {
        return queryRunner.getCoordinator().getResourceGroupManager()
                .orElseThrow(() -> new IllegalArgumentException("no resource group manager"));
    }
}
