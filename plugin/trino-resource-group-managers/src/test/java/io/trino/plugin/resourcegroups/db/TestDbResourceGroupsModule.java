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
package io.trino.plugin.resourcegroups.db;

import io.trino.spi.memory.ClusterMemoryPoolManager;
import io.trino.spi.resourcegroups.ResourceGroupConfigurationManager;
import io.trino.spi.resourcegroups.ResourceGroupConfigurationManagerContext;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDbResourceGroupsModule
{
    @Test
    public void testCreateMigratorSkipsWhenDisabled()
    {
        DbResourceGroupConfig config = new DbResourceGroupConfig().setRunMigrationsEnabled(false);

        DatabaseMigrator migrator = DbResourceGroupsModule.createMigrator(config);

        assertThat(migrator).isInstanceOf(NoOpDatabaseMigrator.class);
    }

    @Test
    public void testCreateManagerWithProductionWiring()
    {
        TestingResourceGroupsDatabase database = TestingResourceGroupsDatabase.create();
        TestingResourceGroupsDao dao = database.getDao();
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        dao.insertResourceGroupsGlobalProperties("physical_data_scan_quota_period", "1h");
        dao.insertResourceGroup(1, "global", "100%", 100, 100, 100, "weighted", null, true, "1h", "1d", "1GB", null, "test");
        dao.insertSelector(1, 1, ".*", null, null, null, null, null, null, null, null);

        ResourceGroupConfigurationManager<?> manager = new DbResourceGroupConfigurationManagerFactory().create(
                database.getConfigurationProperties(),
                new TestingConfigurationManagerContext("test"));
        try {
            assertThat(manager).isInstanceOf(DbResourceGroupConfigurationManager.class);
            assertThat(((DbResourceGroupConfigurationManager) manager).getRootGroups()).hasSize(1);
            assertThat(((DbResourceGroupConfigurationManager) manager).getSelectors()).hasSize(1);
        }
        finally {
            manager.shutdown();
        }
    }

    private record TestingConfigurationManagerContext(String environment)
            implements ResourceGroupConfigurationManagerContext
    {
        @Override
        public ClusterMemoryPoolManager getMemoryPoolManager()
        {
            return _ -> {};
        }

        @Override
        public String getEnvironment()
        {
            return environment;
        }
    }
}
