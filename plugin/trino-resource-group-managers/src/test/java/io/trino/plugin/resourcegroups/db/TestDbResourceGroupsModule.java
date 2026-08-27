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

import com.google.inject.Provider;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDbResourceGroupsModule
{
    @Test
    public void testCreateMigratorSkipsDataSourceWhenDisabled()
    {
        DbResourceGroupConfig config = new DbResourceGroupConfig().setRunMigrationsEnabled(false);
        Provider<DataSource> dataSource = () -> {
            throw new AssertionError("DataSource should not be created when migrations are disabled");
        };

        DatabaseMigrator migrator = DbResourceGroupsModule.createMigrator(dataSource, config);

        assertThat(migrator).isInstanceOf(NoOpDatabaseMigrator.class);
    }
}
