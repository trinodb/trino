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

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public abstract class BaseTestDbResourceGroupsFlywayMigration
{
    protected JdbcDatabaseContainer<?> container;
    protected Jdbi jdbi;

    @BeforeClass
    public final void setup()
    {
        container = startContainer();
        jdbi = Jdbi.create(container.getJdbcUrl(), container.getUsername(), container.getPassword());
    }

    protected abstract JdbcDatabaseContainer<?> startContainer();

    @AfterClass(alwaysRun = true)
    public final void close()
    {
        container.close();
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup()
    {
        dropAllTables();
    }

    @Test
    public void testMigrationWithEmptyDatabase()
    {
        DbResourceGroupConfig config = new DbResourceGroupConfig()
                .setConfigDbUrl(container.getJdbcUrl())
                .setConfigDbUser(container.getUsername())
                .setConfigDbPassword(container.getPassword());
        FlywayMigration.migrate(config);
        verifyResourceGroupsSchema(0);
    }

    @Test
    public void testMigrationWithNonEmptyDatabase()
    {
        String t1Create = "CREATE TABLE t1 (id INT)";
        String t2Create = "CREATE TABLE t2 (id INT)";
        Handle jdbiHandle = jdbi.open();
        jdbiHandle.execute(t1Create);
        jdbiHandle.execute(t2Create);
        DbResourceGroupConfig config = new DbResourceGroupConfig()
                .setConfigDbUrl(container.getJdbcUrl())
                .setConfigDbUser(container.getUsername())
                .setConfigDbPassword(container.getPassword());
        FlywayMigration.migrate(config);
        verifyResourceGroupsSchema(0);
        String t1Drop = "DROP TABLE t1";
        String t2Drop = "DROP TABLE t2";
        jdbiHandle.execute(t1Drop);
        jdbiHandle.execute(t2Drop);
        jdbiHandle.close();
    }

    protected void verifyResourceGroupsSchema(long expectedPropertiesCount)
    {
        verifyResultSetCount("SELECT name FROM resource_groups_global_properties", expectedPropertiesCount);
        verifyResultSetCount("SELECT name FROM resource_groups", 0);
        verifyResultSetCount("SELECT user_regex FROM selectors", 0);
        verifyResultSetCount("SELECT environment FROM exact_match_source_selectors", 0);
    }

    private void verifyResultSetCount(String sql, long expectedCount)
    {
        List<String> results = jdbi.withHandle(handle ->
                handle.createQuery(sql).mapTo(String.class).list());
        assertEquals(results.size(), expectedCount);
    }

    protected void dropAllTables()
    {
        String propertiesTable = "DROP TABLE IF EXISTS resource_groups_global_properties";
        String resourceGroupsTable = "DROP TABLE IF EXISTS resource_groups";
        String selectorsTable = "DROP TABLE IF EXISTS selectors";
        String exactMatchTable = "DROP TABLE IF EXISTS exact_match_source_selectors";
        String flywayHistoryTable = "DROP TABLE IF EXISTS flyway_schema_history";
        Handle jdbiHandle = jdbi.open();
        jdbiHandle.execute(propertiesTable);
        jdbiHandle.execute(selectorsTable);
        jdbiHandle.execute(resourceGroupsTable);
        jdbiHandle.execute(exactMatchTable);
        jdbiHandle.execute(flywayHistoryTable);
        jdbiHandle.close();
    }
}
