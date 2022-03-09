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
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.OracleContainer;
import org.testng.annotations.Test;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

@Test(singleThreaded = true)
public class TestDbResourceGroupsOracleFlywayMigration
        extends BaseTestDbResourceGroupsFlywayMigration
{
    @Override
    protected final JdbcDatabaseContainer<?> startContainer()
    {
        JdbcDatabaseContainer<?> container = new OracleContainer("gvenzl/oracle-xe:18.4.0-slim")
                .withPassword("trino")
                .withEnv("ORACLE_PASSWORD", "trino");
        container.start();
        return container;
    }

    @Override
    protected void dropAllTables()
    {
        String propertiesTable = "resource_groups_global_properties".toUpperCase(Locale.ENGLISH);
        String resourceGroupsTable = "resource_groups".toUpperCase(Locale.ENGLISH);
        String selectorsTable = "selectors".toUpperCase(Locale.ENGLISH);
        String exactMatchTable = "exact_match_source_selectors".toUpperCase(Locale.ENGLISH);
        String flywayHistoryTable = "flyway_schema_history";
        Handle jdbiHandle = jdbi.open();
        if (tableExists(jdbiHandle, propertiesTable)) {
            jdbiHandle.execute("DROP TABLE " + propertiesTable);
        }
        if (tableExists(jdbiHandle, selectorsTable)) {
            jdbiHandle.execute("DROP TABLE " + selectorsTable);
        }
        if (tableExists(jdbiHandle, resourceGroupsTable)) {
            jdbiHandle.execute("DROP TABLE " + resourceGroupsTable);
        }
        if (tableExists(jdbiHandle, exactMatchTable)) {
            jdbiHandle.execute("DROP TABLE " + exactMatchTable);
        }
        if (tableExists(jdbiHandle, flywayHistoryTable)) {
            jdbiHandle.execute("DROP TABLE \"" + flywayHistoryTable + "\"");
        }
        jdbiHandle.close();
    }

    private boolean tableExists(Handle jdbiHandle, String tableName)
    {
        try {
            DatabaseMetaData metaData = jdbiHandle.getConnection().getMetaData();
            try (ResultSet resultSet = metaData.getTables(null, null, tableName, null)) {
                while (resultSet.next()) {
                    String table = resultSet.getString("TABLE_NAME");
                    if (tableName.equalsIgnoreCase(table)) {
                        return true;
                    }
                }
            }
            return false;
        }
        catch (SQLException e) {
            return false;
        }
    }

    @Test
    public void forceTestNgToRespectSingleThreaded()
    {
        // TODO: Remove after updating TestNG to 7.4.0+ (https://github.com/trinodb/trino/issues/8571)
        // TestNG doesn't enforce @Test(singleThreaded = true) when tests are defined in base class. According to
        // https://github.com/cbeust/testng/issues/2361#issuecomment-688393166 a workaround it to add a dummy test to the leaf test class.
    }
}
