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
package io.trino.plugin.lakehouse;

import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.lakehouse.TableType.HIVE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLakehouseHiveConnectorSmokeTest
        extends BaseLakehouseConnectorSmokeTest
{
    protected TestLakehouseHiveConnectorSmokeTest()
    {
        super(HIVE);
    }

    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_TRUNCATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected TestTable newTrinoTable(String namePrefix, String tableDefinition)
    {
        if (tableDefinition.startsWith("(")) {
            tableDefinition += " WITH (transactional = true)";
        }
        else {
            tableDefinition = "WITH (transactional = true) " + tableDefinition;
        }
        return super.newTrinoTable(namePrefix, tableDefinition);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region")).isEqualTo(
                """
                CREATE TABLE lakehouse.tpch.region (
                   regionkey bigint,
                   name varchar(25),
                   comment varchar(152)
                )
                WITH (
                   format = 'ORC',
                   type = 'HIVE'
                )""");
    }

    @Test
    void testTableChangesFunctionFailures()
    {
        String tableName = "test_table_changes_function_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (page_url VARCHAR, domain VARCHAR, views INTEGER) WITH (change_data_feed_enabled = true)");

        assertUpdate("INSERT INTO " + tableName + " VALUES('url1', 'domain1', 1), ('url2', 'domain2', 2), ('url3', 'domain3', 3)", 3);
        assertUpdate("INSERT INTO " + tableName + " VALUES('url4', 'domain4', 4), ('url5', 'domain5', 2), ('url6', 'domain6', 6)", 3);

        assertThat(query("SELECT * FROM TABLE(system.table_changes())"))
                .failure().hasMessageMatching("line 1:21: Missing argument: SCHEMA_NAME");

        assertThat(query("SELECT * FROM TABLE(system.table_changes(NOSCHEMA))"))
                .failure().hasMessageMatching("line 1:42: Column 'noschema' cannot be resolved");

        assertThat(query("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA))"))
                .failure().hasMessageMatching("line 1:42: Missing argument: TABLE_NAME");

        assertThat(query("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + tableName + "'))"))
                .failure().hasMessageMatching("table_changes arguments may not be null");

        assertThat(query("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + tableName + "', 'not-a-number', null, null))"))
                .failure().hasMessage("line 1:100: Cannot cast type varchar(12) to bigint");

        assertThat(query("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + tableName + "', null, 'not-a-number', null))"))
                .failure().hasMessage("line 1:106: Cannot cast type varchar(12) to bigint");

        assertThat(query("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + tableName + "', null, null, 'not-a-number'))"))
                .failure().hasMessage("line 1:112: Cannot cast type varchar(12) to bigint");

        assertThat(query("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + tableName + "', 100))"))
                .failure().hasMessageMatching("table_changes arguments may not be null");

        assertThat(query("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + tableName + "', 100, 200))"))
                .failure().hasMessageMatching("table_changes function is not supported for the given table type");
    }
}
