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

import org.junit.jupiter.api.Test;

import static io.trino.plugin.lakehouse.TableType.DELTA;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestLakehouseDeltaConnectorSmokeTest
        extends BaseLakehouseConnectorSmokeTest
{
    protected TestLakehouseDeltaConnectorSmokeTest()
    {
        super(DELTA);
    }

    @Test
    @Override
    public void testRenameTable()
    {
        assertThatThrownBy(super::testRenameTable)
                .hasMessage("Renaming managed tables is not allowed with current metastore configuration");
    }

    @Test
    @Override
    public void testRenameTableAcrossSchemas()
    {
        assertThatThrownBy(super::testRenameTableAcrossSchemas)
                .hasMessage("Renaming managed tables is not allowed with current metastore configuration");
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region")).matches(
                """
                \\QCREATE TABLE lakehouse.tpch.region (
                   regionkey bigint,
                   name varchar,
                   comment varchar
                )
                WITH (
                   location = \\E's3://test-bucket-.*/tpch/region.*'\\Q,
                   type = 'DELTA'
                )\\E""");
    }

    @Test
    void testTableChangesFunction()
    {
        String tableName = "test_table_changes_function_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (page_url VARCHAR, domain VARCHAR, views INTEGER) WITH (change_data_feed_enabled = true)");

        assertUpdate("INSERT INTO " + tableName + " VALUES('url1', 'domain1', 1), ('url2', 'domain2', 2), ('url3', 'domain3', 3)", 3);
        assertUpdate("INSERT INTO " + tableName + " VALUES('url4', 'domain4', 4), ('url5', 'domain5', 2), ('url6', 'domain6', 6)", 3);

        assertUpdate("UPDATE " + tableName + " SET page_url = 'url22' WHERE views = 2", 2);
        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + tableName + "'))",
                """
                        VALUES
                            ('url1', 'domain1', 1, 'insert', BIGINT '1'),
                            ('url2', 'domain2', 2, 'insert', BIGINT '1'),
                            ('url3', 'domain3', 3, 'insert', BIGINT '1'),
                            ('url4', 'domain4', 4, 'insert', BIGINT '2'),
                            ('url5', 'domain5', 2, 'insert', BIGINT '2'),
                            ('url6', 'domain6', 6, 'insert', BIGINT '2'),
                            ('url2', 'domain2', 2, 'update_preimage', BIGINT '3'),
                            ('url22', 'domain2', 2, 'update_postimage', BIGINT '3'),
                            ('url5', 'domain5', 2, 'update_preimage', BIGINT '3'),
                            ('url22', 'domain5', 2, 'update_postimage', BIGINT '3')
                        """);

        assertUpdate("DELETE FROM " + tableName + " WHERE views = 2", 2);
        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + tableName + "', null, null, 3))",
                """
                        VALUES
                            ('url22', 'domain2', 2, 'delete', BIGINT '4'),
                            ('url22', 'domain5', 2, 'delete', BIGINT '4')
                        """);

        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + tableName + "')) ORDER BY _commit_version, _change_type, domain",
                """
                        VALUES
                            ('url1', 'domain1', 1, 'insert', BIGINT '1'),
                            ('url2', 'domain2', 2, 'insert', BIGINT '1'),
                            ('url3', 'domain3', 3, 'insert', BIGINT '1'),
                            ('url4', 'domain4', 4, 'insert', BIGINT '2'),
                            ('url5', 'domain5', 2, 'insert', BIGINT '2'),
                            ('url6', 'domain6', 6, 'insert', BIGINT '2'),
                            ('url22', 'domain2', 2, 'update_postimage', BIGINT '3'),
                            ('url22', 'domain5', 2, 'update_postimage', BIGINT '3'),
                            ('url2', 'domain2', 2, 'update_preimage', BIGINT '3'),
                            ('url5', 'domain5', 2, 'update_preimage', BIGINT '3'),
                            ('url22', 'domain2', 2, 'delete', BIGINT '4'),
                            ('url22', 'domain5', 2, 'delete', BIGINT '4')
                        """);
    }

    private void assertTableChangesQuery(String sql, String expectedResult)
    {
        assertThat(query(sql))
                .result()
                .exceptColumns("_commit_timestamp")
                .skippingTypesCheck()
                .matches(expectedResult);
    }

    @Test
    void testTableChangesFunctionFailures()
    {
        String tableName = "test_table_changes_function_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (page_url VARCHAR, domain VARCHAR, views INTEGER) WITH (change_data_feed_enabled = true)");

        assertUpdate("INSERT INTO " + tableName + " VALUES('url1', 'domain1', 1), ('url2', 'domain2', 2), ('url3', 'domain3', 3)", 3);
        assertUpdate("INSERT INTO " + tableName + " VALUES('url4', 'domain4', 4), ('url5', 'domain5', 2), ('url6', 'domain6', 6)", 3);

        assertThat(query("SELECT * FROM TABLE(system.table_changes())"))
                .failure().hasMessage("line 1:21: Missing argument: SCHEMA_NAME");

        assertThat(query("SELECT * FROM TABLE(system.table_changes(NOSCHEMA))"))
                .failure().hasMessage("line 1:42: Column 'noschema' cannot be resolved");

        assertThat(query("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA))"))
                .failure().hasMessage("line 1:42: Missing argument: TABLE_NAME");

        assertThat(query("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + tableName + "'))"))
                .succeeds();

        assertThat(query("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + tableName + "', 'not-a-number', null, null))"))
                .failure().hasMessage("line 1:100: Cannot cast type varchar(12) to bigint");

        assertThat(query("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + tableName + "', null, 'not-a-number', null))"))
                .failure().hasMessage("line 1:106: Cannot cast type varchar(12) to bigint");

        assertThat(query("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + tableName + "', null, null, 'not-a-number'))"))
                .failure().hasMessage("line 1:112: Cannot cast type varchar(12) to bigint");

        assertThat(query("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + tableName + "', null, null, 1))"))
                .succeeds();
    }
}
