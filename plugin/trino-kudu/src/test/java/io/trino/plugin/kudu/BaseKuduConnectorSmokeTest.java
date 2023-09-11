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
package io.trino.plugin.kudu;

import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.plugin.kudu.KuduQueryRunnerFactory.createKuduQueryRunnerTpch;
import static io.trino.plugin.kudu.TestKuduConnectorTest.REGION_COLUMNS;
import static io.trino.plugin.kudu.TestKuduConnectorTest.createKuduTableForWrites;
import static io.trino.plugin.kudu.TestingKuduServer.EARLIEST_TAG;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseKuduConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    protected abstract String getKuduServerVersion();

    protected abstract Optional<String> getKuduSchemaEmulationPrefix();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createKuduQueryRunnerTpch(
                closeAfterClass(new TestingKuduServer(getKuduServerVersion())),
                getKuduSchemaEmulationPrefix(), REQUIRED_TPCH_TABLES);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ARRAY,
                    SUPPORTS_COMMENT_ON_COLUMN,
                    SUPPORTS_COMMENT_ON_TABLE,
                    SUPPORTS_CREATE_MATERIALIZED_VIEW,
                    SUPPORTS_CREATE_VIEW,
                    SUPPORTS_NEGATIVE_DATE,
                    SUPPORTS_NOT_NULL_CONSTRAINT,
                    SUPPORTS_RENAME_SCHEMA,
                    SUPPORTS_ROW_TYPE,
                    SUPPORTS_TOPN_PUSHDOWN,
                    SUPPORTS_TRUNCATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected String getCreateTableDefaultDefinition()
    {
        return "(a bigint WITH (primary_key=true), b double) " +
                "WITH (partition_by_hash_columns = ARRAY['a'], partition_by_hash_buckets = 2)";
    }

    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo("CREATE TABLE kudu." + getSession().getSchema().orElseThrow() + ".region (\n" +
                        "   regionkey bigint COMMENT '' WITH ( nullable = true ),\n" +
                        "   name varchar COMMENT '' WITH ( nullable = true ),\n" +
                        "   comment varchar COMMENT '' WITH ( nullable = true )\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   number_of_replicas = 1,\n" +
                        "   partition_by_hash_buckets = 2,\n" +
                        "   partition_by_hash_columns = ARRAY['row_uuid'],\n" +
                        "   partition_by_range_columns = ARRAY['row_uuid'],\n" +
                        "   range_partitions = '[{\"lower\":null,\"upper\":null}]'\n" +
                        ")");
    }

    @Test
    @Override
    public void testDeleteAllDataFromTable()
    {
        String tableName = "test_delete_all_data_" + randomNameSuffix();
        assertUpdate(createKuduTableForWrites("CREATE TABLE %s %s".formatted(tableName, REGION_COLUMNS)));
        assertUpdate("INSERT INTO %s SELECT * FROM region".formatted(tableName), 5);

        assertUpdate("DELETE FROM " + tableName, 5);
        assertQuery("SELECT count(*) FROM " + tableName, "VALUES 0");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override
    public void testRowLevelDelete()
    {
        String tableName = "test_row_delete_" + randomNameSuffix();
        assertUpdate(createKuduTableForWrites("CREATE TABLE %s %s".formatted(tableName, REGION_COLUMNS)));
        assertUpdate("INSERT INTO %s SELECT * FROM region".formatted(tableName), 5);

        assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 2", 1);
        assertThat(query("SELECT * FROM " + tableName + " WHERE regionkey = 2"))
                .returnsEmptyResult();
        assertThat(query("SELECT cast(regionkey AS integer) FROM " + tableName))
                .skippingTypesCheck()
                .matches("VALUES 0, 1, 3, 4");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override
    public void testUpdate()
    {
        String tableName = "test_update_" + randomNameSuffix();
        assertUpdate("CREATE TABLE %s %s".formatted(tableName, getCreateTableDefaultDefinition()));
        assertUpdate("INSERT INTO " + tableName + " (a, b) SELECT regionkey, regionkey * 2.5 FROM region", "SELECT count(*) FROM region");
        assertThat(query("SELECT a, b FROM " + tableName))
                .matches(expectedValues("(0, 0.0), (1, 2.5), (2, 5.0), (3, 7.5), (4, 10.0)"));

        assertUpdate("UPDATE " + tableName + " SET b = b + 1.2 WHERE a % 2 = 0", 3);
        assertThat(query("SELECT a, b FROM " + tableName))
                .matches(expectedValues("(0, 1.2), (1, 2.5), (2, 6.2), (3, 7.5), (4, 11.2)"));
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTableWithTableComment()
    {
        String tableName = "test_create_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a bigint WITH (primary_key=true)) COMMENT 'test comment' WITH (partition_by_hash_columns = ARRAY['a'], partition_by_hash_buckets = 2)");

        // Kudu versions < 1.15.0 ignore a table comment
        String expected = getKuduServerVersion().equals(EARLIEST_TAG) ? null : "test comment";
        assertThat(getTableComment(tableName)).isEqualTo(expected);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropSchemaCascade()
    {
        String schemaName = "test_drop_schema_cascade_" + randomNameSuffix();
        String tableName = "test_table" + randomNameSuffix();
        try {
            if (getKuduSchemaEmulationPrefix().isEmpty()) {
                assertThatThrownBy(() -> assertUpdate("CREATE SCHEMA " + schemaName))
                        .hasMessageContaining("Creating schema in Kudu connector not allowed if schema emulation is disabled.");
                throw new SkipException("Cannot test when schema emulation is disabled");
            }
            assertUpdate("CREATE SCHEMA " + schemaName);
            assertUpdate("CREATE TABLE " + schemaName + "." + tableName + " AS SELECT 1 a", 1);

            assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(schemaName);

            assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");
            assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain(schemaName);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + schemaName + "." + tableName);
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        }
    }
}
