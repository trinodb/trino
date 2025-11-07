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
package io.trino.plugin.databend;

import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Locale;
import java.util.Optional;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDatabendConnectorTest
        extends BaseJdbcConnectorTest
{
    private TestingDatabendServer databendServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        databendServer = closeAfterClass(new TestingDatabendServer());
        return DatabendQueryRunner.createDatabendQueryRunner(databendServer, emptyMap(), emptyMap());
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return databendServer::execute;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior behavior)
    {
        return switch (behavior) {
            case SUPPORTS_RENAME_SCHEMA -> false;
            case SUPPORTS_ADD_COLUMN_WITH_POSITION -> false;
            case SUPPORTS_ADD_COLUMN_NOT_NULL_CONSTRAINT -> false;
            case SUPPORTS_COMMENT_ON_COLUMN -> false;
            case SUPPORTS_COMMENT_ON_TABLE -> false;
            case SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT -> false;
            case SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT -> false;
            case SUPPORTS_SET_COLUMN_TYPE -> false;
            case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS -> false;
            case SUPPORTS_ROW_TYPE -> false;
            default -> super.hasBehavior(behavior);
        };
    }

    @Test
    @Override
    public void testShowColumns()
    {
        assertThat(computeActual("SHOW COLUMNS FROM orders"))
                .isEqualTo(getDescribeOrdersResult());
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar", "", "")
                .build();
    }

    @Override
    protected Optional<String> filterColumnNameTestData(String columnName)
    {
        // Databend converts column names to lowercase
        return Optional.of(columnName.toLowerCase(Locale.ROOT));
    }

    @Override
    @Test
    public void testInsert()
    {
        String tableName = "test_insert_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, name VARCHAR)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'test')", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'test')");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    @Test
    public void testCreateTable()
    {
        String tableName = "test_create_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, name VARCHAR, price DOUBLE)");
        assertThat(computeActual("SHOW TABLES LIKE '" + tableName + "'").getRowCount())
                .isEqualTo(1);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    @Test
    public void testTruncateTable()
    {
        String tableName = "test_truncate_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);
        assertUpdate("TRUNCATE TABLE " + tableName);
        assertQuery("SELECT COUNT(*) FROM " + tableName, "SELECT 0");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testBasicDataTypes()
    {
        String tableName = "test_types_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (" +
                "bool_col BOOLEAN, " +
                "tinyint_col TINYINT, " +
                "smallint_col SMALLINT, " +
                "int_col INTEGER, " +
                "bigint_col BIGINT, " +
                "real_col REAL, " +
                "double_col DOUBLE, " +
                "varchar_col VARCHAR, " +
                "date_col DATE)");

        assertUpdate("INSERT INTO " + tableName + " VALUES (" +
                "true, " +
                "127, " +
                "32767, " +
                "2147483647, " +
                "9223372036854775807, " +
                "REAL '3.14', " +
                "2.718, " +
                "'test string', " +
                "DATE '2024-01-01')", 1);

        assertQuery("SELECT * FROM " + tableName,
                "VALUES (true, 127, 32767, 2147483647, 9223372036854775807, REAL '3.14', 2.718, 'test string', DATE '2024-01-01')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDecimalType()
    {
        String tableName = "test_decimal_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (decimal_col DECIMAL(10, 2))");
        assertUpdate("INSERT INTO " + tableName + " VALUES (123.45)", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (123.45)");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    @Test
    public void testAggregationPushdown()
    {
        String tableName = "test_agg_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);

        assertQuery("SELECT COUNT(*) FROM " + tableName, "SELECT 25");
        assertQuery("SELECT COUNT(regionkey) FROM " + tableName, "SELECT 25");
        assertQuery("SELECT MIN(regionkey) FROM " + tableName, "SELECT 0");
        assertQuery("SELECT MAX(regionkey) FROM " + tableName, "SELECT 4");
        assertQuery("SELECT SUM(regionkey) FROM " + tableName, "SELECT 50");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    @Test
    public void testLimitPushdown()
    {
        String tableName = "test_limit_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);

        assertThat(computeActual("SELECT * FROM " + tableName + " LIMIT 10").getRowCount())
                .isEqualTo(10);
        assertThat(computeActual("SELECT * FROM " + tableName + " LIMIT 5").getRowCount())
                .isEqualTo(5);

        assertUpdate("DROP TABLE " + tableName);
    }
}
