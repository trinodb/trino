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

import io.trino.sql.planner.plan.LimitNode;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.plugin.kudu.KuduQueryRunnerFactory.createKuduQueryRunnerTpch;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestKuduConnectorTest
        extends BaseConnectorTest
{
    private TestingKuduServer kuduServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        kuduServer = new TestingKuduServer();
        return createKuduQueryRunnerTpch(kuduServer, Optional.empty(), REQUIRED_TPCH_TABLES);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (kuduServer != null) {
            kuduServer.close();
            kuduServer = null;
        }
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_RENAME_SCHEMA:
                return false;

            case SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT:
            case SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
                return false;

            case SUPPORTS_NOT_NULL_CONSTRAINT:
                return false;

            case SUPPORTS_DELETE:
            case SUPPORTS_MERGE:
                return true;

            case SUPPORTS_ARRAY:
            case SUPPORTS_ROW_TYPE:
            case SUPPORTS_NEGATIVE_DATE:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected String createTableForWrites(String createTable)
    {
        // assume primary key column is the first column and there are multiple columns
        Matcher matcher = Pattern.compile("CREATE TABLE .* \\((\\w+) .*").matcher(createTable);
        assertThat(matcher.matches()).as(createTable).isTrue();
        String column = matcher.group(1);

        return createTable.replaceFirst(",", " WITH (primary_key=true),") +
                format("WITH (partition_by_hash_columns = ARRAY['%s'], partition_by_hash_buckets = 2)", column);
    }

    @Test
    @Override
    public void testCreateSchema()
    {
        assertThatThrownBy(super::testCreateSchema)
                .hasMessage("Creating schema in Kudu connector not allowed if schema emulation is disabled.");
    }

    @Override
    public void testCreateSchemaWithLongName()
    {
        // TODO: Add a test to BaseKuduConnectorSmokeTest
        assertThatThrownBy(super::testCreateSchemaWithLongName)
                .hasMessage("Creating schema in Kudu connector not allowed if schema emulation is disabled.");
    }

    @Test
    @Override
    public void testDropNonEmptySchemaWithTable()
    {
        assertThatThrownBy(super::testDropNonEmptySchemaWithTable)
                .hasMessage("Creating schema in Kudu connector not allowed if schema emulation is disabled.");
    }

    @Test
    @Override
    public void testRenameTableAcrossSchema()
    {
        assertThatThrownBy(super::testRenameTableAcrossSchema)
                .hasMessage("Creating schema in Kudu connector not allowed if schema emulation is disabled.");
    }

    @Test
    @Override
    public void testRenameTableToUnqualifiedPreservesSchema()
    {
        assertThatThrownBy(super::testRenameTableToUnqualifiedPreservesSchema)
                .hasMessage("Creating schema in Kudu connector not allowed if schema emulation is disabled.");
    }

    @Override
    public void testAddAndDropColumnName(String columnName)
    {
        // TODO: Enable this test
        assertThatThrownBy(() -> super.testAddAndDropColumnName(columnName))
                .hasMessage("Table partitioning must be specified using setRangePartitionColumns or addHashPartitions");
        throw new SkipException("TODO");
    }

    @Override
    public void testRenameColumnName(String columnName)
    {
        // TODO: Enable this test
        assertThatThrownBy(() -> super.testRenameColumnName(columnName))
                .hasMessage("Table partitioning must be specified using setRangePartitionColumns or addHashPartitions");
        throw new SkipException("TODO");
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        String extra = "nullable, encoding=auto, compression=default";
        MaterializedResult expectedColumns = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", extra, "")
                .row("custkey", "bigint", extra, "")
                .row("orderstatus", "varchar", extra, "")
                .row("totalprice", "double", extra, "")
                .row("orderdate", "varchar", extra, "")
                .row("orderpriority", "varchar", extra, "")
                .row("clerk", "varchar", extra, "")
                .row("shippriority", "integer", extra, "")
                .row("comment", "varchar", extra, "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }

    @Test
    @Override
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "nullable, encoding=auto, compression=default", "")
                .row("custkey", "bigint", "nullable, encoding=auto, compression=default", "")
                .row("orderstatus", "varchar", "nullable, encoding=auto, compression=default", "")
                .row("totalprice", "double", "nullable, encoding=auto, compression=default", "")
                .row("orderdate", "varchar", "nullable, encoding=auto, compression=default", "")
                .row("orderpriority", "varchar", "nullable, encoding=auto, compression=default", "")
                .row("clerk", "varchar", "nullable, encoding=auto, compression=default", "")
                .row("shippriority", "integer", "nullable, encoding=auto, compression=default", "")
                .row("comment", "varchar", "nullable, encoding=auto, compression=default", "")
                .build();

        assertEquals(actual, expectedParametrizedVarchar);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .matches("CREATE TABLE kudu\\.\\w+\\.orders \\Q(\n" +
                        "   orderkey bigint COMMENT '' WITH ( nullable = true ),\n" +
                        "   custkey bigint COMMENT '' WITH ( nullable = true ),\n" +
                        "   orderstatus varchar COMMENT '' WITH ( nullable = true ),\n" +
                        "   totalprice double COMMENT '' WITH ( nullable = true ),\n" +
                        "   orderdate varchar COMMENT '' WITH ( nullable = true ),\n" +
                        "   orderpriority varchar COMMENT '' WITH ( nullable = true ),\n" +
                        "   clerk varchar COMMENT '' WITH ( nullable = true ),\n" +
                        "   shippriority integer COMMENT '' WITH ( nullable = true ),\n" +
                        "   comment varchar COMMENT '' WITH ( nullable = true )\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   number_of_replicas = 3,\n" +
                        "   partition_by_hash_buckets = 2,\n" +
                        "   partition_by_hash_columns = ARRAY['row_uuid'],\n" +
                        "   partition_by_range_columns = ARRAY['row_uuid'],\n" +
                        "   range_partitions = '[{\"lower\":null,\"upper\":null}]'\n" +
                        ")");

        assertUpdate("CREATE TABLE IF NOT EXISTS test_show_create_table (\n" +
                "id INT WITH (primary_key=true),\n" +
                "user_name VARCHAR\n" +
                ") WITH (\n" +
                " partition_by_hash_columns = ARRAY['id'],\n" +
                " partition_by_hash_buckets = 2," +
                " number_of_replicas = 1\n" +
                ")");

        MaterializedResult result = computeActual("SHOW CREATE TABLE test_show_create_table");
        String sqlStatement = (String) result.getOnlyValue();
        String tableProperties = sqlStatement.split("\\)\\s*WITH\\s*\\(")[1];
        assertTableProperty(tableProperties, "number_of_replicas", "1");
        assertTableProperty(tableProperties, "partition_by_hash_columns", Pattern.quote("ARRAY['id']"));
        assertTableProperty(tableProperties, "partition_by_hash_buckets", "2");

        assertUpdate("DROP TABLE test_show_create_table");
    }

    @Test
    public void testRowDelete()
    {
        assertUpdate("CREATE TABLE IF NOT EXISTS test_row_delete (" +
                "id INT WITH (primary_key=true), " +
                "second_id INT, " +
                "user_name VARCHAR" +
                ") WITH (" +
                " partition_by_hash_columns = ARRAY['id'], " +
                " partition_by_hash_buckets = 2" +
                ")");

        assertUpdate("INSERT INTO test_row_delete VALUES (0, 1, 'user0'), (3, 4, 'user2'), (2, 3, 'user2'), (1, 2, 'user1')", 4);
        assertQuery("SELECT count(*) FROM test_row_delete", "VALUES 4");

        assertUpdate("DELETE FROM test_row_delete WHERE second_id = 4", 1);
        assertQuery("SELECT count(*) FROM test_row_delete", "VALUES 3");

        assertUpdate("DELETE FROM test_row_delete WHERE user_name = 'user1'", 1);
        assertQuery("SELECT count(*) FROM test_row_delete", "VALUES 2");

        assertUpdate("DELETE FROM test_row_delete WHERE id = 0", 1);
        assertQuery("SELECT * FROM test_row_delete", "VALUES (2, 3, 'user2')");

        assertUpdate("DROP TABLE test_row_delete");
    }

    @Override
    protected void testColumnName(String columnName, boolean delimited)
    {
        String nameInSql = columnName;
        if (delimited) {
            nameInSql = "\"" + columnName.replace("\"", "\"\"") + "\"";
        }
        String tableName = "tcn_" + nameInSql.toLowerCase(ENGLISH).replaceAll("[^a-z0-9]", "") + randomTableSuffix();

        try {
            // TODO test with both CTAS *and* CREATE TABLE + INSERT, since they use different connector API methods.
            assertUpdate("" +
                    "CREATE TABLE " + tableName + "(key varchar(50) WITH (primary_key=true), " + nameInSql + " varchar(50) WITH (nullable=true)) " +
                    "WITH (partition_by_hash_columns = ARRAY['key'], partition_by_hash_buckets = 3)");
        }
        catch (RuntimeException e) {
            if (isColumnNameRejected(e, columnName, delimited)) {
                // It is OK if give column name is not allowed and is clearly rejected by the connector.
                return;
            }
            throw e;
        }
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES ('null value', NULL), ('sample value', 'abc'), ('other value', 'xyz')", 3);

            // SELECT *
            assertQuery("SELECT * FROM " + tableName, "VALUES ('null value', NULL), ('sample value', 'abc'), ('other value', 'xyz')");

            // projection
            assertQuery("SELECT " + nameInSql + " FROM " + tableName, "VALUES (NULL), ('abc'), ('xyz')");

            // predicate
            assertQuery("SELECT key FROM " + tableName + " WHERE " + nameInSql + " IS NULL", "VALUES ('null value')");
            assertQuery("SELECT key FROM " + tableName + " WHERE " + nameInSql + " = 'abc'", "VALUES ('sample value')");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testProjection()
    {
        assertUpdate("CREATE TABLE IF NOT EXISTS test_projection (" +
                "id INT WITH (primary_key=true), " +
                "user_name VARCHAR " +
                ") WITH (" +
                " partition_by_hash_columns = ARRAY['id'], " +
                " partition_by_hash_buckets = 2" +
                ")");

        assertUpdate("INSERT INTO test_projection VALUES (0, 'user0'), (2, 'user2'), (1, 'user1')", 3);

        assertQuery("SELECT id, 'test' FROM test_projection ORDER BY id", "VALUES (0, 'test'), (1, 'test'), (2, 'test')");

        assertUpdate("DROP TABLE test_projection");
    }

    @Test
    @Override
    public void testExplainAnalyzeWithDeleteWithSubquery()
    {
        String tableName = "test_delete_" + randomTableSuffix();

        // delete using a subquery
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM nation", 25);
        assertExplainAnalyze("EXPLAIN ANALYZE DELETE FROM " + tableName + " WHERE regionkey IN (SELECT regionkey FROM region WHERE name LIKE 'A%' LIMIT 1)",
                "SemiJoin.*");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override
    public void testCreateTable()
    {
        // TODO Remove this overriding test once kudu connector can create tables with default partitions
        String tableName = "test_create_" + randomTableSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (" +
                "id INT WITH (primary_key=true)," +
                "a bigint, b double, c varchar(50))" +
                "WITH (partition_by_hash_columns = ARRAY['id'], partition_by_hash_buckets = 2)");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "id", "a", "b", "c");
        assertNull(getTableComment(getSession().getCatalog().orElseThrow(), getSession().getSchema().orElseThrow(), tableName));

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        assertQueryFails("CREATE TABLE " + tableName + " (" +
                        "id INT WITH (primary_key=true)," +
                        "a bad_type, b double, c varchar(50))" +
                        "WITH (partition_by_hash_columns = ARRAY['id'], partition_by_hash_buckets = 2)",
                ".* Unknown type 'bad_type' for column 'a'");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        tableName = "test_create_if_not_exists_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (" +
                "id INT WITH (primary_key=true)," +
                "a bigint, b varchar(50), c double)" +
                "WITH (partition_by_hash_columns = ARRAY['id'], partition_by_hash_buckets = 2)");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "id", "a", "b", "c");

        assertUpdate(
                "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                    "id INT WITH (primary_key=true)," +
                    "d bigint, e varchar(50))" +
                    "WITH (partition_by_hash_columns = ARRAY['id'], partition_by_hash_buckets = 2)");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "id", "a", "b", "c");

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        // Test CREATE TABLE LIKE
        tableName = "test_create_origin_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (" +
                "id INT WITH (primary_key=true)," +
                "a bigint, b double, c varchar(50))" +
                "WITH (partition_by_hash_columns = ARRAY['id'], partition_by_hash_buckets = 2)");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "id", "a", "b", "c");

        // TODO: remove assertThatThrownBy and uncomment the commented lines
        //  and replace finalTableName with tableName
        //  after (https://github.com/trinodb/trino/issues/12469) is fixed
        String tableNameLike = "test_create_like_" + randomTableSuffix();
        final String finalTableName = tableName;
        assertThatThrownBy(() -> assertUpdate(
                "CREATE TABLE " + tableNameLike + " (LIKE " + finalTableName + ", " +
                    "d bigint, e varchar(50))" +
                    "WITH (partition_by_hash_columns = ARRAY['id'], partition_by_hash_buckets = 2)"))
                .hasMessageContaining("This connector does not support creating tables with column comment");
        //assertTrue(getQueryRunner().tableExists(getSession(), tableNameLike));
        //assertTableColumnNames(tableNameLike, "a", "b", "c", "d", "e");

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        //assertUpdate("DROP TABLE " + tableNameLike);
        //assertFalse(getQueryRunner().tableExists(getSession(), tableNameLike));
    }

    @Override
    public void testCreateTableWithLongTableName()
    {
        // Overridden because DDL in base class can't create Kudu table due to lack of primary key and required table properties
        String baseTableName = "test_create_" + randomTableSuffix();
        String validTableName = baseTableName + "z".repeat(256 - baseTableName.length());

        assertUpdate("CREATE TABLE " + validTableName + "(" +
                "id INT WITH (primary_key=true)," +
                "a VARCHAR)" +
                "WITH (partition_by_hash_columns = ARRAY['id'], partition_by_hash_buckets = 2)");
        assertTrue(getQueryRunner().tableExists(getSession(), validTableName));
        assertUpdate("DROP TABLE " + validTableName);

        String invalidTableName = baseTableName + "z".repeat(256 - baseTableName.length() + 1);
        assertThatThrownBy(() -> query("CREATE TABLE " + invalidTableName + "(" +
                "id INT WITH (primary_key=true)," +
                "a VARCHAR)" +
                "WITH (partition_by_hash_columns = ARRAY['id'], partition_by_hash_buckets = 2)"))
                .hasMessageContaining("invalid table name");
        assertFalse(getQueryRunner().tableExists(getSession(), validTableName));
    }

    @Override
    public void testCreateTableWithLongColumnName()
    {
        // Overridden because DDL in base class can't create Kudu table due to lack of primary key and required table properties
        String tableName = "test_long_column" + randomTableSuffix();
        String baseColumnName = "col";

        int maxLength = maxColumnNameLength().orElseThrow();

        String validColumnName = baseColumnName + "z".repeat(maxLength - baseColumnName.length());
        assertUpdate("CREATE TABLE " + tableName + " (" +
                "id INT WITH (primary_key=true)," +
                validColumnName + " bigint)" +
                "WITH (partition_by_hash_columns = ARRAY['id'], partition_by_hash_buckets = 2)");
        assertTrue(columnExists(tableName, validColumnName));
        assertUpdate("DROP TABLE " + tableName);

        String invalidColumnName = validColumnName + "z";
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE " + tableName + " (" +
                "id INT WITH (primary_key=true)," +
                invalidColumnName + " bigint)" +
                "WITH (partition_by_hash_columns = ARRAY['id'], partition_by_hash_buckets = 2)"))
                .satisfies(this::verifyColumnNameLengthFailurePermissible);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Override
    public void testCreateTableWithColumnComment()
    {
        // TODO https://github.com/trinodb/trino/issues/12469 Support column comment when creating tables
        String tableName = "test_create_" + randomTableSuffix();

        assertQueryFails(
                "CREATE TABLE " + tableName + "(" +
                        "id INT WITH (primary_key=true)," +
                        "a VARCHAR COMMENT 'test comment')" +
                        "WITH (partition_by_hash_columns = ARRAY['id'], partition_by_hash_buckets = 2)",
                "This connector does not support creating tables with column comment");

        assertUpdate("DROP TABLE IF EXISTS " + tableName);
    }

    @Override
    public void testDropTable()
    {
        // TODO Remove this overriding test once kudu connector can create tables with default partitions
        String tableName = "test_drop_table_" + randomTableSuffix();
        assertUpdate(
                "CREATE TABLE " + tableName + "(" +
                    "id INT WITH (primary_key=true)," +
                    "col bigint)" +
                    "WITH (partition_by_hash_columns = ARRAY['id'], partition_by_hash_buckets = 2)");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Override
    protected String tableDefinitionForAddColumn()
    {
        return "(x VARCHAR WITH (primary_key=true)) WITH (partition_by_hash_columns = ARRAY['x'], partition_by_hash_buckets = 2)";
    }

    @Test
    @Override
    public void testAddColumnWithComment()
    {
        String tableName = "test_add_column_with_comment" + randomTableSuffix();

        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                "id INT WITH (primary_key=true), " +
                "a_varchar VARCHAR" +
                ") WITH (" +
                " partition_by_hash_columns = ARRAY['id'], " +
                " partition_by_hash_buckets = 2" +
                ")");

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b_varchar varchar COMMENT 'test new column comment'");
        assertThat(getColumnComment(tableName, "b_varchar")).isEqualTo("test new column comment");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    public void testAddColumnWithCommentSpecialCharacter(String comment)
    {
        // Override because Kudu connector doesn't support creating a new table without partition columns
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_add_col_",
                "(id INT WITH (primary_key=true), a_varchar varchar) WITH (partition_by_hash_columns = ARRAY['id'], partition_by_hash_buckets = 2)")) {
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN b_varchar varchar COMMENT " + varcharLiteral(comment));
            assertEquals(getColumnComment(table.getName(), "b_varchar"), comment);
        }
    }

    @Test
    public void testInsertIntoTableHavingRowUuid()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_", " AS SELECT * FROM region WITH NO DATA")) {
            assertUpdate("INSERT INTO " + table.getName() + " SELECT * FROM region", 5);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("SELECT * FROM region");
        }
    }

    @Test
    @Override
    public void testInsertUnicode()
    {
        // TODO Remove this overriding test once kudu connector can create tables with default partitions
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_unicode_",
                "(test varchar(50) WITH (primary_key=true)) " +
                        "WITH (partition_by_hash_columns = ARRAY['test'], partition_by_hash_buckets = 2)")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'Hello', U&'hello\\6d4B\\8Bd5world\\7F16\\7801' ", 2);
            assertThat(computeActual("SELECT test FROM " + table.getName()).getOnlyColumnAsSet())
                    .containsExactlyInAnyOrder("Hello", "hello测试world编码");
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_unicode_",
                "(test varchar(50) WITH (primary_key=true)) " +
                        "WITH (partition_by_hash_columns = ARRAY['test'], partition_by_hash_buckets = 2)")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'aa', 'bé'", 2);
            assertQuery("SELECT test FROM " + table.getName(), "VALUES 'aa', 'bé'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test = 'aa'", "VALUES 'aa'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test > 'ba'", "VALUES 'bé'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test < 'ba'", "VALUES 'aa'");
            assertQueryReturnsEmptyResult("SELECT test FROM " + table.getName() + " WHERE test = 'ba'");
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_unicode_",
                "(test varchar(50) WITH (primary_key=true)) " +
                        "WITH (partition_by_hash_columns = ARRAY['test'], partition_by_hash_buckets = 2)")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'a', 'é'", 2);
            assertQuery("SELECT test FROM " + table.getName(), "VALUES 'a', 'é'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test = 'a'", "VALUES 'a'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test > 'b'", "VALUES 'é'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test < 'b'", "VALUES 'a'");
            assertQueryReturnsEmptyResult("SELECT test FROM " + table.getName() + " WHERE test = 'b'");
        }
    }

    @Test
    @Override
    public void testInsertHighestUnicodeCharacter()
    {
        // TODO Remove this overriding test once kudu connector can create tables with default partitions
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_unicode_",
                "(test varchar(50) WITH (primary_key=true)) " +
                        "WITH (partition_by_hash_columns = ARRAY['test'], partition_by_hash_buckets = 2)")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'Hello', U&'hello\\6d4B\\8Bd5\\+10FFFFworld\\7F16\\7801' ", 2);
            assertThat(computeActual("SELECT test FROM " + table.getName()).getOnlyColumnAsSet())
                    .containsExactlyInAnyOrder("Hello", "hello测试􏿿world编码");
        }
    }

    @Override
    public void testInsertNegativeDate()
    {
        // TODO Remove this overriding test once kudu connector can create tables with default partitions
        // TODO Update this test once kudu connector supports DATE type: https://github.com/trinodb/trino/issues/11009
        // DATE type is not supported by Kudu connector
        try (TestTable table = new TestTable(getQueryRunner()::execute, "insert_date",
                "(dt DATE WITH (primary_key=true)) " +
                        "WITH (partition_by_hash_columns = ARRAY['dt'], partition_by_hash_buckets = 2)")) {
            assertQueryFails(format("INSERT INTO %s VALUES (DATE '-0001-01-01')", table.getName()), errorMessageForInsertNegativeDate("-0001-01-01"));
        }
    }

    @Override
    protected String errorMessageForInsertNegativeDate(String date)
    {
        return "Insert query has mismatched column types: Table: \\[varchar\\], Query: \\[date\\]";
    }

    @Override
    protected TestTable createTableWithOneIntegerColumn(String namePrefix)
    {
        // TODO Remove this overriding method once kudu connector can create tables with default partitions
        return new TestTable(getQueryRunner()::execute, namePrefix,
                "(col integer WITH (primary_key=true)) " +
                "WITH (partition_by_hash_columns = ARRAY['col'], partition_by_hash_buckets = 2)");
    }

    @Test
    @Override
    public void testWrittenStats()
    {
        // TODO Kudu connector supports CTAS and inserts, but the test would fail
        throw new SkipException("TODO");
    }

    @Override
    public void testReadMetadataWithRelationsConcurrentModifications()
    {
        try {
            super.testReadMetadataWithRelationsConcurrentModifications();
        }
        catch (Exception expected) {
            // The test failure is not guaranteed
            // TODO (https://github.com/trinodb/trino/issues/12974): shouldn't fail
            assertThat(expected)
                    .hasMessageMatching(".* table .* was deleted: Table deleted at .* UTC");
            throw new SkipException("to be fixed");
        }
    }

    @Override
    protected String createTableSqlTemplateForConcurrentModifications()
    {
        // TODO Remove this overriding method once kudu connector can create tables with default partitions
        return "CREATE TABLE %s(a integer WITH (primary_key=true)) " +
                "WITH (partition_by_hash_columns = ARRAY['a'], partition_by_hash_buckets = 2)";
    }

    @Test
    @Override
    public void testCreateTableAsSelectNegativeDate()
    {
        // Map date column type to varchar
        String tableName = "negative_date_" + randomTableSuffix();

        try {
            assertUpdate(format("CREATE TABLE %s AS SELECT DATE '-0001-01-01' AS dt", tableName), 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES '-0001-01-01'");
            assertQuery(format("SELECT * FROM %s WHERE dt = '-0001-01-01'", tableName), "VALUES '-0001-01-01'");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    @Override
    public void testDateYearOfEraPredicate()
    {
        assertThatThrownBy(super::testDateYearOfEraPredicate)
                .hasStackTraceContaining("Cannot apply operator: varchar = date");
    }

    @Override
    public void testVarcharCastToDateInPredicate()
    {
        assertThatThrownBy(super::testVarcharCastToDateInPredicate)
                .hasStackTraceContaining("Table partitioning must be specified using setRangePartitionColumns or addHashPartitions");

        throw new SkipException("TODO: implement the test for Kudu");
    }

    @Test
    @Override
    public void testCharVarcharComparison()
    {
        // TODO https://github.com/trinodb/trino/issues/3597 Fix Kudu CREATE TABLE AS SELECT with char(n) type does not preserve trailing spaces
        assertThatThrownBy(super::testCharVarcharComparison)
                .hasMessageContaining("For query: ")
                .hasMessageContaining("Actual rows")
                .hasMessageContaining("Expected rows");

        throw new SkipException("TODO");
    }

    @Test
    public void testLimitPushdown()
    {
        assertThat(query("SELECT name FROM nation LIMIT 30")).isNotFullyPushedDown(LimitNode.class); // Use high limit for result determinism
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("time")
                || typeName.equals("time(6)")
                || typeName.equals("timestamp(6)")
                || typeName.equals("timestamp(3) with time zone")
                || typeName.equals("timestamp(6) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        if (typeName.equals("date") // date gets stored as varchar
                || typeName.equals("varbinary") // TODO (https://github.com/trinodb/trino/issues/3416)
                || (typeName.startsWith("char") && dataMappingTestSetup.getSampleValueLiteral().contains(" "))) { // TODO: https://github.com/trinodb/trino/issues/3597
            // TODO this should either work or fail cleanly
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("Kudu connector does not support column default values");
    }

    @Override
    protected String tableDefinitionForQueryLoggingCount()
    {
        return "( " +
                " foo_1 int WITH (primary_key=true), " +
                " foo_2_4 int " +
                ") " +
                "WITH ( " +
                " partition_by_hash_columns = ARRAY['foo_1'], " +
                " partition_by_hash_buckets = 2 " +
                ")";
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(256);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("invalid table name: identifier");
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(256);
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("invalid column name: identifier");
    }

    private void assertTableProperty(String tableProperties, String key, String regexValue)
    {
        assertTrue(Pattern.compile(key + "\\s*=\\s*" + regexValue + ",?\\s+").matcher(tableProperties).find(),
                "Not found: " + key + " = " + regexValue + " in " + tableProperties);
    }
}
