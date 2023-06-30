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

import io.airlift.log.Logger;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.plugin.kudu.KuduQueryRunnerFactory.createKuduQueryRunnerTpch;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestKuduConnectorTest
        extends BaseConnectorTest
{
    private static final Logger log = Logger.get(TestKuduConnectorTest.class);
    protected static final String NATION_COLUMNS = "(nationkey bigint, name varchar(25), regionkey bigint, comment varchar(152))";
    protected static final String ORDER_COLUMNS = "(orderkey bigint, custkey bigint, orderstatus varchar(1), totalprice double, orderdate date, orderpriority varchar(15), clerk varchar(15), shippriority integer, comment varchar(79))";
    public static final String REGION_COLUMNS = "(regionkey bigint, name varchar(25), comment varchar(152))";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createKuduQueryRunnerTpch(
                closeAfterClass(new TestingKuduServer()),
                Optional.empty(),
                REQUIRED_TPCH_TABLES);
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_TRUNCATE:
                return false;

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

            case SUPPORTS_SET_COLUMN_TYPE:
                return false;

            case SUPPORTS_CREATE_VIEW:
            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
                return false;

            case SUPPORTS_NOT_NULL_CONSTRAINT:
                return false;

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
        return createKuduTableForWrites(createTable);
    }

    public static String createKuduTableForWrites(String createTable)
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
    public void testCreateSchemaWithNonLowercaseOwnerName()
    {
        assertThatThrownBy(super::testCreateSchemaWithNonLowercaseOwnerName)
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

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        String extra = "nullable, encoding=auto, compression=default";
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
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
    }

    @Test
    @Override
    public void testShowColumns()
    {
        assertThat(query("SHOW COLUMNS FROM orders")).matches(getDescribeOrdersResult());
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat(computeScalar("SHOW CREATE TABLE orders"))
                .isEqualTo("CREATE TABLE kudu.default.orders (\n" +
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

        String sqlStatement = (String) computeScalar("SHOW CREATE TABLE test_show_create_table");
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
        String tableName = "tcn_" + nameInSql.toLowerCase(ENGLISH).replaceAll("[^a-z0-9]", "") + randomNameSuffix();

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

    @Override
    public void testAddNotNullColumnToEmptyTable()
    {
        // TODO: Enable this test
        assertThatThrownBy(super::testAddNotNullColumnToEmptyTable)
                .hasMessage("Table partitioning must be specified using setRangePartitionColumns or addHashPartitions");
        throw new SkipException("TODO");
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
        String tableName = "test_delete_" + randomNameSuffix();

        // delete using a subquery
        assertUpdate(createTableForWrites("CREATE TABLE %s %s".formatted(tableName, NATION_COLUMNS)));
        assertUpdate("INSERT INTO %s SELECT * FROM nation".formatted(tableName), 25);
        assertExplainAnalyze("EXPLAIN ANALYZE DELETE FROM " + tableName + " WHERE regionkey IN (SELECT regionkey FROM region WHERE name LIKE 'A%' LIMIT 1)",
                "SemiJoin.*");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override
    public void testCreateTable()
    {
        // TODO Remove this overriding test once kudu connector can create tables with default partitions
        String tableName = "test_create_" + randomNameSuffix();

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

        tableName = "test_create_if_not_exists_" + randomNameSuffix();
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
        tableName = "test_create_origin_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (" +
                "id INT WITH (primary_key=true)," +
                "a bigint, b double, c varchar(50))" +
                "WITH (partition_by_hash_columns = ARRAY['id'], partition_by_hash_buckets = 2)");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "id", "a", "b", "c");

        // TODO: remove assertThatThrownBy and uncomment the commented lines
        //  and replace finalTableName with tableName
        //  after (https://github.com/trinodb/trino/issues/12469) is fixed
        String tableNameLike = "test_create_like_" + randomNameSuffix();
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
        String baseTableName = "test_create_" + randomNameSuffix();
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
        String tableName = "test_long_column" + randomNameSuffix();
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
        String tableName = "test_create_" + randomNameSuffix();

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
        String tableName = "test_drop_table_" + randomNameSuffix();
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
        String tableName = "test_add_column_with_comment" + randomNameSuffix();

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

    @Test
    @Override
    public void testDelete()
    {
        // delete successive parts of the table
        withTableName("test_delete", tableName -> {
            assertUpdate(createTableForWrites("CREATE TABLE %s %s".formatted(tableName, ORDER_COLUMNS)));
            assertUpdate("INSERT INTO " + tableName + " SELECT * FROM orders", 15000);
            assertUpdate("DELETE FROM " + tableName + " WHERE custkey <= 100", "SELECT count(*) FROM orders WHERE custkey <= 100");
            assertQuery("SELECT * FROM " + tableName, "SELECT * FROM orders WHERE custkey > 100");

            assertUpdate("DELETE FROM " + tableName + " WHERE custkey <= 300", "SELECT count(*) FROM orders WHERE custkey > 100 AND custkey <= 300");
            assertQuery("SELECT * FROM " + tableName, "SELECT * FROM orders WHERE custkey > 300");

            assertUpdate("DELETE FROM " + tableName + " WHERE custkey <= 500", "SELECT count(*) FROM orders WHERE custkey > 300 AND custkey <= 500");
            assertQuery("SELECT * FROM " + tableName, "SELECT * FROM orders WHERE custkey > 500");
        });

        withTableName("test_delete", tableName -> {
            assertUpdate(createTableForWrites("CREATE TABLE %s %s".formatted(tableName, ORDER_COLUMNS)));
            assertUpdate("INSERT INTO " + tableName + " SELECT * FROM orders", 15000);
            assertUpdate("DELETE FROM " + tableName + " WHERE custkey <= 100", "SELECT count(*) FROM orders WHERE custkey <= 100");
            assertQuery("SELECT * FROM " + tableName, "SELECT * FROM orders WHERE custkey > 100");

            assertUpdate("DELETE FROM " + tableName + " WHERE custkey <= 300", "SELECT count(*) FROM orders WHERE custkey > 100 AND custkey <= 300");
            assertQuery("SELECT * FROM " + tableName, "SELECT * FROM orders WHERE custkey > 300");

            assertUpdate("DELETE FROM " + tableName + " WHERE custkey <= 500", "SELECT count(*) FROM orders WHERE custkey > 300 AND custkey <= 500");
            assertQuery("SELECT * FROM " + tableName, "SELECT * FROM orders WHERE custkey > 500");
        });

        // delete without matching any rows
        withTableName("test_delete", tableName -> {
            assertUpdate(createTableForWrites("CREATE TABLE %s %s".formatted(tableName, ORDER_COLUMNS)));
            assertUpdate("INSERT INTO " + tableName + " SELECT * FROM orders", 15000);
            assertUpdate("DELETE FROM " + tableName + " WHERE orderkey < 0", 0);
        });

        // delete with a predicate that optimizes to false
        withTableName("test_delete", tableName -> {
            assertUpdate(createTableForWrites("CREATE TABLE %s %s".formatted(tableName, ORDER_COLUMNS)));
            assertUpdate("INSERT INTO " + tableName + " SELECT * FROM orders", 15000);
            assertUpdate("DELETE FROM " + tableName + " WHERE orderkey > 5 AND orderkey < 4", 0);
        });
    }

    @Test
    @Override
    public void testDeleteWithLike()
    {
        withTableName("test_with_like", tableName -> {
            assertUpdate(createTableForWrites("CREATE TABLE %s %s".formatted(tableName, NATION_COLUMNS)));
            assertUpdate("INSERT INTO %s SELECT * FROM nation".formatted(tableName), 25);
            assertUpdate("DELETE FROM " + tableName + " WHERE name LIKE '%a%'", "VALUES 0");
            assertUpdate("DELETE FROM " + tableName + " WHERE name LIKE '%A%'", "SELECT count(*) FROM nation WHERE name LIKE '%A%'");
        });
    }

    @Override
    protected TestTable createTableWithOneIntegerColumn(String namePrefix)
    {
        // TODO Remove this overriding method once kudu connector can create tables with default partitions
        return new TestTable(getQueryRunner()::execute, namePrefix,
                "(col integer WITH (primary_key=true)) " +
                "WITH (partition_by_hash_columns = ARRAY['col'], partition_by_hash_buckets = 2)");
    }

    /**
     * This test fails intermittently because Kudu doesn't have strong enough
     * semantics to support writing from multiple threads.
     */
    @Test(enabled = false)
    @Override
    public void testUpdateWithPredicates()
    {
        withTableName("test_update_with_predicates", tableName -> {
            assertUpdate(createTableForWrites("CREATE TABLE %s (a INT, b INT, c INT)".formatted(tableName)));
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 2, 3), (11, 12, 13), (21, 22, 23)", 3);
            assertUpdate("UPDATE " + tableName + " SET a = a - 1 WHERE c = 3", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (0, 2, 3), (11, 12, 13), (21, 22, 23)");

            assertUpdate("UPDATE " + tableName + " SET c = c + 1 WHERE a = 11", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (0, 2, 3), (11, 12, 14), (21, 22, 23)");

            assertUpdate("UPDATE " + tableName + " SET b = b * 2 WHERE b = 22", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (0, 2, 3), (11, 12, 14), (21, 44, 23)");
        });
    }

    /**
     * This test fails intermittently because Kudu doesn't have strong enough
     * semantics to support writing from multiple threads.
     */
    @Test(enabled = false)
    @Override
    public void testUpdateAllValues()
    {
        withTableName("test_update_all_columns", tableName -> {
            assertUpdate(createTableForWrites("CREATE TABLE %s (a INT, b INT, c INT)".formatted(tableName)));
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 2, 3), (11, 12, 13), (21, 22, 23)", 3);
            assertUpdate("UPDATE " + tableName + " SET b = b - 1, c = c * 2", 3);
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, 1, 6), (11, 11, 26), (21, 21, 46)");
        });
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
        String tableName = "negative_date_" + randomNameSuffix();

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
                .hasMessageContaining("For query")
                .hasMessageContaining("Actual rows")
                .hasMessageContaining("Expected rows");

        throw new SkipException("TODO");
    }

    @Test
    public void testLimitPushdown()
    {
        assertThat(query("SELECT name FROM nation LIMIT 30")).isNotFullyPushedDown(LimitNode.class); // Use high limit for result determinism
    }

    @Test
    @Override
    public void testDeleteWithComplexPredicate()
    {
        withTableName("test_delete_complex", tableName -> {
            assertUpdate(createTableForWrites("CREATE TABLE %s %s".formatted(tableName, ORDER_COLUMNS)));
            assertUpdate("INSERT INTO " + tableName + " SELECT * FROM orders", 15000);
            assertUpdate("DELETE FROM " + tableName + " WHERE orderkey % 2 = 0", "SELECT count(*) FROM orders WHERE orderkey % 2 = 0");
            assertQuery("SELECT * FROM " + tableName, "SELECT * FROM orders WHERE orderkey % 2 <> 0");

            assertUpdate("DELETE FROM " + tableName, "SELECT count(*) FROM orders WHERE orderkey % 2 <> 0");
            assertQuery("SELECT * FROM " + tableName, "SELECT custkey, orderkey FROM orders LIMIT 0");

            assertUpdate("DELETE FROM " + tableName + " WHERE rand() < 0", 0);
        });
    }

    @Test
    @Override
    public void testDeleteWithSubquery()
    {
        // TODO (https://github.com/trinodb/trino/issues/13210) Migrate these tests to AbstractTestEngineOnlyQueries
        withTableName("test_delete_subquery", tableName -> {
            assertUpdate(createTableForWrites("CREATE TABLE %s %s".formatted(tableName, NATION_COLUMNS)));
            assertUpdate("INSERT INTO " + tableName + " SELECT * FROM nation", 25);
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey IN (SELECT regionkey FROM region WHERE name LIKE 'A%')", 15);
            assertQuery(
                    "SELECT * FROM " + tableName,
                    "SELECT * FROM nation WHERE regionkey IN (SELECT regionkey FROM region WHERE name NOT LIKE 'A%')");
        });

        withTableName("test_delete_subquery", tableName -> {
            assertUpdate(createTableForWrites("CREATE TABLE %s %s".formatted(tableName, ORDER_COLUMNS)));
            assertUpdate("INSERT INTO " + tableName + " SELECT * FROM orders", 15000);

            // delete using a scalar and EXISTS subquery
            assertUpdate("DELETE FROM " + tableName + " WHERE orderkey = (SELECT orderkey FROM orders ORDER BY orderkey LIMIT 1)", 1);
            assertUpdate("DELETE FROM " + tableName + " WHERE orderkey = (SELECT orderkey FROM orders WHERE false)", 0);
            assertUpdate("DELETE FROM " + tableName + " WHERE EXISTS(SELECT 1 WHERE false)", 0);
            assertUpdate("DELETE FROM " + tableName + " WHERE EXISTS(SELECT 1)", "SELECT count(*) - 1 FROM orders");
        });

        withTableName("test_delete_subquery", tableName -> {
            assertUpdate(createTableForWrites("CREATE TABLE %s %s".formatted(tableName, NATION_COLUMNS)));
            assertUpdate("INSERT INTO " + tableName + " SELECT * FROM nation", 25);

            // delete using correlated EXISTS subquery
            assertUpdate(format("DELETE FROM %1$s WHERE EXISTS(SELECT regionkey FROM region WHERE regionkey = %1$s.regionkey AND name LIKE 'A%%')", tableName), 15);
            assertQuery(
                    "SELECT * FROM " + tableName,
                    "SELECT * FROM nation WHERE regionkey IN (SELECT regionkey FROM region WHERE name NOT LIKE 'A%')");
        });

        withTableName("test_delete_subquery", tableName -> {
            assertUpdate(createTableForWrites("CREATE TABLE %s %s".formatted(tableName, NATION_COLUMNS)));
            assertUpdate("INSERT INTO " + tableName + " SELECT * FROM nation", 25);
            // delete using correlated IN subquery
            assertUpdate(format("DELETE FROM %1$s WHERE regionkey IN (SELECT regionkey FROM region WHERE regionkey = %1$s.regionkey AND name LIKE 'A%%')", tableName), 15);
            assertQuery(
                    "SELECT * FROM " + tableName,
                    "SELECT * FROM nation WHERE regionkey IN (SELECT regionkey FROM region WHERE name NOT LIKE 'A%')");
        });
    }

    @Test
    @Override
    public void testDeleteWithSemiJoin()
    {
        withTableName("test_delete_semijoin", tableName -> {
            assertUpdate(createTableForWrites("CREATE TABLE %s %s".formatted(tableName, NATION_COLUMNS)));
            assertUpdate("INSERT INTO " + tableName + " SELECT * FROM nation", 25);
            // delete with multiple SemiJoin
            assertUpdate(
                    "DELETE FROM " + tableName + " " +
                            "WHERE regionkey IN (SELECT regionkey FROM region WHERE name LIKE 'A%') " +
                            "  AND regionkey IN (SELECT regionkey FROM region WHERE length(comment) < 50)",
                    10);
            assertQuery(
                    "SELECT * FROM " + tableName,
                    "SELECT * FROM nation " +
                            "WHERE regionkey IN (SELECT regionkey FROM region WHERE name NOT LIKE 'A%') " +
                            "  OR regionkey IN (SELECT regionkey FROM region WHERE length(comment) >= 50)");
        });

        withTableName("test_delete_semijoin", tableName -> {
            assertUpdate(createTableForWrites("CREATE TABLE %s %s".formatted(tableName, ORDER_COLUMNS)));
            assertUpdate("INSERT INTO " + tableName + " SELECT * FROM orders", 15000);

            // delete with SemiJoin null handling
            assertUpdate(
                    "DELETE FROM " + tableName + "\n" +
                            "WHERE (orderkey IN (SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END FROM tpch.tiny.lineitem)) IS NULL\n",
                    "SELECT count(*) FROM orders\n" +
                            "WHERE (orderkey IN (SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END FROM lineitem)) IS NULL\n");
            assertQuery(
                    "SELECT * FROM " + tableName,
                    "SELECT * FROM orders\n" +
                            "WHERE (orderkey IN (SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END FROM lineitem)) IS NOT NULL\n");
        });
    }

    @Test
    @Override
    public void testDeleteWithVarcharPredicate()
    {
        withTableName("test_delete_varchar", tableName -> {
            assertUpdate(createTableForWrites("CREATE TABLE %s %s".formatted(tableName, ORDER_COLUMNS)));
            assertUpdate("INSERT INTO " + tableName + " SELECT * FROM orders", 15000);
            assertUpdate("DELETE FROM " + tableName + " WHERE orderstatus = 'O'", "SELECT count(*) FROM orders WHERE orderstatus = 'O'");
            assertQuery("SELECT * FROM " + tableName, "SELECT * FROM orders WHERE orderstatus <> 'O'");
        });
    }

    @Test
    @Override
    public void testDeleteAllDataFromTable()
    {
        withTableName("test_delete_all_data", tableName -> {
            assertUpdate(createTableForWrites("CREATE TABLE %s %s".formatted(tableName, REGION_COLUMNS)));
            assertUpdate("INSERT INTO " + tableName + " SELECT * FROM region", 5);

            // not using assertUpdate as some connectors provide update count and some not
            getQueryRunner().execute("DELETE FROM " + tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 0");
        });
    }

    @Test
    @Override
    public void testRowLevelDelete()
    {
        withTableName("test_row_delete", tableName -> {
            assertUpdate(createTableForWrites("CREATE TABLE %s %s".formatted(tableName, REGION_COLUMNS)));
            assertUpdate("INSERT INTO " + tableName + " SELECT * FROM region", 5);
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 2", 1);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 4");
        });
    }

    /**
     * This test fails intermittently because Kudu doesn't have strong enough
     * semantics to support writing from multiple threads.
     */
    @Test(enabled = false)
    @Override
    public void testUpdate()
    {
        withTableName("test_update", tableName -> {
            assertUpdate(createTableForWrites("CREATE TABLE %s %s".formatted(tableName, NATION_COLUMNS)));
            assertUpdate("INSERT INTO " + tableName + " SELECT * FROM nation", 25);
            assertUpdate("UPDATE " + tableName + " SET nationkey = 100 + nationkey WHERE regionkey = 2", 5);
            assertThat(query("SELECT * FROM " + tableName))
                    .skippingTypesCheck()
                    .matches("SELECT IF(regionkey=2, nationkey + 100, nationkey) nationkey, name, regionkey, comment FROM tpch.tiny.nation");

            // UPDATE after UPDATE
            // Adding 1000 avoids duplicate keys
            assertUpdate("UPDATE " + tableName + " SET nationkey = nationkey * 2 + 1000 WHERE regionkey IN (2,3)", 10);
            assertThat(query("SELECT * FROM " + tableName))
                    .skippingTypesCheck()
                    .matches("SELECT CASE regionkey WHEN 2 THEN 2*(nationkey+100) + 1000 WHEN 3 THEN nationkey * 2 + 1000 ELSE nationkey END nationkey, name, regionkey, comment FROM tpch.tiny.nation");
        });
    }

    @Override
    public void testUpdateRowConcurrently()
            throws Exception
    {
        throw new SkipException("Kudu doesn't support concurrent update of different columns in a row");
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

    private void withTableName(String prefix, Consumer<String> consumer)
    {
        String tableName = "%s_%s".formatted(prefix, randomNameSuffix());
        try {
            consumer.accept(tableName);
        }
        catch (Exception e) {
            log.error(e);
            throw new RuntimeException(e);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }
}
