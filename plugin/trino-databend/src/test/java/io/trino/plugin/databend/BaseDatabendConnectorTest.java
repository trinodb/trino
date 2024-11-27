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

import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.util.List;
import java.util.OptionalInt;

import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseDatabendConnectorTest
        extends BaseJdbcConnectorTest
{
    protected TestingDatabendServer databendServer;

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_AGGREGATION_PUSHDOWN,
                    SUPPORTS_JOIN_PUSHDOWN -> true;
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                    SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION,
                    SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT,
                    SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE,
                    SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION,
                    SUPPORTS_ARRAY,
                    SUPPORTS_COMMENT_ON_COLUMN,
                    SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                    SUPPORTS_DROP_NOT_NULL_CONSTRAINT,
                    SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM,
                    SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN,
                    SUPPORTS_NEGATIVE_DATE,
                    SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY,
                    SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                    SUPPORTS_SET_COLUMN_TYPE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                "tpch.table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    protected TestTable createTableWithUnsupportedColumn()
    {
        return new TestTable(
                onRemoteDatabase(),
                "tpch.test_unsupported_column_present",
                "(one bigint, two decimal(50,0), three varchar(10))");
    }

    @Test
    @Override
    public void testShowColumns()
    {
        assertThat(query("SHOW COLUMNS FROM orders")).result().matches(getDescribeOrdersResult());
    }

    @Override
    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        return nullToEmpty(exception.getMessage()).matches(".*(Incorrect column name).*");
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar(255)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(255)", "", "")
                .row("clerk", "varchar(255)", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar(255)", "", "")
                .build();
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE databend.tpch.orders (\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar(255),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(255),\n" +
                        "   clerk varchar(255),\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar(255)\n" +
                        ")");
    }

    @Test
    @Override
    public void testDeleteWithLike()
    {
        assertThatThrownBy(super::testDeleteWithLike)
                .hasStackTraceContaining("TrinoException: " + MODIFYING_ROWS_MESSAGE);
    }

    @Test
    public void testViews()
    {
        onRemoteDatabase().execute("CREATE OR REPLACE VIEW tpch.test_view AS SELECT * FROM tpch.orders");
        assertQuery("SELECT orderkey FROM test_view", "SELECT orderkey FROM orders");
        onRemoteDatabase().execute("DROP VIEW IF EXISTS tpch.test_view");
    }

    @Test
    public void testNameEscaping()
    {
        Session session = testSessionBuilder()
                .setCatalog("databend")
                .setSchema(getSession().getSchema())
                .build();

        assertThat(getQueryRunner().tableExists(session, "test_table")).isFalse();

        assertUpdate(session, "CREATE TABLE test_table AS SELECT 123 x", 1);
        assertThat(getQueryRunner().tableExists(session, "test_table")).isTrue();

        assertQuery(session, "SELECT * FROM test_table", "SELECT 123");

        assertUpdate(session, "DROP TABLE test_table");
        assertThat(getQueryRunner().tableExists(session, "test_table")).isFalse();
    }

    @Test
    public void testDatabendTinyint()
    {
        onRemoteDatabase().execute("CREATE TABLE tpch.databend_test_tinyint1 (c_tinyint TINYINT)");

        assertQuery("SHOW COLUMNS FROM databend_test_tinyint1", "VALUES ('c_tinyint', 'tinyint', '', '')");

        onRemoteDatabase().execute("INSERT INTO tpch.databend_test_tinyint1 VALUES (127), (-128)");
        MaterializedResult materializedRows = computeActual("SELECT * FROM tpch.databend_test_tinyint1 WHERE c_tinyint = 127");
        assertThat(materializedRows.getOnlyValue())
                .isEqualTo((byte) 127);

        assertUpdate("DROP TABLE databend_test_tinyint1");
    }

    @Override
    protected String errorMessageForCreateTableAsSelectNegativeDate(String date)
    {
        return format("Failed to insert data: Data truncation: Incorrect datetime value: '%s'", date);
    }

    @Override
    protected String errorMessageForInsertNegativeDate(String date)
    {
        return format("Failed to insert data: Data truncation: Incorrect datetime value: '%s'", date);
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return format("Failed to insert data: Field '%s' doesn't have a default value", columnName);
    }

    @Test
    public void testColumnComment()
    {
        // TODO add support for setting comments on existing column and replace the test with io.trino.testing.BaseConnectorTest#testCommentColumn

        onRemoteDatabase().execute("CREATE TABLE tpch.test_column_comment (col1 bigint COMMENT 'test comment', col2 bigint COMMENT '', col3 bigint)");

        assertQuery(
                "SELECT column_name, column_comment FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = 'test_column_comment'",
                "VALUES ('col1', 'test comment'), ('col2', ''), ('col3', '')");

        assertUpdate("DROP TABLE test_column_comment");
    }

    @Test
    @Override
    public void testAddNotNullColumn()
    {
        assertThatThrownBy(super::testAddNotNullColumn)
                .isInstanceOf(AssertionError.class)
                .hasMessage("Should fail to add not null column without a default value to a non-empty table");

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_nn_col", "(a_varchar varchar)")) {
            String tableName = table.getName();

            assertUpdate("INSERT INTO " + tableName + " VALUES ('a')", 1);
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b_varchar varchar NOT NULL");
            assertThat(query("TABLE " + tableName))
                    .skippingTypesCheck()
                    .matches("VALUES ('a', '')");
        }
    }

    @Test
    public void testLikePredicatePushdownWithCollation()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_like_predicate_pushdown",
                "(id integer, a_varchar varchar(1) CHARACTER SET utf8 COLLATE utf8_bin)",
                List.of(
                        "1, 'A'",
                        "2, 'a'",
                        "3, 'B'",
                        "4, 'ą'",
                        "5, 'Ą'"))) {
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE a_varchar LIKE '%A%'"))
                    .isFullyPushedDown();
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE a_varchar LIKE '%ą%'"))
                    .isFullyPushedDown();
        }
    }

    private DataSetup databendCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new DatabendSqlExecutor(onRemoteDatabase()), tableNamePrefix);
    }

    @Test
    public void testDatabendNullPushdown()
    {
        TestNullPushdownDataType.connectorExpressionOnly()
                .addSpecialColumn("String", "'z'", "CAST('z' AS varchar)")
                .addTestCase("Nullable(decimal(3, 1))")
                .addTestCase("Nullable(decimal(30, 5))")
                .execute(getQueryRunner(), databendCreateAndInsert("tpch.test_is_null"));

        TestNullPushdownDataType.create()
                .addSpecialColumn("String", "'z'", "CAST('z' AS varchar)")
                .addTestCase("Nullable(tinyint)")
                .addTestCase("Nullable(smallint)")
                .addTestCase("Nullable(integer)")
                .addTestCase("Nullable(bigint)")
                .addTestCase("Nullable(UInt8)")
                .addTestCase("Nullable(UInt16)")
                .addTestCase("Nullable(UInt32)")
                .addTestCase("Nullable(UInt64)")
                .addTestCase("Nullable(double)")
                .addTestCase("Nullable(varchar(30))")
                .addTestCase("Nullable(String)")
                .addTestCase("Nullable(date)")
                .addTestCase("Nullable(datetime)")
                .execute(getQueryRunner(), databendCreateAndInsert("tpch.test_is_null"));
    }

    @Test
    public void testPredicatePushdown()
    {
        // varchar like
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name LIKE '%ROM%'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(255)))")
                .isNotFullyPushedDown(FilterNode.class);

        // varchar equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(255)))")
                .isNotFullyPushedDown(FilterNode.class);

        // varchar range
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(255)))")
                .isNotFullyPushedDown(FilterNode.class);

        // varchar different case
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania'"))
                .returnsEmptyResult()
                .isNotFullyPushedDown(FilterNode.class);

        // bigint equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey = 19"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(255)))")
                .isFullyPushedDown();

        // bigint range, with decimal to bigint simplification
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey BETWEEN 18.5 AND 19.5"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(255)))")
                .isFullyPushedDown();

        // date equality
        assertThat(query("SELECT orderkey FROM orders WHERE orderdate = DATE '1992-09-29'"))
                .matches("VALUES BIGINT '1250', 34406, 38436, 57570")
                .isFullyPushedDown();

        onRemoteDatabase().execute("CREATE TABLE tpch.binary_test (x int, y varbinary(100))");
        onRemoteDatabase().execute("INSERT INTO tpch.binary_test VALUES (3, from_base64('AFCBhLrkidtNTZcA9Ru3hw=='))");

        // varbinary equality
        assertThat(query("SELECT x, y FROM tpch.binary_test WHERE y = from_base64('AFCBhLrkidtNTZcA9Ru3hw==')"))
                .matches("VALUES (3, from_base64('AFCBhLrkidtNTZcA9Ru3hw=='))")
                .isFullyPushedDown();

        onRemoteDatabase().execute("DROP TABLE tpch.binary_test");

        // predicate over aggregation key (likely to be optimized before being pushed down into the connector)
        assertThat(query("SELECT * FROM (SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey) WHERE regionkey = 3"))
                .matches("VALUES (BIGINT '3', BIGINT '77')")
                .isFullyPushedDown();

        // predicate over aggregation result
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey HAVING sum(nationkey) = 77"))
                .matches("VALUES (BIGINT '3', BIGINT '77')")
                .isFullyPushedDown();
    }

    /**
     * This test helps to tune TupleDomain simplification threshold.
     */
    @Test
    public void testNativeMultipleInClauses()
    {
        String longInClauses = range(0, 14)
                .mapToObj(value -> getLongInClause(value * 10_000, 10_000))
                .collect(joining(" OR "));
        onRemoteDatabase().execute("SELECT count(*) FROM tpch.orders WHERE " + longInClauses);
    }

    @Test
    @Override
    public void testNativeQueryInsertStatementTableDoesNotExist()
    {
        // override because Databend succeeds in preparing query, and then fails because of no metadata available
        assertThat(getQueryRunner().tableExists(getSession(), "non_existent_table")).isFalse();
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'INSERT INTO non_existent_table VALUES (1)'))"))
                .failure().hasMessageContaining("Unknown table");
    }

    private String getLongInClause(int start, int length)
    {
        String longValues = range(start, start + length)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        return "orderkey IN (" + longValues + ")";
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(64);
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return databendServer::execute;
    }

    @Override
    protected Session joinPushdownEnabled(Session session)
    {
        return Session.builder(super.joinPushdownEnabled(session))
                // strategy is AUTOMATIC by default and would not work for certain test cases (even if statistics are collected)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_strategy", "EAGER")
                .build();
    }

    @Test
    public void verifyDatabendJdbcDriverNegativeDateHandling()
            throws Exception
    {
        LocalDate negativeDate = LocalDate.of(-1, 1, 1);
        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.verify_negative_date", "(dt DATE)")) {
            // Insert via prepared statement succeeds but writes incorrect value due to bug in driver
            try (Connection connection = databendServer.createConnection();
                    PreparedStatement insert = connection.prepareStatement("INSERT INTO " + table.getName() + " VALUES (?)")) {
                insert.setObject(1, negativeDate);
                int affectedRows = insert.executeUpdate();
                assertThat(affectedRows).isEqualTo(1);
            }

            try (Connection connection = databendServer.createConnection();
                    ResultSet resultSet = connection.createStatement().executeQuery("SELECT dt FROM " + table.getName())) {
                while (resultSet.next()) {
                    LocalDate dateReadBackFromDatabend = resultSet.getObject(1, LocalDate.class);
                    assertThat(dateReadBackFromDatabend).isNotEqualTo(negativeDate);
                    assertThat(dateReadBackFromDatabend.toString()).isEqualTo("0002-01-01");
                }
            }
        }
    }
}
