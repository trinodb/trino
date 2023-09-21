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
package io.trino.plugin.mysql;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public abstract class BaseMySqlConnectorTest
        extends BaseJdbcConnectorTest
{
    protected TestingMySqlServer mySqlServer;

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
                    SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM,
                    SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN,
                    SUPPORTS_NEGATIVE_DATE,
                    SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY,
                    SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                    SUPPORTS_RENAME_SCHEMA,
                    SUPPORTS_ROW_TYPE,
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

    @Override
    public void testShowColumns()
    {
        assertThat(query("SHOW COLUMNS FROM orders")).matches(getDescribeOrdersResult());
    }

    @Override
    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        return nullToEmpty(exception.getMessage()).matches(".*(Incorrect column name).*");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("timestamp(3) with time zone") ||
                typeName.equals("timestamp(6) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        if (typeName.equals("timestamp")) {
            // TODO this should either work or fail cleanly
            return Optional.empty();
        }

        if (typeName.equals("boolean")) {
            // MySQL does not have built-in support for boolean type. MySQL provides BOOLEAN as the synonym of TINYINT(1)
            // Querying the column with a boolean predicate subsequently fails with "Cannot apply operator: tinyint = boolean"
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
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

    @Override
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE mysql.tpch.orders (\n" +
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
                .setCatalog("mysql")
                .setSchema(getSession().getSchema())
                .build();

        assertFalse(getQueryRunner().tableExists(session, "test_table"));

        assertUpdate(session, "CREATE TABLE test_table AS SELECT 123 x", 1);
        assertTrue(getQueryRunner().tableExists(session, "test_table"));

        assertQuery(session, "SELECT * FROM test_table", "SELECT 123");

        assertUpdate(session, "DROP TABLE test_table");
        assertFalse(getQueryRunner().tableExists(session, "test_table"));
    }

    @Test
    public void testMySqlTinyint()
    {
        onRemoteDatabase().execute("CREATE TABLE tpch.mysql_test_tinyint1 (c_tinyint tinyint(1))");

        assertQuery("SHOW COLUMNS FROM mysql_test_tinyint1", "VALUES ('c_tinyint', 'tinyint', '', '')");

        onRemoteDatabase().execute("INSERT INTO tpch.mysql_test_tinyint1 VALUES (127), (-128)");
        MaterializedResult materializedRows = computeActual("SELECT * FROM tpch.mysql_test_tinyint1 WHERE c_tinyint = 127");
        assertEquals(materializedRows.getOnlyValue(), (byte) 127);

        assertUpdate("DROP TABLE mysql_test_tinyint1");
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
                "SELECT column_name, comment FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = 'test_column_comment'",
                "VALUES ('col1', 'test comment'), ('col2', null), ('col3', null)");

        assertUpdate("DROP TABLE test_column_comment");
    }

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
                    // MySQL adds implicit default value of '' for b_varchar
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

    @Test
    public void testLikeWithEscapePredicatePushdownWithCollation()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_like_with_escape_predicate_pushdown",
                "(id integer, a_varchar varchar(4) CHARACTER SET utf8 COLLATE utf8_bin)",
                List.of(
                        "1, 'A%b'",
                        "2, 'Asth'",
                        "3, 'ą%b'",
                        "4, 'ąsth'"))) {
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE a_varchar LIKE '%A\\%%' ESCAPE '\\'"))
                    .isFullyPushedDown();
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE a_varchar LIKE '%ą\\%%' ESCAPE '\\'"))
                    .isFullyPushedDown();
        }
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

    @Test(dataProvider = "charsetAndCollation")
    public void testPredicatePushdownWithCollationView(String charset, String collation)
    {
        onRemoteDatabase().execute(format("CREATE OR REPLACE VIEW tpch.test_view AS SELECT regionkey, nationkey, CONVERT(name USING %s) COLLATE %s AS name FROM tpch.nation;", charset, collation));
        testNationCollationQueries("test_view");
        onRemoteDatabase().execute("DROP VIEW tpch.test_view");
    }

    @Test(dataProvider = "charsetAndCollation")
    public void testPredicatePushdownWithCollation(String charset, String collation)
    {
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                "tpch.nation_collate",
                format("AS SELECT regionkey, nationkey, CONVERT(name USING %s) COLLATE %s AS name FROM tpch.nation", charset, collation))) {
            testNationCollationQueries(testTable.getName());
        }
    }

    private void testNationCollationQueries(String objectName)
    {
        // varchar like
        assertThat(query(format("SELECT regionkey, nationkey, name FROM %s WHERE name LIKE '%%ROM%%'", objectName)))
                .isFullyPushedDown();

        // varchar inequality
        assertThat(query(format("SELECT regionkey, nationkey, name FROM %s WHERE name != 'ROMANIA' AND name != 'ALGERIA'", objectName)))
                .isFullyPushedDown();

        // varchar equality
        assertThat(query(format("SELECT regionkey, nationkey, name FROM %s WHERE name = 'ROMANIA'", objectName)))
                .isFullyPushedDown();

        // varchar range
        assertThat(query(format("SELECT regionkey, nationkey, name FROM %s WHERE name BETWEEN 'POLAND' AND 'RPA'", objectName)))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(255)))")
                // We are not supporting range predicate pushdown for varchars
                .isNotFullyPushedDown(FilterNode.class);

        // varchar NOT IN
        assertThat(query(format("SELECT regionkey, nationkey, name FROM %s WHERE name NOT IN ('POLAND', 'ROMANIA', 'VIETNAM')", objectName)))
                .isFullyPushedDown();

        // varchar NOT IN with small compaction threshold
        assertThat(query(
                Session.builder(getSession())
                        .setCatalogSessionProperty("mysql", "domain_compaction_threshold", "1")
                        .build(),
                format("SELECT regionkey, nationkey, name FROM %s WHERE name NOT IN ('POLAND', 'ROMANIA', 'VIETNAM')", objectName)))
                // no pushdown because it was converted to range predicate
                .isNotFullyPushedDown(
                        node(
                                FilterNode.class,
                                // verify that no constraint is applied by the connector
                                tableScan(
                                        tableHandle -> ((JdbcTableHandle) tableHandle).getConstraint().isAll(),
                                        TupleDomain.all(),
                                        ImmutableMap.of())));

        // varchar IN without domain compaction
        assertThat(query(format("SELECT regionkey, nationkey, name FROM %s WHERE name IN ('POLAND', 'ROMANIA', 'VIETNAM')", objectName)))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(255))), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar(255)))")
                .isFullyPushedDown();

        // varchar IN with small compaction threshold
        assertThat(query(
                Session.builder(getSession())
                        .setCatalogSessionProperty("mysql", "domain_compaction_threshold", "1")
                        .build(),
                format("SELECT regionkey, nationkey, name FROM %s WHERE name IN ('POLAND', 'ROMANIA', 'VIETNAM')", objectName)))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(255))), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar(255)))")
                // no pushdown because it was converted to range predicate
                .isNotFullyPushedDown(
                        node(
                                FilterNode.class,
                                // verify that no constraint is applied by the connector
                                tableScan(
                                        tableHandle -> ((JdbcTableHandle) tableHandle).getConstraint().isAll(),
                                        TupleDomain.all(),
                                        ImmutableMap.of())));
        // varchar different case
        assertThat(query(format("SELECT regionkey, nationkey, name FROM %s WHERE name = 'romania'", objectName)))
                .returnsEmptyResult()
                .isFullyPushedDown();

        Session joinPushdownEnabled = joinPushdownEnabled(getSession());
        // join on varchar columns
        assertThat(query(joinPushdownEnabled, format("SELECT n.name, n2.regionkey FROM %1$s n JOIN %1$s n2 ON n.name = n2.name", objectName)))
                .joinIsNotFullyPushedDown();
    }

    @DataProvider
    public static Object[][] charsetAndCollation()
    {
        return new Object[][] {{"latin1", "latin1_general_cs"}, {"utf8", "utf8_bin"}};
    }

    /**
     * This test helps to tune TupleDomain simplification threshold.
     */
    @Test
    public void testNativeLargeIn()
    {
        // Using IN list of size 140_000 as bigger list causes error:
        // "com.mysql.jdbc.PacketTooBigException: Packet for query is too large (XXX > 1048576).
        //  You can change this value on the server by setting the max_allowed_packet' variable."
        onRemoteDatabase().execute("SELECT count(*) FROM tpch.orders WHERE " + getLongInClause(0, 140_000));
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

    @Override
    public void testNativeQueryInsertStatementTableDoesNotExist()
    {
        // override because MySQL succeeds in preparing query, and then fails because of no metadata available
        assertFalse(getQueryRunner().tableExists(getSession(), "non_existent_table"));
        assertThatThrownBy(() -> query("SELECT * FROM TABLE(system.query(query => 'INSERT INTO non_existent_table VALUES (1)'))"))
                .hasMessageContaining("Query not supported: ResultSetMetaData not available for query: INSERT INTO non_existent_table VALUES (1)");
    }

    @Override
    public void testNativeQueryIncorrectSyntax()
    {
        // override because MySQL succeeds in preparing query, and then fails because of no metadata available
        assertThatThrownBy(() -> query("SELECT * FROM TABLE(system.query(query => 'some wrong syntax'))"))
                .hasMessageContaining("Query not supported: ResultSetMetaData not available for query: some wrong syntax");
    }

    @Test
    public void testNativeQueryWithClause()
    {
        // MySQL JDBC driver < 8.0.29 didn't return metadata when the query contained a WITH clause
        assertQuery(
                    """
                    SELECT * FROM TABLE(mysql.system.query(query => '
                    WITH t AS (SELECT DISTINCT custkey FROM tpch.orders)
                    SELECT custkey, name FROM tpch.customer
                    WHERE custkey = 1
                    '))
                    """,
                "VALUES (1, 'Customer#000000001')");
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
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("Identifier name .* is too long");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(64);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("Identifier name .* is too long");
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(64);
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return mySqlServer::execute;
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
    public void verifyMySqlJdbcDriverNegativeDateHandling()
            throws Exception
    {
        LocalDate negativeDate = LocalDate.of(-1, 1, 1);
        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.verify_negative_date", "(dt DATE)")) {
            // Direct insert to database fails due to validation on database side
            assertThatThrownBy(() -> onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES (DATE '" + negativeDate + "')"))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageMatching(".*\\QIncorrect DATE value: '" + negativeDate + "'\\E");

            // Insert via prepared statement succeeds but writes incorrect value due to bug in driver
            try (Connection connection = mySqlServer.createConnection();
                    PreparedStatement insert = connection.prepareStatement("INSERT INTO " + table.getName() + " VALUES (?)")) {
                insert.setObject(1, negativeDate);
                int affectedRows = insert.executeUpdate();
                assertThat(affectedRows).isEqualTo(1);
            }

            try (Connection connection = mySqlServer.createConnection();
                    ResultSet resultSet = connection.createStatement().executeQuery("SELECT dt FROM " + table.getName())) {
                while (resultSet.next()) {
                    LocalDate dateReadBackFromMySql = resultSet.getObject(1, LocalDate.class);
                    assertThat(dateReadBackFromMySql).isNotEqualTo(negativeDate);
                    assertThat(dateReadBackFromMySql.toString()).isEqualTo("0002-01-01");
                }
            }
        }
    }
}
