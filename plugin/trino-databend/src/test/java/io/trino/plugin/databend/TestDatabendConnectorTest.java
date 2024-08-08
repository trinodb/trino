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

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.SystemSessionProperties.IGNORE_STATS_CALCULATOR_FAILURES;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ADD_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ADD_COLUMN_WITH_COMMENT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DROP_NOT_NULL_CONSTRAINT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_INSERT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_NATIVE_QUERY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ROW_LEVEL_DELETE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_UPDATE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestDatabendConnectorTest
        extends BaseJdbcConnectorTest
{
    protected TestingDatabendServer databendServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        databendServer = closeAfterClass(new TestingDatabendServer(TestingDatabendServer.DATABEND_DEFAULT_IMAGE));
        return DatabendQueryRunner.builder(databendServer)
                .addConnectorProperty("databend.connection-timeout", Duration.valueOf("60s").toString())
                .addConnectorProperty("databend.presigned-url-disabled", "true")
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    @Disabled
    @Test
    public void testAddColumnWithPosition(){}

    @Test
    @Disabled
    @Override
    public void testBulkColumnListingOptions()
    {
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            // Tests requires s3 presigned url to be disabled
            /*
            the query plan of databend is like: Output[columnNames = [_col0, _col1]]
│   Layout: [pfgnrtd:double, pfgnrtd_1:real]
│   _col0 := pfgnrtd
│   _col1 := pfgnrtd_1
└─ TableScan[table = databend:Query[SELECT corr("t_double", "u_double") AS "_pfgnrtd_0", corr("v_real", "w_real") AS "_pfgnrtd_1" FROM "tpch"."test_corr_pushdownjp0d6s0d4r"] columns=[_pfgnrtd_0:double:Float64, _pfgnrtd_1:real:Float32]]
       Layout: [pfgnrtd:double, pfgnrtd_1:real]
       pfgnrtd := _pfgnrtd_0:double:Float64
       pfgnrtd_1 := _pfgnrtd_1:real:Float32

            **/
            case SUPPORTS_AGGREGATION_PUSHDOWN,
                    SUPPORTS_JOIN_PUSHDOWN,
                    SUPPORTS_LIMIT_PUSHDOWN,
                    SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION,
                    SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION,
                    SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV,
                    SUPPORTS_DYNAMIC_FILTER_PUSHDOWN,
                    SUPPORTS_TOPN_PUSHDOWN -> false;

            case SUPPORTS_ARRAY,
                    SUPPORTS_COMMENT_ON_COLUMN,
                    SUPPORTS_NEGATIVE_DATE, // min date is 0001-01-01
                    SUPPORTS_ROW_TYPE,
                    SUPPORTS_DROP_NOT_NULL_CONSTRAINT,
                    SUPPORTS_ROW_LEVEL_DELETE,
                    SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                    SUPPORTS_INSERT,
                    SUPPORTS_NATIVE_QUERY,
                    SUPPORTS_SET_COLUMN_TYPE,
                    SUPPORTS_UPDATE -> false;

            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void testNumericAggregationPushdown()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        try (TestTable testTable = createAggregationTestTable(schemaName + ".test_aggregation_pushdown",
                ImmutableList.of("100.000, 100000000.000000000, 100.000, 100000000", "123.321, 123456789.987654321, 123.321, 123456789"))) {
            assertThat(query("SELECT min(short_decimal), min(long_decimal), min(a_bigint), min(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT max(short_decimal), max(long_decimal), max(a_bigint), max(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT sum(short_decimal), sum(long_decimal), sum(a_bigint), sum(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT avg(a_bigint), avg(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT avg(short_decimal), avg(long_decimal) FROM " + testTable.getName())).isNotFullyPushedDown(AggregationNode.class);
        }
    }

    @Test
    @Override
    public void testRenameTableAcrossSchema()
    {
        abort("skip this because Databend doesn't support renaming table across schema");
    }

    @Override
    @Test
    public void testRenameSchemaToLongName()
    {
        // Override because the max length is different from CREATE SCHEMA case
        String sourceTableName = "test_rename_source_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + sourceTableName);

        String baseSchemaName = "test_rename_target_" + randomNameSuffix();

        int maxLength = 255 - ".sql".length();

        String validTargetSchemaName = baseSchemaName + "z".repeat(maxLength - baseSchemaName.length());
        assertUpdate("ALTER SCHEMA " + sourceTableName + " RENAME TO " + validTargetSchemaName);
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(validTargetSchemaName);
        assertUpdate("DROP SCHEMA " + validTargetSchemaName);

        assertUpdate("CREATE SCHEMA " + sourceTableName);
        String invalidTargetSchemaName = validTargetSchemaName + "z";
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain(invalidTargetSchemaName);
    }

    @Test
    @Override
    public void testDataMappingSmokeTest()
    {
        abort("skip this because some cases are not supported"); //  .add(new DataMappingTestSetup("boolean", "false", "true"))
    }

    @Test
    @Disabled
    @Override
    public void testTopNPushdown() {} // Databend query plan is different

    /*
    Output[columnNames = [orderkey]]
│   Layout: [orderkey:bigint]
└─ TableScan[table = databend:tpch.orders tpch.tpch.orders sortOrder=[orderkey:bigint:Int64 ASC NULLS LAST] limit=10 columns=[orderkey:bigint:Int64]]
       Layout: [orderkey:bigint]
       orderkey := orderkey:bigint:Int64

    * */

    @Test
    @Disabled
    @Override
    public void testStddevAggregationPushdown() {}

    @Test
    @Override
    public void testSortItemsReflectedInExplain()
    {
        assertExplain(
                "EXPLAIN SELECT name FROM nation ORDER BY nationkey DESC NULLS LAST LIMIT 5",
                ".*nationkey.*DESC.*NULLS LAST.*");
    }

    @Test
    @Disabled
    @Override
    public void testVarianceAggregationPushdown() {}

    @Test
    @Disabled
    @Override
    public void verifySupportsRowLevelDeleteDeclaration() {}

    @Test
    @Override
    public void testAddColumnWithComment()
    {
        // Override because the default storage type doesn't support adding columns
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_col_desc_", "(a_varchar varchar NOT NULL)")) {
            String tableName = table.getName();

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b_varchar varchar COMMENT 'test new column comment'");
            assertThat(getColumnComment(tableName, "b_varchar")).isEqualTo("'test new column comment'");

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN empty_comment varchar COMMENT ''");
            assertThat(getColumnComment(tableName, "empty_comment")).isNull();
        }
    }

    @Test
    @Disabled
    @Override
    public void testUpdateNotNullColumn() {}

    @Test
    @Disabled
    @Override
    public void testSelectVersionOfNonExistentTable() {}

    @Test
    @Override
    public void testAddNotNullColumnToEmptyTable()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_notnull_col_to_empty", "(a_varchar varchar NOT NULL)")) {
            String tableName = table.getName();

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b_varchar varchar NOT NULL");
            assertUpdate("INSERT INTO " + tableName + " VALUES ('a', 'b')", 1);
            assertThat(query("TABLE " + tableName))
                    .skippingTypesCheck()
                    .matches("VALUES ('a', 'b')");
        }
    }

    @Test
    @Disabled
    @Override
    public void testAddColumnConcurrently()
    {
        // TODO: Enable this test after finding the failure cause
    }

    @Test
    @Override
    public void testExecuteProcedureWithNamedArgument()
    {
        String tableName = "test_execute" + randomNameSuffix();
        String schemaTableName = getSession().getSchema().orElseThrow() + "." + tableName;

        assertUpdate("CREATE TABLE " + schemaTableName + "(a int)");
        try {
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
            assertUpdate("DROP TABLE " + schemaTableName + "");
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + schemaTableName);
        }
    }

    @Test
    @Override
    public void testRenameColumnWithComment()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_rename_column_",
                "(id INT NOT NULL, col INT COMMENT 'test column comment')")) {
            assertThat(getColumnComment(table.getName(), "col")).isEqualTo("'test column comment'");

            assertUpdate("ALTER TABLE " + table.getName() + " RENAME COLUMN col TO renamed_col");
            assertThat(getColumnComment(table.getName(), "renamed_col")).isEqualTo("'test column comment'");
        }
    }

    @Test
    @Override
    public void testColumnName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        for (String columnName : testColumnNameDataProvider()) {
            testColumnName(columnName, true);
        }
    }

    @Override
    protected void testColumnName(String columnName, boolean delimited)
    {
        String nameInSql = toColumnNameInSql(columnName, delimited);
        String tableName = "tcn_" + nameInSql.toLowerCase(ENGLISH).replaceAll("[^a-z0-9]", "") + randomNameSuffix();

        try {
            assertUpdate("CREATE TABLE " + tableName + "(key varchar(50), " + nameInSql + " varchar(50))");
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

            // predicate
            assertQuery("SELECT key FROM " + tableName + " WHERE " + nameInSql + " IS NULL", "VALUES ('null value')");
            assertQuery("SELECT key FROM " + tableName + " WHERE " + nameInSql + " = 'abc'", "VALUES ('sample value')");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    private static String toColumnNameInSql(String columnName, boolean delimited)
    {
        String nameInSql = columnName;
        if (delimited) {
            nameInSql = "\"" + columnName.replace("\"", "\"\"") + "\"";
        }
        return nameInSql;
    }

    @Override
    public List<String> testColumnNameDataProvider()
    {
        return testColumnNameTestData().stream()
                .map(this::filterColumnNameTestData)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());
    }

    private List<String> testColumnNameTestData()
    {
        return ImmutableList.<String>builder()
                .add("lowercase")
                .add("UPPERCASE")
                .add("MixedCase")
                .add("atrailingspace ")
                .add("an'apostrophe")
                .add("adigit0")
                .add("0startwithdigit")
                .add("カラム")
                .build();
    }

    @Test
    @Override
    public void testTrySelectTableVersion()
    {
        abort("skip this");
    }

    @Test
    @Override
    public void verifySupportsNativeQueryDeclaration()
    {
        if (hasBehavior(SUPPORTS_NATIVE_QUERY)) {
            // Covered by testNativeQuerySelectFromNation
            return;
        }
        assertQueryFails(
                format("SELECT * FROM TABLE(system.query(query => 'SELECT name FROM %s.nation WHERE nationkey = 0'))", getSession().getSchema().orElseThrow()),
                ".* ResultSetMetaData not available for query.*");
    }

    @Test
    @Override
    @Disabled
    public void testExplainAnalyze() {}

    @Test
    @Override
    @Disabled
    public void testTableSampleWithFiltering() {}

    @Test
    @Override
    @Disabled
    public void testExplainAnalyzeVerbose() {}

    @Test
    @Override
    public void testInsertInPresenceOfNotSupportedColumn()
    {
        abort();
    }

    @Test
    @Override
    public void testVarcharCharComparison()
    {
        abort("skip for char");
    }

    @Test
    @Override
    public void testJoin()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(IGNORE_STATS_CALCULATOR_FAILURES, "false")
                .build();

        // 2 inner joins, eligible for join reodering
        assertQuery(
                session,
                "SELECT 'c.name', 'n.name', 'r.name' " +
                        "FROM nation n " +
                        "JOIN customer c ON 'c.nationkey' = 'n.nationkey' " +
                        "JOIN region r ON 'n.regionkey' = 'r.regionkey'");

        // 2 inner joins, eligible for join reodering, where one table has a filter
        assertQuery(
                session,
                "SELECT 'c.name', 'n.name', 'r.name' " +
                        "FROM nation n " +
                        "JOIN customer c ON 'c.nationkey' = 'n.nationkey' " +
                        "JOIN region r ON 'n.regionkey' = 'r.regionkey' " +
                        "WHERE n.name = 'ARGENTINA'");

        // 2 inner joins, eligible for join reodering, on top of aggregation
        assertQuery(
                session,
                "SELECT 'c.name', 'n.name', 'n.count', 'r.name' " +
                        "FROM (SELECT 'name', 'regionkey', 'nationkey', count(*) count FROM nation GROUP BY 'name', 'regionkey', 'nationkey') n " +
                        "JOIN customer c ON 'c.nationkey' = 'n.nationkey' " +
                        "JOIN region r ON 'n.regionkey' = 'r.regionkey'");
    }

    @Test
    @Override
    public void testNativeQueryColumnAliasNotFound()
    {
        abort();
    }

    @Test
    @Override
    public void testNativeQueryIncorrectSyntax()
    {
        abort();
    }

    @Test
    @Override
    public void testAddColumnWithCommentSpecialCharacter()
    {
        testAddColumnWithCommentSpecialCharacter("a;semicolon");
        testAddColumnWithCommentSpecialCharacter("an@at");
        testAddColumnWithCommentSpecialCharacter("a\"quote");
        testAddColumnWithCommentSpecialCharacter("a`backtick`");
        testAddColumnWithCommentSpecialCharacter("a/slash");
        testAddColumnWithCommentSpecialCharacter("a\\backslash");
        testAddColumnWithCommentSpecialCharacter("a?question");
        testAddColumnWithCommentSpecialCharacter("[square bracket]");
    }

    @Test
    @Override
    public void testDeleteWithVarcharEqualityPredicate()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE) && hasBehavior(SUPPORTS_ROW_LEVEL_DELETE));
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_varchar", "(col varchar(1))", ImmutableList.of("'a'", "'A'", "null"))) {
            assertQuery("DELETE FROM " + table.getName() + " WHERE col = 'A'");
            assertQuery("SELECT * FROM " + table.getName(), "VALUES 'a', null");
        }
    }

    @Test
    @Override
    public void testDeleteWithVarcharInequalityPredicate()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE) && hasBehavior(SUPPORTS_ROW_LEVEL_DELETE));
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_varchar", "(col varchar(1))", ImmutableList.of("'a'", "'A'", "null"))) {
            assertQuery("DELETE FROM " + table.getName() + " WHERE col != 'A'");
            assertQuery("SELECT * FROM " + table.getName(), "VALUES 'A', null");
        }
    }

    @Test
    @Disabled
    @Override
    public void testCovarianceAggregationPushdown() {}

    @Test
    @Disabled
    @Override
    public void testCorrAggregationPushdown() {}

    @Test
    @Disabled
    @Override
    public void testAggregationPushdown() {}

    @Test
    @Disabled
    @Override
    public void testDelete() {}

    @Test
    @Override
    public void testDeleteWithVarcharGreaterAndLowerPredicate()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE) && hasBehavior(SUPPORTS_ROW_LEVEL_DELETE));
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_varchar", "(col varchar(1))", ImmutableList.of("'0'", "'a'", "'A'", "'b'", "null"))) {
            assertQuery("DELETE FROM " + table.getName() + " WHERE col < 'A'");
            assertQuery("SELECT * FROM " + table.getName(), "VALUES 'a', 'A', 'b', null");
            assertQuery("DELETE FROM " + table.getName() + " WHERE col > 'A'");
            assertQuery("SELECT * FROM " + table.getName(), "VALUES 'A', null");
        }
    }

    @Override
    protected void testAddColumnWithCommentSpecialCharacter(String comment)
    {
        skipTestUnless(hasBehavior(SUPPORTS_ADD_COLUMN_WITH_COMMENT));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_col_", "(a_varchar varchar)")) {
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN b_varchar varchar COMMENT " + varcharLiteral(comment));
            assertThat(getColumnComment(table.getName(), "b_varchar")).isEqualTo("'" + comment + "'");
        }
    }

    protected static String varcharLiteral(String value)
    {
        requireNonNull(value, "value is null");
        return "'" + value.replace("'", "''") + "'";
    }

    @Override
    @Test
    public void testCreateSchemaWithLongName()
    {
        abort("Dropping schema with long name causes Databend to return code 500");
    }

    @Test
    @Override
    public void testInsert()
    {
        if (!hasBehavior(SUPPORTS_INSERT)) {
            return;
        }

        // We are using SUPPORTS_CREATE_TABLE_WITH_DATA because WITH NO DATA is using same execution code paths
        if (!hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA)) {
            throw new AssertionError("Cannot test INSERT without CTAS, the test needs to be implemented in a connector-specific way");
        }

        String query = "SELECT name, nationkey, regionkey FROM nation";

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_", "AS " + query + " WITH NO DATA")) {
            assertQuery("SELECT count(*) FROM " + table.getName() + "", "SELECT 0");

            assertUpdate("INSERT INTO " + table.getName() + " " + query, 25);

            assertQuery("SELECT * FROM " + table.getName() + "", query);

            // UNION query produces columns in the opposite order
            // of how they are declared in the table schema
            assertUpdate(
                    "INSERT INTO " + table.getName() + " (nationkey, name, regionkey) " +
                            "SELECT nationkey, name, regionkey FROM nation " +
                            "UNION ALL " +
                            "SELECT nationkey, name, regionkey FROM nation",
                    50);
        }
    }

    @Test
    @Override
    public void testCreateTableAsSelectNegativeDate()
    {
        assertThatThrownBy(super::testCreateTableAsSelectNegativeDate)
                .hasStackTraceContaining("input is out of range");
    }

    @Test
    @Override
    public void testInsertNegativeDate()
    {
        assertThatThrownBy(super::testInsertNegativeDate)
                .hasStackTraceContaining("input is out of range");
    }

    @Test
    @Disabled
    @Override
    public void testLimitPushdown() {}

    @Test
    @Override
    public void testInsertIntoNotNullColumn()
    {
        abort("TODO test");
    }

    @Test
    @Override
    public void testNativeQuerySimple()
    {
        skipTestUnless(hasBehavior(SUPPORTS_NATIVE_QUERY));
        assertQuery("SELECT * FROM TABLE(system.query(query => 'SELECT 1'))", "VALUES 1");
    }

    @Test
    @Override
    public void testAddColumn()
    {
        if (!hasBehavior(SUPPORTS_ADD_COLUMN)) {
            assertQueryFails("ALTER TABLE nation ADD COLUMN test_add_column bigint", "This connector does not support adding columns");
            return;
        }

        String tableName;
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_column_", tableDefinitionForAddColumn())) {
            tableName = table.getName();
            assertUpdate("INSERT INTO " + table.getName() + " SELECT 'first'", 1);
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN x bigint", ".* Column 'x' already exists");
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN X bigint", ".* Column 'X' already exists");
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN q bad_type", ".* Unknown type 'bad_type' for column 'q'");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN a varchar(50)");
            // Verify table state after adding a column, but before inserting anything to it
            assertQuery(
                    "SELECT * FROM " + table.getName(),
                    "VALUES ('first', NULL)");
            assertQuery(
                    "SELECT * FROM " + table.getName() + " WHERE a IS NULL",
                    "VALUES ('first', NULL)");
        }

        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " ADD COLUMN x bigint");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " ADD COLUMN IF NOT EXISTS x bigint");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    @Test
    @Disabled
    @Override
    public void verifySupportsUpdateDeclaration()
    {
    }

    @Test
    @Override
    public void testCreateTableAsSelectWithTableComment()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));

        String tableName = "test_ctas_" + randomNameSuffix();

        if (!hasBehavior(SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT)) {
            assertQueryFails("CREATE TABLE " + tableName + " COMMENT 'test comment' AS SELECT name FROM nation", "This connector does not support creating tables with table comment");
            return;
        }

        assertUpdate("CREATE TABLE " + tableName + " COMMENT 'test comment' AS SELECT name FROM nation", 25);
        assertThat(getTableComment(getSession().getCatalog().orElseThrow(), getSession().getSchema().orElseThrow(), tableName)).isEqualTo("test comment");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override
    public void testCreateTableAsSelect()
    {
        if (!hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA)) {
            assertQueryFails("CREATE TABLE xxxx AS SELECT BIGINT '42' a, DOUBLE '-38.5' b", "This connector does not support creating tables with data");
            return;
        }

        String tableName = "test_create_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT BIGINT '42' a, DOUBLE '-38.5' b", 1);
        assertThat(query("SELECT CAST(a AS bigint), b FROM " + tableName))
                .matches("VALUES (BIGINT '42', -385e-1)");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override
    public void testCreateTableWithColumnComment()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        String tableName = "test_create_" + randomNameSuffix();

        if (!hasBehavior(SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT)) {
            assertQueryFails("CREATE TABLE " + tableName + " (a bigint COMMENT 'test comment')", "This connector does not support creating tables with column comment");
            return;
        }

        assertUpdate("CREATE TABLE " + tableName + " (a bigint COMMENT 'test comment')");
        assertThat(getColumnComment(tableName, "a").replace("'", "")).isEqualTo("test comment");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override
    public void testCreateTableWithColumnCommentSpecialCharacter()
    {
        testCreateTableWithColumnCommentSpecialCharacter("a;semicolon");
        testCreateTableWithColumnCommentSpecialCharacter("an@at");
        testCreateTableWithColumnCommentSpecialCharacter("a\"quote");
        testCreateTableWithColumnCommentSpecialCharacter("a`backtick`");
        testCreateTableWithColumnCommentSpecialCharacter("a/slash");
        testCreateTableWithColumnCommentSpecialCharacter("a\\backslash");
        testCreateTableWithColumnCommentSpecialCharacter("a?question");
        testCreateTableWithColumnCommentSpecialCharacter("[square bracket]");
    }

    @Test
    @Override
    public void testDropNotNullConstraint()
    {
        abort("not support");
    }

    @Test
    @Override
    public void testDistinctAggregationPushdown()
    {
        abort("Databend query plan");
    }

    @Test
    @Override
    public void testDistinctHaving()
    {
        // wait this pr: https://github.com/databendlabs/databend-jdbc/pull/290
//        assertQuery("SELECT COUNT(DISTINCT clerk) AS count " +
//                "FROM orders " +
//                "GROUP BY orderdate " +
//                "HAVING COUNT(DISTINCT clerk) > 1");
        abort();
    }

    @Test
    @Override
    public void testMultipleRangesPredicate()
    {
        // wait this pr: https://github.com/databendlabs/databend-jdbc/pull/290
        // List columns explicitly. Some connectors do not maintain column ordering.
//        assertQuery("" +
//                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
//                "FROM orders " +
//                "WHERE orderkey BETWEEN 10 AND 50 OR orderkey BETWEEN 100 AND 150");
        abort();
    }

    @Test
    @Override
    public void testRangePredicate()
    {
        // wait this pr: https://github.com/databendlabs/databend-jdbc/pull/290
        // List columns explicitly. Some connectors do not maintain column ordering.
//        assertQuery("" +
//                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
//                "FROM orders " +
//                "WHERE orderkey BETWEEN 10 AND 50");
    }

    @Test
    @Override
    @Disabled
    public void testDynamicFiltering()
    {}

    @Test
    @Override
    public void testSelectAll()
    {
        abort();
    }

    private void testCreateTableWithColumnCommentSpecialCharacter(String comment)
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_", " (a bigint COMMENT " + varcharLiteral(comment) + ")")) {
            assertThat(getColumnComment(table.getName(), "a").replace("'", "")).isEqualTo(comment);
        }
    }

    @Test
    @Override
    public void testExecuteProcedure()
    {
        String tableName = "test_execute" + randomNameSuffix();
        String schemaTableName = "default" + "." + tableName;

        assertUpdate("CREATE TABLE " + schemaTableName + "(a int)");

        try {
            assertUpdate("INSERT INTO " + schemaTableName + " VALUES (1)", 1);
            assertQuery("SELECT * FROM " + schemaTableName, "VALUES 1");

            assertUpdate("UPDATE " + schemaTableName + " SET a = 2 WHERE true", 1);
            assertQuery("SELECT * FROM " + schemaTableName, "VALUES 2");

            assertUpdate("DELETE FROM " + schemaTableName + " WHERE true", 0);
            assertQueryReturnsEmptyResult("SELECT * FROM " + schemaTableName);

            assertUpdate("CALL system.execute('DROP TABLE " + schemaTableName + "')");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + schemaTableName);
        }
    }

    @Test
    @Override
    public void testExecuteProcedureWithInvalidQuery()
    {
        assertQuery("SELECT 1");
        assertQueryFails("invalid", ".*mismatched input 'invalid'.*");
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
    protected TestTable createTableWithDoubleAndRealColumns(String name, List<String> rows)
    {
        return new TestTable(onRemoteDatabase(), name, "(t_double Nullable(Float64), u_double Nullable(Float64), v_real Nullable(Float32), w_real Nullable(Float32)) Engine=FUSE", rows);
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
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar", "", "")
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
                        "   orderstatus varchar,\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar,\n" +
                        "   clerk varchar,\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   engine = 'FUSE'\n" +
                        ")");
    }

    @Test
    @Override
    public void testDeleteWithLike()
    {
        assertThatThrownBy(super::testDeleteWithLike)
                .hasStackTraceContaining("TrinoException: " + "");
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

    @Test
    @Override
    public void testDateYearOfEraPredicate()
    {
        // wait this pr fix: https://github.com/databendlabs/databend-jdbc/pull/290
//        assertQuery("SELECT orderdate FROM orders WHERE orderdate = DATE '1997-09-14'", "VALUES DATE '1997-09-14'");
        abort();
    }

    @Test
    @Override
    public void testSelectInTransaction()
    {
        inTransaction(session -> {
            assertQuery(session, "SELECT 'nationkey', 'name', 'regionkey' FROM nation");
            assertQuery(session, "SELECT 'regionkey', 'name' FROM region");
            assertQuery(session, "SELECT 'nationkey', 'name', 'regionkey' FROM nation");
        });
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
        onRemoteDatabase().execute("CREATE TABLE tpch.test_column_comment (col1 bigint COMMENT 'test comment', col2 bigint COMMENT '', col3 bigint)");

        assertQuery(
                "SELECT column_name, column_comment FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = 'test_column_comment'",
                "VALUES ('col1', '''test comment'''), ('col2', null), ('col3', null)");

        assertUpdate("DROP TABLE test_column_comment");
    }

    @Test
    @Override
    public void testAlterTableRenameColumnToLongName()
    {
        String tableName = "test_long_column" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x", 1);

        String baseColumnName = "col";
        int maxLength = maxColumnNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validTargetColumnName = baseColumnName + "z".repeat(maxLength - baseColumnName.length());
        assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN x TO " + validTargetColumnName);
        assertQuery("SELECT " + validTargetColumnName + " FROM " + tableName, "VALUES 123");
        assertUpdate("DROP TABLE " + tableName);

        if (maxColumnNameLength().isEmpty()) {
            return;
        }

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x", 1);
        assertQuery("SELECT x FROM " + tableName, "VALUES 123");
    }

    @Test
    @Override
    public void testCharTrailingSpace()
    {
        assertThatThrownBy(super::testCharTrailingSpace)
                .hasMessageContaining("For query")
                .hasMessageContaining("Actual rows")
                .hasMessageContaining("Expected rows");
    }

    @Test
    @Override
    public void testCharVarcharComparison()
    {
        assertThatThrownBy(super::testCharVarcharComparison)
                .hasMessageContaining("For query")
                .hasMessageContaining("Actual rows")
                .hasMessageContaining("Expected rows");
    }

    @Test
    @Override
    public void testAddNotNullColumn()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_nn_col", "(a_varchar varchar)")) {
            String tableName = table.getName();

            assertUpdate("INSERT INTO " + tableName + " VALUES ('a')", 1);
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b_varchar varchar NOT NULL");
            assertThat(query("TABLE " + tableName))
                    .skippingTypesCheck()
                    .matches("VALUES ('a', null)");
        }
    }

    @Test
    public void testLikePredicatePushdownWithCollation()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_like_predicate_pushdown",
                "(id integer, a_varchar varchar(1))",
                List.of(
                        "1, 'A'",
                        "2, 'a'",
                        "3, 'B'",
                        "4, 'ą'",
                        "5, 'Ą'"))) {
            assertQuery(
                    "SELECT id FROM " + table.getName() + " WHERE a_varchar LIKE '%A%'",
                    "VALUES (1)");

            assertQuery(
                    "SELECT id FROM " + table.getName() + " WHERE a_varchar LIKE '%ą%'",
                    "VALUES (4)");
        }
    }

    private DataSetup databendCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new DatabendSqlExecutor(onRemoteDatabase()), tableNamePrefix);
    }

    @Test
    @Override
    public void testNativeQueryColumnAlias()
    {
        assertThat(query("SELECT name AS region_name FROM tpch.region WHERE regionkey = 0"))
                .matches("VALUES CAST('AFRICA' AS VARCHAR)");
    }

    @Test
    public void testPredicatePushdown()
    {
        // varchar like
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name LIKE '%ROM%'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar))")
                .isNotFullyPushedDown(FilterNode.class);

        // varchar equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar))")
                .isNotFullyPushedDown(FilterNode.class);

        // varchar range
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar))")
                .isNotFullyPushedDown(FilterNode.class);

        // varchar different case
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania'"))
                .returnsEmptyResult()
                .isNotFullyPushedDown(FilterNode.class);

        // bigint equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey = 19"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar))")
                .isFullyPushedDown();

        // bigint range, with decimal to bigint simplification
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey BETWEEN 18.5 AND 19.5"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar))")
                .isFullyPushedDown();

        onRemoteDatabase().execute("CREATE TABLE tpch.binary_test (x int, y varbinary(100))");
        onRemoteDatabase().execute("INSERT INTO tpch.binary_test VALUES (3, from_base64('AFCBhLrkidtNTZcA9Ru3hw=='))");

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
                .failure().hasMessageContaining("Query not supported: ResultSetMetaData not available for query: INSERT INTO non_existent_table VALUES (1)");
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
//                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_strategy", "EAGER")
                .build();
    }

    @Test
    public void verifyDatabendJdbcDriverNegativeDateHandling()
            throws Exception
    {
        LocalDate negativeDate = LocalDate.of(-1, 1, 1);
        if (!databendSupportsNegativeDates()) {
            System.out.println("Databend does not support negative dates. Skipping this test.");
            return;
        }
        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.verify_negative_date", "(dt DATE)")) {
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

    // check databend support for negative dates
    private boolean databendSupportsNegativeDates()
    {
        try (Connection connection = databendServer.createConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("SELECT DATE '-0001-01-01'");
            return true;
        }
        catch (SQLException e) {
            return false;
        }
    }
}
