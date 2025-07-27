package io.trino.plugin.teradata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.MoreCollectors;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.plugin.jdbc.JoinOperator;
import io.trino.spi.connector.JoinCondition;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.TestingNames;
import io.trino.testing.assertions.TrinoExceptionAssert;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.assertj.core.api.AssertProvider;
import org.assertj.core.api.Assertions;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.teradata.util.TeradataConstants.TERADATA_OBJECT_NAME_LIMIT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test class for Teradata JDBC Connector.
 * Sets up schema and tables before tests and cleans up afterwards.
 */
public class TeradataJdbcConnectorTest
        extends BaseJdbcConnectorTest
{
    private static final Logger log = Logger.get(TeradataJdbcConnectorTest.class);
    protected final TestTeradataDatabase database = new TestTeradataDatabase(DatabaseConfig.fromEnv());

    private static void verifyResultOrFailure(AssertProvider<QueryAssertions.QueryAssert> queryAssertProvider, Consumer<QueryAssertions.QueryAssert> verifyResults, Consumer<TrinoExceptionAssert> verifyFailure)
    {
        requireNonNull(verifyResults, "verifyResults is null");
        requireNonNull(verifyFailure, "verifyFailure is null");
        QueryAssertions.QueryAssert queryAssert = Assertions.assertThat(queryAssertProvider);

        try {
            QueryAssertions.ResultAssert var4 = queryAssert.result();
        }
        catch (Throwable var5) {
            verifyFailure.accept(queryAssert.failure());
            return;
        }

        verifyResults.accept(queryAssert);
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return database;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_VIEW,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_DELETE,
                 SUPPORTS_INSERT,
                 SUPPORTS_UPDATE,
                 SUPPORTS_ADD_COLUMN,
                 SUPPORTS_DROP_COLUMN,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_TRUNCATE,
                 SUPPORTS_MERGE,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_RENAME_SCHEMA,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_ROW_LEVEL_DELETE,
                 SUPPORTS_DROP_SCHEMA_CASCADE,
                 SUPPORTS_NATIVE_QUERY,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                 SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_DEREFERENCE_PUSHDOWN,
                 SUPPORTS_NEGATIVE_DATE -> false;
            case SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_PREDICATE_PUSHDOWN,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_JOIN_PUSHDOWN,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR,
                 SUPPORTS_PREDICATE_ARITHMETIC_EXPRESSION_PUSHDOWN,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN -> true;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        database.createTestDatabaseIfAbsent();
        return TeradataQueryRunner.builder().setInitialTables(REQUIRED_TPCH_TABLES).build();
    }

    @AfterAll
    public void cleanupTestDatabase()
    {
        database.dropTestDatabaseIfExists();
    }

    @Test
    public void testDistinctLimit()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT DISTINCT orderstatus, custkey FROM orders LIMIT 10)");
        assertQuery("SELECT DISTINCT custkey, orderstatus FROM orders WHERE custkey = 1268 LIMIT 2");
        assertQuery("SELECT DISTINCT x " +
                        "FROM (VALUES 1) t(x) JOIN (VALUES 10, 20) u(a) ON t.x < u.a " +
                        "LIMIT 100",
                "SELECT 1");
    }

    /* Overriding the method as Teradata avg calculations are slightly different than trino so Skipping the results check for avg
        Expecting actual: (111.660, 111728394.9938271616, 1.117283945E8, 111.6605) to contain exactly in any order: [(111.661, 111728394.9938271605, 1.117283945E8, 111.6605)] */
    @Test
    public void testNumericAggregationPushdown()
    {
        if (!this.hasBehavior(TestingConnectorBehavior.SUPPORTS_AGGREGATION_PUSHDOWN)) {
            Assertions.assertThat(this.query("SELECT min(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
            Assertions.assertThat(this.query("SELECT max(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
            Assertions.assertThat(this.query("SELECT sum(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
            Assertions.assertThat(this.query("SELECT avg(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
        }
        else {
            String schemaName = this.getSession().getSchema().orElseThrow();

            try (TestTable emptyTable = this.createAggregationTestTable(schemaName + ".test_num_agg_pd", ImmutableList.of())) {
                Assertions.assertThat(this.query("SELECT min(short_decimal), min(long_decimal), min(a_bigint), min(t_double) FROM " + emptyTable.getName())).isFullyPushedDown();
                Assertions.assertThat(this.query("SELECT max(short_decimal), max(long_decimal), max(a_bigint), max(t_double) FROM " + emptyTable.getName())).isFullyPushedDown();
                Assertions.assertThat(this.query("SELECT sum(short_decimal), sum(long_decimal), sum(a_bigint), sum(t_double) FROM " + emptyTable.getName())).isFullyPushedDown();
                Assertions.assertThat(this.query("SELECT avg(short_decimal), avg(long_decimal), avg(a_bigint), avg(t_double) FROM " + emptyTable.getName())).skipResultsCorrectnessCheckForPushdown().isFullyPushedDown();
            }

            try (TestTable testTable = this.createAggregationTestTable(schemaName + ".test_num_agg_pd", ImmutableList.of("100.000, 100000000.000000000, 100.000, 100000000", "123.321, 123456789.987654321, 123.321, 123456789"))) {
                Assertions.assertThat(this.query("SELECT min(short_decimal), min(long_decimal), min(a_bigint), min(t_double) FROM " + testTable.getName())).isFullyPushedDown();
                Assertions.assertThat(this.query("SELECT max(short_decimal), max(long_decimal), max(a_bigint), max(t_double) FROM " + testTable.getName())).isFullyPushedDown();
                Assertions.assertThat(this.query("SELECT sum(short_decimal), sum(long_decimal), sum(a_bigint), sum(t_double) FROM " + testTable.getName())).isFullyPushedDown();
                Assertions.assertThat(this.query("SELECT avg(short_decimal), avg(long_decimal), avg(a_bigint), avg(t_double) FROM " + testTable.getName())).skipResultsCorrectnessCheckForPushdown().isFullyPushedDown();
                Assertions.assertThat(this.query("SELECT min(short_decimal), min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110 AND long_decimal < 124")).isFullyPushedDown();
                Assertions.assertThat(this.query("SELECT min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110")).isFullyPushedDown();
                Assertions.assertThat(this.query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " GROUP BY short_decimal")).isFullyPushedDown();
                Assertions.assertThat(this.query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110 AND long_decimal < 124 GROUP BY short_decimal")).isFullyPushedDown();
                Assertions.assertThat(this.query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110 GROUP BY short_decimal")).isFullyPushedDown();
                Assertions.assertThat(this.query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE long_decimal < 124 GROUP BY short_decimal")).isFullyPushedDown();
            }
        }
    }

    // Overriding this test case as Teradata defines varchar with a length.
    @Test
    public void testVarcharCastToDateInPredicate()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));
        String schemaName = this.getSession().getSchema().orElseThrow();
        String tableName = schemaName + ".varchar_as_date_pred";
        try (TestTable table = newTrinoTable(
                tableName,
                "(a varchar(50))",
                List.of(
                        "'999-09-09'",
                        "'1005-09-09'",
                        "'2005-06-06'", "'2005-06-6'", "'2005-6-06'", "'2005-6-6'", "' 2005-06-06'", "'2005-06-06 '", "' +2005-06-06'", "'02005-06-06'",
                        "'2005-09-06'", "'2005-09-6'", "'2005-9-06'", "'2005-9-6'", "' 2005-09-06'", "'2005-09-06 '", "' +2005-09-06'", "'02005-09-06'",
                        "'2005-09-09'", "'2005-09-9'", "'2005-9-09'", "'2005-9-9'", "' 2005-09-09'", "'2005-09-09 '", "' +2005-09-09'", "'02005-09-09'",
                        "'2005-09-10'", "'2005-9-10'", "' 2005-09-10'", "'2005-09-10 '", "' +2005-09-10'", "'02005-09-10'",
                        "'2005-09-20'", "'2005-9-20'", "' 2005-09-20'", "'2005-09-20 '", "' +2005-09-20'", "'02005-09-20'",
                        "'9999-09-09'",
                        "'99999-09-09'"))) {
            for (String date : List.of("2005-09-06", "2005-09-09", "2005-09-10")) {
                for (String operator : List.of("=", "<=", "<", ">", ">=", "!=", "IS DISTINCT FROM", "IS NOT DISTINCT FROM")) {
                    assertThat(query("SELECT a FROM %s WHERE CAST(a AS date) %s DATE '%s'".formatted(table.getName(), operator, date)))
                            .hasCorrectResultsRegardlessOfPushdown();
                }
            }
        }
        try (TestTable table = newTrinoTable(tableName,
                "(a varchar(50))",
                List.of("'2005-06-bad-date'", "'2005-09-10'"))) {
            assertThat(query("SELECT a FROM %s WHERE CAST(a AS date) < DATE '2005-09-10'".formatted(table.getName())))
                    .failure().hasMessage("Value cannot be cast to date: 2005-06-bad-date");
            verifyResultOrFailure(
                    query("SELECT a FROM %s WHERE CAST(a AS date) = DATE '2005-09-10'".formatted(table.getName())),
                    queryAssert -> queryAssert
                            .skippingTypesCheck()
                            .matches("VALUES '2005-09-10'"),
                    failureAssert -> failureAssert
                            .hasMessage("Value cannot be cast to date: 2005-06-bad-date"));
            // This failure isn't guaranteed: a row may be filtered out on the connector side with a derived predicate on a varchar column.
            verifyResultOrFailure(
                    query("SELECT a FROM %s WHERE CAST(a AS date) != DATE '2005-9-1'".formatted(table.getName())),
                    queryAssert -> queryAssert
                            .skippingTypesCheck()
                            .matches("VALUES '2005-09-10'"),
                    failureAssert -> failureAssert
                            .hasMessage("Value cannot be cast to date: 2005-06-bad-date"));
            // This failure isn't guaranteed: a row may be filtered out on the connector side with a derived predicate on a varchar column.
            verifyResultOrFailure(
                    query("SELECT a FROM %s WHERE CAST(a AS date) > DATE '2022-08-10'".formatted(table.getName())),
                    queryAssert -> queryAssert
                            .skippingTypesCheck()
                            .returnsEmptyResult(),
                    failureAssert -> failureAssert
                            .hasMessage("Value cannot be cast to date: 2005-06-bad-date"));
        }
        try (TestTable table = newTrinoTable(
                tableName,
                "(a varchar(50))",
                List.of("'2005-09-10'"))) {
            // 2005-09-01, when written as 2005-09-1, is a prefix of an existing data point: 2005-09-10
            assertThat(query("SELECT a FROM %s WHERE CAST(a AS date) != DATE '2005-09-01'".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES '2005-09-10'");
        }
    }

    // Overriding this test case as Teradata raises different error message for division by zero.
    @Test
    public void testArithmeticPredicatePushdown()
    {
        if (!this.hasBehavior(TestingConnectorBehavior.SUPPORTS_PREDICATE_ARITHMETIC_EXPRESSION_PUSHDOWN)) {
            Assertions.assertThat(this.query("SELECT shippriority FROM orders WHERE shippriority % 4 = 0")).isNotFullyPushedDown(FilterNode.class);
        }
        else {
            Assertions.assertThat(this.query("SELECT shippriority FROM orders WHERE shippriority % 4 = 0")).isFullyPushedDown();
            Assertions.assertThat(this.query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey > 0 AND (nationkey - regionkey) % nationkey = 2")).isFullyPushedDown().matches("VALUES (BIGINT '3', CAST('CANADA' AS varchar(25)), BIGINT '1')");
            Assertions.assertThat(this.query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey > 0 AND (nationkey - regionkey) % -nationkey = 2")).isFullyPushedDown().matches("VALUES (BIGINT '3', CAST('CANADA' AS varchar(25)), BIGINT '1')");
            Assertions.assertThat(this.query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey > 0 AND (nationkey - regionkey) % 0 = 2")).failure().hasMessageContaining("Operation Error");
            Assertions.assertThat(this.query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey > 0 AND (nationkey - regionkey) % (regionkey - 1) = 2")).failure().hasMessageContaining("Operation Error");
        }
    }

    @Test
    public void testCreateTableAsSelect()
    {
        String tableName = "test_ctas" + randomNameSuffix();
        if (!hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA)) {
            assertQueryFails("CREATE TABLE IF NOT EXISTS " + tableName + " AS SELECT name, regionkey FROM nation", "This connector does not support creating tables with data");
            return;
        }
        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " AS SELECT name, regionkey FROM nation", "SELECT count(*) FROM nation");
        assertTableColumnNames(tableName, "name", "regionkey");
        assertThat(getTableComment(tableName)).isNull();
        assertUpdate("DROP TABLE " + tableName);

        // Some connectors support CREATE TABLE AS but not the ordinary CREATE TABLE. Let's test CTAS IF NOT EXISTS with a table that is guaranteed to exist.
        assertUpdate("CREATE TABLE IF NOT EXISTS nation AS SELECT nationkey, regionkey FROM nation", 0);
        assertTableColumnNames("nation", "nationkey", "name", "regionkey", "comment");

        assertCreateTableAsSelect(
                "SELECT nationkey, name, regionkey FROM nation",
                "SELECT count(*) FROM nation");

        assertCreateTableAsSelect(
                "SELECT mktsegment, sum(acctbal) x FROM customer GROUP BY mktsegment",
                "SELECT count(DISTINCT mktsegment) FROM customer");

        assertCreateTableAsSelect(
                "SELECT count(*) x FROM nation JOIN region ON nation.regionkey = region.regionkey",
                "SELECT 1");

        assertCreateTableAsSelect(
                "SELECT nationkey FROM nation ORDER BY nationkey LIMIT 10",
                "SELECT 10");

        // Tests for CREATE TABLE with UNION ALL: exercises PushTableWriteThroughUnion optimizer

        assertCreateTableAsSelect(
                "SELECT name, nationkey, regionkey FROM nation WHERE nationkey % 2 = 0 UNION ALL " +
                        "SELECT name, nationkey, regionkey FROM nation WHERE nationkey % 2 = 1",
                "SELECT name, nationkey, regionkey FROM nation",
                "SELECT count(*) FROM nation");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "true").build(),
                "SELECT CAST(nationkey AS BIGINT) nationkey, regionkey FROM nation UNION ALL " +
                        "SELECT 1234567890, 123",
                "SELECT nationkey, regionkey FROM nation UNION ALL " +
                        "SELECT 1234567890, 123",
                "SELECT count(*) + 1 FROM nation");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "false").build(),
                "SELECT CAST(nationkey AS BIGINT) nationkey, regionkey FROM nation UNION ALL " +
                        "SELECT 1234567890, 123",
                "SELECT nationkey, regionkey FROM nation UNION ALL " +
                        "SELECT 1234567890, 123",
                "SELECT count(*) + 1 FROM nation");

        tableName = "test_ctas" + randomNameSuffix();
        assertExplainAnalyze("EXPLAIN ANALYZE CREATE TABLE " + tableName + " AS SELECT name FROM nation");
        assertQuery("SELECT * from " + tableName, "SELECT name FROM nation");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        return switch (typeName) {
            case "boolean", "tinyint", "real", "timestamp(6)", "timestamp(6) with time zone", "char(3)", "varchar",
                 "U&'a \\000a newline'" -> Optional.empty();
            default -> Optional.of(dataMappingTestSetup);
        };
    }

    // Overriding this test case as Teradata does not support negative dates.
    @Test
    public void testDateYearOfEraPredicate()
    {
        this.assertQuery("SELECT orderdate FROM orders WHERE orderdate = DATE '1997-09-14'", "VALUES DATE '1997-09-14'");
    }

    // Override this test case as Teradata has different syntax for creating tables with AS SELECT statement.
    @Test
    public void verifySupportsRowLevelUpdateDeclaration()
    {
        if (!this.hasBehavior(TestingConnectorBehavior.SUPPORTS_ROW_LEVEL_UPDATE)) {
            skipTestUnless(this.hasBehavior(TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA));

            try (TestTable table = this.newTrinoTable("test_supports_update", "AS ( SELECT * FROM nation) WITH DATA")) {
                this.assertQueryFails("UPDATE " + table.getName() + " SET nationkey = nationkey * 100 WHERE regionkey = 2", "This connector does not support modifying table rows");
            }
        }
    }

    // Override this test case as Teradata has different syntax for creating tables with AS SELECT statement.
    // TODO Will handle this while Teradata connector supporting WRITE operations.
    @Test
    public void testJoinPushdown()
    {
        Session session = this.joinPushdownEnabled(this.getSession());
        if (!this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN)) {
            Assertions.assertThat(this.query(session, "SELECT r.name, n.name FROM nation n JOIN region r ON n.regionkey = r.regionkey")).joinIsNotFullyPushedDown();
        }
        else {
            try (TestTable nationLowercaseTable = this.newTrinoTable("nation_lowercase", "AS ( SELECT nationkey, lower(name) name, regionkey FROM trino.nation ) WITH DATA")) {
                for (JoinOperator joinOperator : JoinOperator.values()) {
                    if (joinOperator == JoinOperator.FULL_JOIN && !this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN)) {
                        Assertions.assertThat(this.query(session, "SELECT r.name, n.name FROM nation n FULL JOIN region r ON n.regionkey = r.regionkey")).joinIsNotFullyPushedDown();
                    }
                    else {
                        Session withoutDynamicFiltering = Session.builder(session).setSystemProperty("enable_dynamic_filtering", "false").build();
                        List<String> nonEqualities = Stream.concat(Stream.of(JoinCondition.Operator.values()).filter((operatorx) -> operatorx != JoinCondition.Operator.EQUAL && operatorx != JoinCondition.Operator.IDENTICAL).map(JoinCondition.Operator::getValue), Stream.of("IS DISTINCT FROM", "IS NOT DISTINCT FROM")).collect(toImmutableList());
                        Assertions.assertThat(this.query(session, String.format("SELECT r.name, n.name FROM nation n %s region r ON n.regionkey = r.regionkey", joinOperator))).skipResultsCorrectnessCheckForPushdown().isFullyPushedDown();
                        Assertions.assertThat(this.query(session, String.format("SELECT r.name, n.name FROM nation n %s region r ON n.nationkey = r.regionkey", joinOperator))).skipResultsCorrectnessCheckForPushdown().isFullyPushedDown();
                        Assertions.assertThat(this.query(session, String.format("SELECT r.name, n.name FROM nation n %s region r USING(regionkey)", joinOperator))).skipResultsCorrectnessCheckForPushdown().isFullyPushedDown();
                        this.assertJoinConditionallyPushedDown(session, String.format("SELECT n.name, n2.regionkey FROM nation n %s nation n2 ON n.name = n2.name", joinOperator), this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY)).skipResultsCorrectnessCheckForPushdown();
                        this.assertJoinConditionallyPushedDown(session, String.format("SELECT n.name, nl.regionkey FROM nation n %s %s nl ON n.name = nl.name", joinOperator, nationLowercaseTable.getName()), this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY)).skipResultsCorrectnessCheckForPushdown();
                        Assertions.assertThat(this.query(session, String.format("SELECT n.name, c.name FROM nation n %s customer c ON n.nationkey = c.nationkey and n.regionkey = c.custkey", joinOperator))).skipResultsCorrectnessCheckForPushdown().isFullyPushedDown();
                        for (String operator : nonEqualities) {
                            this.assertJoinConditionallyPushedDown(withoutDynamicFiltering, String.format("SELECT r.name, n.name FROM nation n %s region r ON n.regionkey %s r.regionkey", joinOperator, operator), this.expectJoinPushdown(operator) && this.expectJoinPushdownOnInequalityOperator(joinOperator)).skipResultsCorrectnessCheckForPushdown();
                            this.assertJoinConditionallyPushedDown(withoutDynamicFiltering, String.format("SELECT n.name, nl.name FROM nation n %s %s nl ON n.name %s nl.name", joinOperator, nationLowercaseTable.getName(), operator), this.expectVarcharJoinPushdown(operator) && this.expectJoinPushdownOnInequalityOperator(joinOperator)).skipResultsCorrectnessCheckForPushdown();
                            this.assertJoinConditionallyPushedDown(session, String.format("SELECT n.name, c.name FROM nation n %s customer c ON n.nationkey = c.nationkey AND n.regionkey %s c.custkey", joinOperator, operator), this.expectJoinPushdown(operator)).skipResultsCorrectnessCheckForPushdown();
                        }
                        for (String operator : nonEqualities) {
                            this.assertJoinConditionallyPushedDown(session, String.format("SELECT n.name, nl.name FROM nation n %s %s nl ON n.regionkey = nl.regionkey AND n.name %s nl.name", joinOperator, nationLowercaseTable.getName(), operator), this.expectVarcharJoinPushdown(operator)).skipResultsCorrectnessCheckForPushdown();
                        }
                        Assertions.assertThat(this.query(session, String.format("SELECT c.name, n.name FROM (SELECT * FROM customer WHERE acctbal > 8000) c %s nation n ON c.custkey = n.nationkey", joinOperator))).isFullyPushedDown().skipResultsCorrectnessCheckForPushdown();
                        this.assertJoinConditionallyPushedDown(session, String.format("SELECT c.name, n.name FROM (SELECT * FROM customer WHERE address = 'TcGe5gaZNgVePxU5kRrvXBfkasDTea') c %s nation n ON c.custkey = n.nationkey", joinOperator), this.hasBehavior(TestingConnectorBehavior.SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY)).skipResultsCorrectnessCheckForPushdown();
                        this.assertJoinConditionallyPushedDown(session, String.format("SELECT c.name, n.name FROM (SELECT * FROM customer WHERE address < 'TcGe5gaZNgVePxU5kRrvXBfkasDTea') c %s nation n ON c.custkey = n.nationkey", joinOperator), this.hasBehavior(TestingConnectorBehavior.SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY)).skipResultsCorrectnessCheckForPushdown();
                        this.assertJoinConditionallyPushedDown(session, String.format("SELECT * FROM (SELECT regionkey rk, count(nationkey) c FROM nation GROUP BY regionkey) n %s region r ON n.rk = r.regionkey", joinOperator), this.hasBehavior(TestingConnectorBehavior.SUPPORTS_AGGREGATION_PUSHDOWN)).skipResultsCorrectnessCheckForPushdown();
                        this.assertJoinConditionallyPushedDown(session, String.format("SELECT * FROM (SELECT nationkey FROM nation LIMIT 30) n %s region r ON n.nationkey = r.regionkey", joinOperator), this.hasBehavior(TestingConnectorBehavior.SUPPORTS_LIMIT_PUSHDOWN)).skipResultsCorrectnessCheckForPushdown();
                        this.assertJoinConditionallyPushedDown(session, String.format("SELECT * FROM (SELECT nationkey FROM nation ORDER BY regionkey LIMIT 5) n %s region r ON n.nationkey = r.regionkey", joinOperator), this.hasBehavior(TestingConnectorBehavior.SUPPORTS_TOPN_PUSHDOWN)).skipResultsCorrectnessCheckForPushdown();
                        Assertions.assertThat(this.query(session, "SELECT * FROM nation n, region r, customer c WHERE n.regionkey = r.regionkey AND r.regionkey = c.custkey")).isFullyPushedDown().skipResultsCorrectnessCheckForPushdown();
                    }
                }
            }
            catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void testCharVarcharComparison()
    {
        skipTestUnless(this.hasBehavior(TestingConnectorBehavior.SUPPORTS_CREATE_TABLE));

        try (TestTable table = newTrinoTable("test_char_varchar", "(k int, v char(3))", List.of("-1, CAST(NULL AS char(3))", "3, CAST('   ' AS char(3))", "6, CAST('x  ' AS char(3))"))) {
            this.assertQuery("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('  ' AS varchar(2))", "VALUES (3, '   ')");
            this.assertQuery("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('  ' AS varchar(4))", "VALUES (3, '   ')");
            this.assertQuery("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('x ' AS varchar(2))", "VALUES (6, 'x  ')");
        }
    }

    @Test
    public void testRenameSchema()
    {
        Assumptions.abort("Skipping as connector does not support RENAME SCHEMA");
    }

    @Test
    public void testColumnName()
    {
        Assumptions.abort("Skipping as connector does not support column level write operations");
    }

    @Test
    public void testCreateTableAsSelectWithUnicode()
    {
        Assumptions.abort("Skipping as connector does not support creating table with UNICODE characters");
    }

    @Test
    public void testUpdateNotNullColumn()
    {
        Assumptions.abort("Skipping as connector does not support insert operations");
    }

    @Test
    public void testWriteBatchSizeSessionProperty()
    {
        Assumptions.abort("Skipping as connector does not support insert operations");
    }

    @Test
    public void testInsertWithoutTemporaryTable()
    {
        Assumptions.abort("Skipping as connector does not support insert operations");
    }

    @Test
    public void testWriteTaskParallelismSessionProperty()
    {
        Assumptions.abort("Skipping as connector does not support insert operations");
    }

    @Test
    public void testInsertIntoNotNullColumn()
    {
        Assumptions.abort("Skipping as connector does not support insert operations");
    }

    @Test
    public void testDropSchemaCascade()
    {
        Assumptions.abort("Skipping as connector does not support dropping schemas with CASCADE option");
    }

    @Test
    public void testAddColumn()
    {
        Assumptions.abort("Skipping as connector does not support column level write operations");
    }

    @Test
    public void verifySupportsUpdateDeclaration()
    {
        Assumptions.abort("Skipping as connector does not support update operations");
    }

    @Test
    public void testDropNotNullConstraint()
    {
        Assumptions.abort("Skipping as connector does not support dropping a not null constraint");
    }

    @Test
    public void testExecuteProcedureWithInvalidQuery()
    {
        Assumptions.abort("Skipping as connector does not support execute procedure");
    }

    @Test
    public void testCreateTableAsSelectNegativeDate()
    {
        Assumptions.abort("Skipping as connector does not support creating table with negative date");
    }

    protected void assertCreateTableAsSelect(Session session, String query, String expectedQuery, String rowCountQuery)
    {
        String table = "test_ctas_" + TestingNames.randomNameSuffix();
        this.assertUpdate(session, "CREATE TABLE " + table + " AS ( " + query + ") WITH DATA", rowCountQuery);
        this.assertQuery(session, "SELECT * FROM " + table, expectedQuery);
        this.assertUpdate(session, "DROP TABLE " + table);
        Assertions.assertThat(this.getQueryRunner().tableExists(session, table)).isFalse();
    }

    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessage(format("Schema name must be shorter than or equal to '%s' characters but got '%s'", TERADATA_OBJECT_NAME_LIMIT, TERADATA_OBJECT_NAME_LIMIT + 1));
    }

    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(TERADATA_OBJECT_NAME_LIMIT);
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching(format("Column name must be shorter than or equal to '%s' characters but got '%s': '.*'", TERADATA_OBJECT_NAME_LIMIT, TERADATA_OBJECT_NAME_LIMIT + 1));
    }

    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(TERADATA_OBJECT_NAME_LIMIT);
    }

    @Override
    protected Session joinPushdownEnabled(Session session)
    {
        return Session.builder(super.joinPushdownEnabled(session))
                // strategy is AUTOMATIC by default and would not work for certain test cases (even if statistics are collected)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_strategy", "EAGER")
                .build();
    }

    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessage(format("Table name must be shorter than or equal to '%s' characters but got '%s'", TERADATA_OBJECT_NAME_LIMIT, TERADATA_OBJECT_NAME_LIMIT + 1));
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(TERADATA_OBJECT_NAME_LIMIT);
    }

    protected TestTable newTrinoTable(String namePrefix, @Language("SQL") String tableDefinition, List<String> rowsToInsert)
    {
        return new TestTable(database, namePrefix, tableDefinition, rowsToInsert);
    }

    private boolean expectVarcharJoinPushdown(String operator)
            throws Throwable
    {
        if ("IS DISTINCT FROM".equals(operator)) {
            return this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM) && this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY);
        }
        else {
            boolean var10000;
            switch (this.toJoinConditionOperator(operator)) {
                case EQUAL:
                case NOT_EQUAL:
                    var10000 = this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY);
                    break;
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    var10000 = this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_INEQUALITY);
                    break;
                case IDENTICAL:
                    var10000 = this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM) && this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY);
                    break;
                default:
                    throw new MatchException(null, null);
            }
            return var10000;
        }
    }

    private JoinCondition.Operator toJoinConditionOperator(String operator)
            throws Throwable
    {
        return operator.equals("IS NOT DISTINCT FROM") ? JoinCondition.Operator.IDENTICAL : (JoinCondition.Operator) ((Optional) Stream.of(JoinCondition.Operator.values()).filter((joinOperator) -> joinOperator.getValue().equals(operator)).collect(MoreCollectors.toOptional())).orElseThrow(() -> new IllegalArgumentException("Not found: " + operator));
    }
}
