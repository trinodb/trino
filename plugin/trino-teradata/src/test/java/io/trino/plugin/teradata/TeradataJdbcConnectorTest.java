package io.trino.plugin.teradata;

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MoreCollectors;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.plugin.jdbc.JoinOperator;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.*;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.plugin.teradata.util.TeradataConstants.TERADATA_OBJECT_NAME_LIMIT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test class for Teradata JDBC Connector.
 * Sets up schema and tables before tests and cleans up afterwards.
 */
public class TeradataJdbcConnectorTest
        extends BaseJdbcConnectorTest
{
    protected final TestTeradataDatabase database = new TestTeradataDatabase(DatabaseConfig.fromEnv());

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
                 SUPPORTS_CREATE_TABLE_WITH_DATA,
                 SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_RENAME_SCHEMA,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_ROW_LEVEL_DELETE,
                 SUPPORTS_DROP_SCHEMA_CASCADE,
                 SUPPORTS_NATIVE_QUERY -> false;
            case SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_PREDICATE_PUSHDOWN,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_JOIN_PUSHDOWN,
                 SUPPORTS_LIMIT_PUSHDOWN -> true;
//                 SUPPORTS_DROP_SCHEMA_CASCADE -> true;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return database;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        database.createTestDatabaseIfAbsent();
        return TeradataQueryRunner.builder().addCoordinatorProperty("http-server.http.port", "8080").setInitialTables(REQUIRED_TPCH_TABLES).build();
    }

    @AfterAll
    public void cleanupTestDatabase()
    {
        //database.dropTestDatabaseIfExists();
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(TERADATA_OBJECT_NAME_LIMIT);
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

    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessage(format("Table name must be shorter than or equal to '%s' characters but got '%s'", TERADATA_OBJECT_NAME_LIMIT, TERADATA_OBJECT_NAME_LIMIT + 1));
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
    public void testAddColumn()
    {
        Assumptions.abort("Skipping as connector does not support column level write operations");
    }

    @Test
    public void testPredicate()
    {
        this.assertQuery("""
                    SELECT  *
                      FROM (
                        SELECT orderkey+1 AS a FROM orders WHERE orderstatus = 'F'
                        UNION ALL
                        SELECT orderkey AS a FROM orders WHERE MOD(orderkey, 2) = 0
                        UNION ALL
                        SELECT orderkey+custkey AS a FROM orders
                      ) AS unioned
                      WHERE a < 20 OR a > 100
                      ORDER BY a;
                """);
    }

    @Test
    public void testTeradataLimitPushdown()
    {
        assertThat(query("SELECT name FROM nation LIMIT 5")).skipResultsCorrectnessCheckForPushdown().isFullyPushedDown();
        assertThat(query("SELECT name FROM nation ORDER BY name LIMIT 5")).skipResultsCorrectnessCheckForPushdown().isFullyPushedDown();
        assertThat(query("SELECT name FROM nation WHERE regionkey = 3 LIMIT 5")).skipResultsCorrectnessCheckForPushdown().isFullyPushedDown();
        assertThat(query("SELECT name FROM nation WHERE name < 'EEE' LIMIT 5")).skipResultsCorrectnessCheckForPushdown().isFullyPushedDown();
    }

    @Test
    public void testNullSensitiveTopNPushdown()
    {
        if (this.hasBehavior(TestingConnectorBehavior.SUPPORTS_TOPN_PUSHDOWN)) {
            if (!database.isTableExists("test_null_sensitive_topn_pushdown")) {
                String sql = "CREATE TABLE trino.test_null_sensitive_topn_pushdown(name varchar(10), a bigint)";
                database.execute(sql);
            }
            List<String> rowsToInsert = List.of("'small', 42", "'big', 134134", "'negative', -15", "'null', NULL");
            for (String row : rowsToInsert) {
                database.execute(format("INSERT INTO %s VALUES (%s)", "trino.test_null_sensitive_topn_pushdown", row));
            }
            Verify.verify(SortOrder.values().length == 4, "The test needs to be updated when new options are added");
            Assertions.assertThat(this.query("SELECT name FROM trino.test_null_sensitive_topn_pushdown ORDER BY a ASC NULLS FIRST LIMIT 5")).ordered().isFullyPushedDown();
            Assertions.assertThat(this.query("SELECT name FROM trino.test_null_sensitive_topn_pushdown ORDER BY a ASC NULLS LAST LIMIT 5")).ordered().isFullyPushedDown();
            Assertions.assertThat(this.query("SELECT name FROM trino.test_null_sensitive_topn_pushdown ORDER BY a DESC NULLS FIRST LIMIT 5")).ordered().isFullyPushedDown();
            Assertions.assertThat(this.query("SELECT name FROM trino.test_null_sensitive_topn_pushdown ORDER BY a DESC NULLS LAST LIMIT 5")).ordered().isFullyPushedDown();
        }
    }

    @Test
    public void testCaseSensitiveTopNPushdown() {

        if (this.hasBehavior(TestingConnectorBehavior.SUPPORTS_TOPN_PUSHDOWN)) {
            boolean expectTopNPushdown = this.hasBehavior(TestingConnectorBehavior.SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR);
            String tblName = "trino.test_case_sensitive_topn_pushdown";
            if (!database.isTableExists("test_case_sensitive_topn_pushdown")) {
                String sql = "CREATE TABLE " + tblName + "(name varchar(10), a bigint)";
                database.execute(sql);
            }
            List<String> rowsToInsert = List.of("'small', 42", "'big', 134134", "'negative', -15", "'null', NULL");
            for (String row : rowsToInsert) {
                database.execute(format("INSERT INTO %s VALUES (%s)", tblName, row));
            }
            PlanMatchPattern topNOverTableScan = PlanMatchPattern.project(PlanMatchPattern.node(TopNNode.class, new PlanMatchPattern[]{PlanMatchPattern.anyTree(new PlanMatchPattern[]{PlanMatchPattern.node(TableScanNode.class, new PlanMatchPattern[0])})}));


            this.assertConditionallyOrderedPushedDown(this.getSession(), "SELECT a_bigint FROM " + tblName + " ORDER BY a_string ASC LIMIT 2", expectTopNPushdown, topNOverTableScan);
            this.assertConditionallyOrderedPushedDown(this.getSession(), "SELECT a_bigint FROM " + tblName + " ORDER BY a_string DESC LIMIT 2", expectTopNPushdown, topNOverTableScan);
            this.assertConditionallyOrderedPushedDown(this.getSession(), "SELECT a_bigint FROM " + tblName + " ORDER BY a_char ASC LIMIT 2", expectTopNPushdown, topNOverTableScan);
            this.assertConditionallyOrderedPushedDown(this.getSession(), "SELECT a_bigint FROM " + tblName + " ORDER BY a_char DESC LIMIT 2", expectTopNPushdown, topNOverTableScan);
            this.assertConditionallyOrderedPushedDown(this.getSession(), "SELECT a_bigint FROM " + tblName + " ORDER BY a_bigint, a_char LIMIT 2", expectTopNPushdown, topNOverTableScan);
            this.assertConditionallyOrderedPushedDown(this.getSession(), "SELECT a_bigint FROM " + tblName + " ORDER BY a_bigint, a_string DESC LIMIT 2", expectTopNPushdown, topNOverTableScan);
        }
    }

    @Test
    public void testJoinPushdown() {
        Session session = this.joinPushdownEnabled(this.getSession());
        if (!this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN)) {
            ((QueryAssertions.QueryAssert)Assertions.assertThat(this.query(session, "SELECT r.name, n.name FROM nation n JOIN region r ON n.regionkey = r.regionkey"))).joinIsNotFullyPushedDown();
        } else {
            try (TestTable nationLowercaseTable = this.newTrinoTable("nation_lowercase", "AS SELECT nationkey, lower(name) name, regionkey FROM nation")) {
                for(JoinOperator joinOperator : JoinOperator.values()) {
                    System.out.println(format("Testing joinOperator=%s", new Object[]{joinOperator}));
                    if (joinOperator == JoinOperator.FULL_JOIN && !this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN)) {
                        ((QueryAssertions.QueryAssert)Assertions.assertThat(this.query(session, "SELECT r.name, n.name FROM nation n FULL JOIN region r ON n.regionkey = r.regionkey"))).joinIsNotFullyPushedDown();
                    } else {
                        Session withoutDynamicFiltering = Session.builder(session).setSystemProperty("enable_dynamic_filtering", "false").build();
                        List<String> nonEqualities = (List) Stream.concat(Stream.of(JoinCondition.Operator.values()).filter((operatorx) -> operatorx != JoinCondition.Operator.EQUAL && operatorx != JoinCondition.Operator.IDENTICAL).map(JoinCondition.Operator::getValue), Stream.of("IS DISTINCT FROM", "IS NOT DISTINCT FROM")).collect(ImmutableList.toImmutableList());
                        ((QueryAssertions.QueryAssert)Assertions.assertThat(this.query(session, String.format("SELECT r.name, n.name FROM nation n %s region r ON n.regionkey = r.regionkey", joinOperator)))).isFullyPushedDown();
                        ((QueryAssertions.QueryAssert)Assertions.assertThat(this.query(session, String.format("SELECT r.name, n.name FROM nation n %s region r ON n.nationkey = r.regionkey", joinOperator)))).isFullyPushedDown();
                        ((QueryAssertions.QueryAssert)Assertions.assertThat(this.query(session, String.format("SELECT r.name, n.name FROM nation n %s region r USING(regionkey)", joinOperator)))).isFullyPushedDown();
                        this.assertJoinConditionallyPushedDown(session, String.format("SELECT n.name, n2.regionkey FROM nation n %s nation n2 ON n.name = n2.name", joinOperator), this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY));
                        this.assertJoinConditionallyPushedDown(session, String.format("SELECT n.name, nl.regionkey FROM nation n %s %s nl ON n.name = nl.name", joinOperator, nationLowercaseTable.getName()), this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY));
                        ((QueryAssertions.QueryAssert)Assertions.assertThat(this.query(session, String.format("SELECT n.name, c.name FROM nation n %s customer c ON n.nationkey = c.nationkey and n.regionkey = c.custkey", joinOperator)))).isFullyPushedDown();

                        for(String operator : nonEqualities) {
                            this.assertJoinConditionallyPushedDown(withoutDynamicFiltering, String.format("SELECT r.name, n.name FROM nation n %s region r ON n.regionkey %s r.regionkey", joinOperator, operator), this.expectJoinPushdown(operator) && this.expectJoinPushdownOnInequalityOperator(joinOperator));
                            try {
                                this.assertJoinConditionallyPushedDown(withoutDynamicFiltering, String.format("SELECT n.name, nl.name FROM nation n %s %s nl ON n.name %s nl.name", joinOperator, nationLowercaseTable.getName(), operator), this.expectVarcharJoinPushdown(operator) && this.expectJoinPushdownOnInequalityOperator(joinOperator));
                            } catch (Throwable e) {
                                throw new RuntimeException(e);
                            }
                        }

                        for(String operator : nonEqualities) {
                            System.out.println(format("Testing [joinOperator=%s] operator=%s on number", new Object[]{joinOperator, operator}));
                            this.assertJoinConditionallyPushedDown(session, String.format("SELECT n.name, c.name FROM nation n %s customer c ON n.nationkey = c.nationkey AND n.regionkey %s c.custkey", joinOperator, operator), this.expectJoinPushdown(operator));
                        }

                        for(String operator : nonEqualities) {
                            System.out.println(format("Testing [joinOperator=%s] operator=%s on varchar", new Object[]{joinOperator, operator}));
                            try {
                                this.assertJoinConditionallyPushedDown(session, String.format("SELECT n.name, nl.name FROM nation n %s %s nl ON n.regionkey = nl.regionkey AND n.name %s nl.name", joinOperator, nationLowercaseTable.getName(), operator), this.expectVarcharJoinPushdown(operator));
                            } catch (Throwable e) {
                                throw new RuntimeException(e);
                            }
                        }

                        ((QueryAssertions.QueryAssert)Assertions.assertThat(this.query(session, String.format("SELECT c.name, n.name FROM (SELECT * FROM customer WHERE acctbal > 8000) c %s nation n ON c.custkey = n.nationkey", joinOperator)))).isFullyPushedDown();
                        this.assertJoinConditionallyPushedDown(session, String.format("SELECT c.name, n.name FROM (SELECT * FROM customer WHERE address = 'TcGe5gaZNgVePxU5kRrvXBfkasDTea') c %s nation n ON c.custkey = n.nationkey", joinOperator), this.hasBehavior(TestingConnectorBehavior.SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY));
                        this.assertJoinConditionallyPushedDown(session, String.format("SELECT c.name, n.name FROM (SELECT * FROM customer WHERE address < 'TcGe5gaZNgVePxU5kRrvXBfkasDTea') c %s nation n ON c.custkey = n.nationkey", joinOperator), this.hasBehavior(TestingConnectorBehavior.SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY));
                        this.assertJoinConditionallyPushedDown(session, String.format("SELECT * FROM (SELECT regionkey rk, count(nationkey) c FROM nation GROUP BY regionkey) n %s region r ON n.rk = r.regionkey", joinOperator), this.hasBehavior(TestingConnectorBehavior.SUPPORTS_AGGREGATION_PUSHDOWN));
                        this.assertJoinConditionallyPushedDown(session, String.format("SELECT * FROM (SELECT nationkey FROM nation LIMIT 30) n %s region r ON n.nationkey = r.regionkey", joinOperator), this.hasBehavior(TestingConnectorBehavior.SUPPORTS_LIMIT_PUSHDOWN));
                        this.assertJoinConditionallyPushedDown(session, String.format("SELECT * FROM (SELECT nationkey FROM nation ORDER BY regionkey LIMIT 5) n %s region r ON n.nationkey = r.regionkey", joinOperator), this.hasBehavior(TestingConnectorBehavior.SUPPORTS_TOPN_PUSHDOWN));
                        ((QueryAssertions.QueryAssert)Assertions.assertThat(this.query(session, "SELECT * FROM nation n, region r, customer c WHERE n.regionkey = r.regionkey AND r.regionkey = c.custkey"))).isFullyPushedDown();
                    }
                }
            }

        }
    }

    private JoinCondition.Operator toJoinConditionOperator(String operator) throws Throwable {
        return operator.equals("IS NOT DISTINCT FROM") ? JoinCondition.Operator.IDENTICAL : (JoinCondition.Operator)((Optional)Stream.of(JoinCondition.Operator.values()).filter((joinOperator) -> joinOperator.getValue().equals(operator)).collect(MoreCollectors.toOptional())).orElseThrow(() -> new IllegalArgumentException("Not found: " + operator));
    }

    private boolean expectVarcharJoinPushdown(String operator) throws Throwable {
        if ("IS DISTINCT FROM".equals(operator)) {
            return this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM) && this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY);
        } else {
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
                    throw new MatchException((String)null, (Throwable)null);
            }

            return var10000;
        }
    }

    @Test
    public void testCaseSensitiveAggregationPushdown() {
        String tblName = "trino.test_cs_agg_pushdown";
        if (this.hasBehavior(TestingConnectorBehavior.SUPPORTS_AGGREGATION_PUSHDOWN)) {
            if (!database.isTableExists("test_cs_agg_pushdown")) {
                String sql = "CREATE TABLE " + tblName + "(name varchar(10), a bigint)";
                database.execute(sql);
            }
            List<String> rowsToInsert = ImmutableList.of("'A', 'A', 1", "'B', 'B', 1", "'a', 'a', 3", "'b', 'b', 4");
            for (String row : rowsToInsert) {
                database.execute(format("INSERT INTO %s VALUES (%s)", tblName, row));
            }
            boolean supportsPushdownWithVarcharInequality = this.hasBehavior(TestingConnectorBehavior.SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY);
            boolean supportsCountDistinctPushdown = this.hasBehavior(TestingConnectorBehavior.SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT);
            boolean supportsSumDistinctPushdown = this.hasBehavior(TestingConnectorBehavior.SUPPORTS_AGGREGATION_PUSHDOWN);
            PlanMatchPattern aggregationOverTableScan = PlanMatchPattern.node(AggregationNode.class, new PlanMatchPattern[]{PlanMatchPattern.node(TableScanNode.class, new PlanMatchPattern[0])});
            PlanMatchPattern groupingAggregationOverTableScan = PlanMatchPattern.node(AggregationNode.class, new PlanMatchPattern[]{PlanMatchPattern.node(TableScanNode.class, new PlanMatchPattern[0])});
            this.assertConditionallyPushedDown(this.getSession(), "SELECT max(a_string), min(a_string), max(a_char), min(a_char) FROM " + tblName, supportsPushdownWithVarcharInequality, aggregationOverTableScan).skippingTypesCheck().matches("VALUES ('b', 'A', 'b', 'A')");
            this.assertConditionallyPushedDown(this.getSession(), "SELECT distinct a_string FROM " + tblName, supportsPushdownWithVarcharInequality, groupingAggregationOverTableScan).skippingTypesCheck().matches("VALUES 'A', 'B', 'a', 'b'");
            this.assertConditionallyPushedDown(this.getSession(), "SELECT distinct a_char FROM " + tblName, supportsPushdownWithVarcharInequality, groupingAggregationOverTableScan).skippingTypesCheck().matches("VALUES 'A', 'B', 'a', 'b'");
            this.assertConditionallyPushedDown(this.getSession(), "SELECT a_string, count(*) FROM " + tblName + " GROUP BY a_string", supportsPushdownWithVarcharInequality, groupingAggregationOverTableScan).skippingTypesCheck().matches("VALUES ('A', BIGINT '1'), ('a', BIGINT '1'), ('b', BIGINT '1'), ('B', BIGINT '1')");
            this.assertConditionallyPushedDown(this.getSession(), "SELECT a_char, count(*) FROM " + tblName + " GROUP BY a_char", supportsPushdownWithVarcharInequality, groupingAggregationOverTableScan).skippingTypesCheck().matches("VALUES ('A', BIGINT '1'), ('B', BIGINT '1'), ('a', BIGINT '1'), ('b', BIGINT '1')");
            ((QueryAssertions.QueryAssert)Assertions.assertThat(this.query("SELECT count(a_string), count(a_char) FROM " + tblName))).isFullyPushedDown();
            ((QueryAssertions.QueryAssert)Assertions.assertThat(this.query("SELECT count(a_string), count(a_char) FROM " + tblName + " GROUP BY a_bigint"))).isFullyPushedDown();
            this.assertConditionallyPushedDown(this.getSession(), "SELECT count(DISTINCT a_string) FROM " + tblName, supportsPushdownWithVarcharInequality, groupingAggregationOverTableScan).skippingTypesCheck().matches("VALUES BIGINT '4'");
            this.assertConditionallyPushedDown(this.getSession(), "SELECT count(DISTINCT a_char) FROM " + tblName, supportsPushdownWithVarcharInequality, groupingAggregationOverTableScan).skippingTypesCheck().matches("VALUES BIGINT '4'");
            Session withMarkDistinct = Session.builder(this.getSession()).setSystemProperty("distinct_aggregations_strategy", "mark_distinct").build();
            Session withSingleStep = Session.builder(this.getSession()).setSystemProperty("distinct_aggregations_strategy", "single_step").build();
            Session withPreAggregate = Session.builder(this.getSession()).setSystemProperty("distinct_aggregations_strategy", "pre_aggregate").build();
            this.verifyMultipleDistinctPushdown(withMarkDistinct, PlanMatchPattern.node(ExchangeNode.class, new PlanMatchPattern[]{PlanMatchPattern.node(AggregationNode.class, new PlanMatchPattern[]{PlanMatchPattern.anyTree(new PlanMatchPattern[]{PlanMatchPattern.node(TableScanNode.class, new PlanMatchPattern[0])})})}), supportsPushdownWithVarcharInequality, supportsCountDistinctPushdown, supportsSumDistinctPushdown, tblName);
            this.verifyMultipleDistinctPushdown(withSingleStep, PlanMatchPattern.node(AggregationNode.class, new PlanMatchPattern[]{PlanMatchPattern.anyTree(new PlanMatchPattern[]{PlanMatchPattern.node(TableScanNode.class, new PlanMatchPattern[0])})}), supportsPushdownWithVarcharInequality, supportsCountDistinctPushdown, supportsSumDistinctPushdown, tblName);
            this.verifyMultipleDistinctPushdown(withPreAggregate, PlanMatchPattern.node(AggregationNode.class, new PlanMatchPattern[]{PlanMatchPattern.project(PlanMatchPattern.node(AggregationNode.class, new PlanMatchPattern[]{PlanMatchPattern.anyTree(new PlanMatchPattern[]{PlanMatchPattern.node(GroupIdNode.class, new PlanMatchPattern[]{PlanMatchPattern.node(TableScanNode.class, new PlanMatchPattern[0])})})}))}), supportsPushdownWithVarcharInequality, supportsCountDistinctPushdown, supportsSumDistinctPushdown, tblName);
          }
    }

    private void verifyMultipleDistinctPushdown(Session session, PlanMatchPattern otherwiseExpected, boolean supportsPushdownWithVarcharInequality, boolean supportsCountDistinctPushdown, boolean supportsSumDistinctPushdown, String tableName) {
        this.assertConditionallyPushedDown(session, "SELECT count(DISTINCT a_string), count(DISTINCT a_bigint) FROM " + tableName, supportsPushdownWithVarcharInequality && supportsCountDistinctPushdown, otherwiseExpected).skippingTypesCheck().matches("VALUES (BIGINT '4', BIGINT '3')");
        this.assertConditionallyPushedDown(session, "SELECT count(DISTINCT a_char), count(DISTINCT a_bigint) FROM " + tableName, supportsPushdownWithVarcharInequality && supportsCountDistinctPushdown, otherwiseExpected).skippingTypesCheck().matches("VALUES (BIGINT '4', BIGINT '3')");
        this.assertConditionallyPushedDown(session, "SELECT count(DISTINCT a_string), sum(DISTINCT a_bigint) FROM " + tableName, supportsPushdownWithVarcharInequality && supportsSumDistinctPushdown, otherwiseExpected).skippingTypesCheck().matches(this.sumDistinctAggregationPushdownExpectedResult());
        this.assertConditionallyPushedDown(session, "SELECT count(DISTINCT a_char), sum(DISTINCT a_bigint) FROM " + tableName, supportsPushdownWithVarcharInequality && supportsSumDistinctPushdown, otherwiseExpected).skippingTypesCheck().matches(this.sumDistinctAggregationPushdownExpectedResult());
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        switch (typeName) {
            case "boolean":
            case "tinyint":
            case "real":
            case "timestamp(6)":
            case "timestamp(6) with time zone":
            case "char(3)":
            case "varchar":
            case "U&'a \\000a newline'":
                return Optional.empty();
            default:
                return Optional.of(dataMappingTestSetup);
        }
    }


    @Test
    public void testUpdateNotNullColumn() {
        Assumptions.abort("Skipping as connector does not support insert operations");
    }
    @Test
    public void testWriteBatchSizeSessionProperty() {
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
    public void testDropNotNullConstraint()
    {
        Assumptions.abort("Skipping as connector does not support dropping a not null constraint");
    }
}

