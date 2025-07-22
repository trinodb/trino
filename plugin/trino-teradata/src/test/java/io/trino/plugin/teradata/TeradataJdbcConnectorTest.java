package io.trino.plugin.teradata;

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MoreCollectors;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.plugin.jdbc.JoinOperator;
import io.trino.plugin.jdbc.JoinPushdownStrategy;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.TableScanNode;
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
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_AGGREGATION_PUSHDOWN;
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
                 SUPPORTS_NATIVE_QUERY,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_INEQUALITY -> false;
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

    @Test
    public void testAggregationPushdown()
    {

        if (!hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN)) {
            assertThat(query("SELECT count(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
            return;
        }

        this.assertConditionallyPushedDown(this.getSession(), "SELECT regionkey, sum(nationkey) FROM nation  GROUP BY regionkey", this.hasBehavior(TestingConnectorBehavior.SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN_WITH_LIKE), PlanMatchPattern.node(FilterNode.class, PlanMatchPattern.node(TableScanNode.class)));
    }

    @Override
    protected Session joinPushdownEnabled(Session session)
    {
        return Session.builder(super.joinPushdownEnabled(session))
                // strategy is AUTOMATIC by default and would not work for certain test cases (even if statistics are collected)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_strategy", "EAGER")
                .build();
    }



}

