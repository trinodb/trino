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
package io.trino.sql.query;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.trino.Session;
import io.trino.client.Warning;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.execution.QueryInfo;
import io.trino.execution.TableInfo;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.eventlistener.ColumnInfo;
import io.trino.spi.eventlistener.RoutineInfo;
import io.trino.spi.expression.Constant;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.Identity;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.VarcharType;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.StandaloneQueryRunner;
import io.trino.testing.TestingAccessControlManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.SystemSessionProperties.COLLECT_PLAN_STATISTICS_FOR_ALL_QUERIES;
import static io.trino.SystemSessionProperties.SECURE_EXPRESSION_REDACTION_ENABLED;
import static io.trino.connector.MockConnectorEntities.TPCH_NATION_DATA;
import static io.trino.connector.MockConnectorEntities.TPCH_NATION_SCHEMA;
import static io.trino.connector.MockConnectorEntities.TPCH_NATION_WITH_HIDDEN_COLUMN;
import static io.trino.connector.MockConnectorEntities.TPCH_NATION_WITH_OPTIONAL_COLUMN;
import static io.trino.connector.MockConnectorEntities.TPCH_WITH_HIDDEN_COLUMN_DATA;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_SCALAR;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.SecureExpression.REDACTED;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestRowFilter
{
    private static final String POLICY_TOKEN = "row-policy-secret-token";
    private static final String LOCAL_CATALOG = "local";
    private static final String MOCK_CATALOG = "mock";
    private static final String MOCK_CATALOG_MISSING_COLUMNS = "mockmissingcolumns";
    private static final String MOCK_CATALOG_PUSHDOWN = "mockpushdown";
    private static final String MOCK_CATALOG_CONSUMING_PREDICATES = "mockconsumingpredicates";
    private static final long POLICY_KEY = 887766L;
    private static final String USER = "user";
    private static final String VIEW_OWNER = "view-owner";
    private static final String RUN_AS_USER = "run-as-user";

    private static final Session SESSION = testSessionBuilder()
            .setCatalog(LOCAL_CATALOG)
            .setSchema(TINY_SCHEMA_NAME)
            .setIdentity(Identity.forUser(USER).build())
            .setSystemProperty(SECURE_EXPRESSION_REDACTION_ENABLED, "true")
            .build();

    private final QueryAssertions assertions;
    private final TestingAccessControlManager accessControl;
    private final AtomicInteger pushedExpressions = new AtomicInteger();
    private final AtomicReference<TupleDomain<ColumnHandle>> pushedPredicate = new AtomicReference<>(TupleDomain.all());

    public TestRowFilter()
    {
        QueryRunner runner = new StandaloneQueryRunner(SESSION);
        runner.installPlugin(new TpchPlugin());
        runner.createCatalog(LOCAL_CATALOG, "tpch", ImmutableMap.of("tpch.splits-per-node", "1"));

        ConnectorViewDefinition view = new ConnectorViewDefinition(
                "SELECT nationkey, name FROM local.tiny.nation",
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(
                        new ConnectorViewDefinition.ViewColumn("nationkey", BigintType.BIGINT.getTypeId(), Optional.empty()),
                        new ConnectorViewDefinition.ViewColumn("name", VarcharType.createVarcharType(25).getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.of(VIEW_OWNER),
                false,
                ImmutableList.of());

        runner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withGetViews((_, _) -> ImmutableMap.of(new SchemaTableName("default", "nation_view"), view))
                .withGetColumns(schemaTableName -> {
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation"))) {
                        return TPCH_NATION_SCHEMA;
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_with_hidden_column"))) {
                        return TPCH_NATION_WITH_HIDDEN_COLUMN;
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_with_optional_column"))) {
                        return TPCH_NATION_WITH_OPTIONAL_COLUMN;
                    }
                    throw new UnsupportedOperationException();
                })
                .withBranches(ImmutableList.of("dev"))
                .withData(schemaTableName -> {
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation"))) {
                        return TPCH_NATION_DATA;
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_with_hidden_column"))) {
                        return TPCH_WITH_HIDDEN_COLUMN_DATA;
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_with_optional_column"))) {
                        return TPCH_NATION_DATA;
                    }
                    throw new UnsupportedOperationException();
                })
                .build()));
        runner.createCatalog(MOCK_CATALOG, "mock", ImmutableMap.of());

        runner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withName("mockmissingcolumns")
                .withGetViews((_, _) -> ImmutableMap.of(
                        new SchemaTableName("default", "nation_view"), view))
                .withGetColumns(schemaTableName -> {
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_with_optional_column"))) {
                        return TPCH_NATION_WITH_OPTIONAL_COLUMN;
                    }
                    throw new UnsupportedOperationException();
                })
                .withData(schemaTableName -> {
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_with_optional_column"))) {
                        return TPCH_NATION_DATA;
                    }
                    throw new UnsupportedOperationException();
                })
                .withAllowMissingColumnsOnInsert(true)
                .build()));

        runner.createCatalog(MOCK_CATALOG_MISSING_COLUMNS, "mockmissingcolumns", ImmutableMap.of());

        // Accepts every pushed domain for pruning without enforcing it, and reports it back through table properties and statistics
        runner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withName("mockpushdown")
                .withGetColumns(_ -> TPCH_NATION_SCHEMA)
                .withData(_ -> TPCH_NATION_DATA)
                .withApplyFilter((_, handle, constraint) -> {
                    MockConnectorTableHandle table = (MockConnectorTableHandle) handle;
                    TupleDomain<ColumnHandle> newConstraint = table.getConstraint().intersect(constraint.getSummary());
                    if (newConstraint.equals(table.getConstraint())) {
                        return Optional.empty();
                    }
                    return Optional.of(new ConstraintApplicationResult<>(
                            new MockConnectorTableHandle(table.getTableName(), newConstraint, table.getColumns()),
                            constraint.getSummary(),
                            constraint.getExpression(),
                            false));
                })
                .withGetTableProperties((_, handle) -> new ConnectorTableProperties(
                        ((MockConnectorTableHandle) handle).getConstraint(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of()))
                .withGetTableStatistics(_ -> TableStatistics.builder()
                        .setRowCount(Estimate.of(25))
                        .setColumnStatistics(
                                new MockConnectorColumnHandle("nationkey", BIGINT),
                                ColumnStatistics.builder()
                                        .setRange(new DoubleRange(POLICY_KEY, POLICY_KEY))
                                        .setDistinctValuesCount(Estimate.of(1))
                                        .setNullsFraction(Estimate.zero())
                                        .build())
                        .build())
                .build()));
        runner.createCatalog(MOCK_CATALOG_PUSHDOWN, "mockpushdown", ImmutableMap.of());

        // The fixture data satisfies the predicates used below. Model a connector that consumes
        // accepted predicates and retains their domains in its handle, as JDBC connectors can.
        runner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withName(MOCK_CATALOG_CONSUMING_PREDICATES)
                .withGetColumns(_ -> TPCH_NATION_SCHEMA)
                .withData(_ -> TPCH_NATION_DATA)
                .withApplyFilter((_, handle, constraint) -> {
                    MockConnectorTableHandle table = (MockConnectorTableHandle) handle;
                    TupleDomain<ColumnHandle> newConstraint = table.getConstraint().intersect(constraint.getSummary());
                    boolean hasExpression = !constraint.getExpression().equals(Constant.TRUE);
                    if (newConstraint.equals(table.getConstraint()) && !hasExpression) {
                        return Optional.empty();
                    }
                    pushedPredicate.set(newConstraint);
                    if (hasExpression) {
                        pushedExpressions.incrementAndGet();
                    }
                    return Optional.of(new ConstraintApplicationResult<>(
                            new MockConnectorTableHandle(table.getTableName(), newConstraint, table.getColumns()),
                            TupleDomain.all(),
                            Constant.TRUE,
                            false));
                })
                .withGetTableProperties((_, handle) -> new ConnectorTableProperties(
                        ((MockConnectorTableHandle) handle).getConstraint(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of()))
                .build()));
        runner.createCatalog(MOCK_CATALOG_CONSUMING_PREDICATES, MOCK_CATALOG_CONSUMING_PREDICATES, ImmutableMap.of());

        assertions = new QueryAssertions(runner);

        assertions.addFunctions(InternalFunctionBundle.builder()

                .scalars(DeprecatedFunctions.class)

                .build());
        accessControl = assertions.getQueryRunner().getAccessControl();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testSecureFilterIsTransparentAndRedacted()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("clerk <> '" + POLICY_TOKEN + "'")
                        .secure(true)
                        .build());

        assertThat(assertions.query("SELECT count(*) FROM orders")).matches("VALUES BIGINT '15000'");
        assertRedactedPlan("EXPLAIN (TYPE LOGICAL) SELECT count(*) FROM orders");
        assertRedactedPlan("EXPLAIN SELECT count(*) FROM orders");
        assertRedactedPlan("EXPLAIN (TYPE LOGICAL, FORMAT JSON) SELECT count(*) FROM orders");
        assertRedactedPlan("EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON) SELECT count(*) FROM orders");
        assertRedactedPlan("EXPLAIN (FORMAT GRAPHVIZ) SELECT count(*) FROM orders");

        String ioPlan = (String) assertions.getQueryRunner()
                .execute(SESSION, "EXPLAIN (TYPE IO) SELECT count(*) FROM orders")
                .getOnlyValue();
        assertThat(ioPlan).doesNotContain(POLICY_TOKEN);
    }

    @Test
    public void testSecurePredicateConsumptionDoesNotExposePolicy()
    {
        accessControl.reset();
        pushedPredicate.set(TupleDomain.all());
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG_CONSUMING_PREDICATES, "tiny", "nation"),
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("nationkey <= " + POLICY_KEY)
                        .secure(true)
                        .build());
        StandaloneQueryRunner runner = (StandaloneQueryRunner) assertions.getQueryRunner();
        String query = "SELECT nationkey FROM " + MOCK_CATALOG_CONSUMING_PREDICATES + ".tiny.nation ORDER BY nationkey LIMIT 3";
        String plan = (String) runner.execute(SESSION, "EXPLAIN " + query).getOnlyValue();
        assertThat(plan).doesNotContain(Long.toString(POLICY_KEY)).contains("[REDACTED]");

        MaterializedResultWithPlan result = runner.executeWithPlan(SESSION, query);
        assertThat(result.result().getOnlyColumn()).containsExactly(0L, 1L, 2L);

        QueryInfo info = runner.getCoordinator().getQueryManager().getFullQueryInfo(result.queryId());
        JsonCodecFactory codecs = runner.getCoordinator().getInstance(Key.get(JsonCodecFactory.class));
        JsonCodec<TableInfo> tableInfoCodec = codecs.jsonCodec(TableInfo.class);
        List<String> reportedPredicates = info.getStages().orElseThrow().getStages().stream()
                .flatMap(stage -> stage.tables().values().stream())
                .map(table -> tableInfoCodec.fromJson(tableInfoCodec.toJson(table)).predicate().toString())
                .toList();
        assertThat(reportedPredicates).isNotEmpty().allSatisfy(predicate -> assertThat(predicate).doesNotContain(Long.toString(POLICY_KEY)));
        String json = codecs.jsonCodec(QueryInfo.class).toJson(info);
        assertThat(json).contains("RedactedTableHandle");
        assertThat(pushedPredicate.get().getDomains().orElseThrow()).containsKey(new MockConnectorColumnHandle("nationkey", BIGINT));
    }

    @Test
    public void testPublicConnectorExpressionCanBeConsumed()
    {
        accessControl.reset();
        pushedExpressions.set(0);
        String query = "SELECT count(*) FROM " + MOCK_CATALOG_CONSUMING_PREDICATES + ".tiny.nation WHERE length(comment) < " + POLICY_KEY;
        assertThat(assertions.query(query)).matches("VALUES BIGINT '25'");
        assertThat(pushedExpressions.get()).isPositive();
        String plan = (String) assertions.getQueryRunner().execute(SESSION, "EXPLAIN " + query).getOnlyValue();
        assertThat(plan).doesNotContain("filterPredicate", "[REDACTED]");
    }

    @Test
    public void testWarningsAboutSecureExpressionsAreNotReported()
    {
        // the warning would name the deprecated function the policy calls
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("deprecated_identity(orderkey) > 0")
                        .secure(true)
                        .build());
        assertThat(assertions.getQueryRunner().execute(SESSION, "SELECT count(*) FROM orders").getWarnings()).isEmpty();

        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("deprecated_identity(orderkey) > 0")
                        .build());
        assertThat(assertions.getQueryRunner().execute(SESSION, "SELECT count(*) FROM orders").getWarnings())
                .extracting(Warning::getMessage)
                .anyMatch(message -> message.contains("deprecated_identity"));
    }

    @Test
    public void testSecureFilterDomainIsNotInferredAcrossJoinsInClearText()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG_PUSHDOWN, "tiny", "nation"),
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("nationkey <= " + POLICY_KEY)
                        .secure(true)
                        .build());

        // the pushed domain is reported through table properties and must not be inferred onto regionkey in clear text
        String query = "SELECT count(*) FROM mockpushdown.tiny.nation n JOIN mockpushdown.tiny.region r ON n.nationkey = r.regionkey";
        assertThat(assertions.query(query)).matches("VALUES BIGINT '25'");

        for (String explain : ImmutableList.of("EXPLAIN", "EXPLAIN (TYPE LOGICAL)", "EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON)", "EXPLAIN ANALYZE", "EXPLAIN ANALYZE VERBOSE")) {
            String plan = (String) assertions.getQueryRunner().execute(SESSION, explain + " " + query).getOnlyValue();
            assertThat(plan)
                    .contains("[REDACTED]")
                    .doesNotContain(Long.toString(POLICY_KEY));
        }
        String ioPlan = (String) assertions.getQueryRunner().execute(SESSION, "EXPLAIN (TYPE IO) " + query).getOnlyValue();
        assertThat(ioPlan).doesNotContain(Long.toString(POLICY_KEY));
    }

    @Test
    public void testValueRangesAreNotRecordedForPlansWithSecureExpressions()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG_PUSHDOWN, "tiny", "nation"),
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("nationkey <= " + POLICY_KEY)
                        .secure(true)
                        .build());

        // the connector pins nationkey's range at POLICY_KEY, which would show in the per-stage statistics of the query JSON
        Session session = Session.builder(SESSION)
                .setSystemProperty(COLLECT_PLAN_STATISTICS_FOR_ALL_QUERIES, "true")
                .build();
        StandaloneQueryRunner runner = (StandaloneQueryRunner) assertions.getQueryRunner();
        MaterializedResultWithPlan result = runner.executeWithPlan(session, "SELECT nationkey FROM mockpushdown.tiny.nation ORDER BY nationkey LIMIT 3");
        assertThat(result.result().getOnlyColumn()).containsExactly(0L, 1L, 2L);

        QueryInfo queryInfo = runner.getCoordinator().getQueryManager().getFullQueryInfo(result.queryId());
        List<SymbolStatsEstimate> statistics = queryInfo.getStages().orElseThrow().getStages().stream()
                .flatMap(stage -> stage.plan().getStatsAndCosts().getStats().values().stream())
                .flatMap(estimate -> estimate.getSymbolStatistics().values().stream())
                .collect(toImmutableList());
        assertThat(statistics).isNotEmpty();
        assertThat(statistics).noneMatch(estimate -> estimate.getLowValue() == POLICY_KEY || estimate.getHighValue() == POLICY_KEY);
        // the rest of the estimate is still reported
        assertThat(statistics).anyMatch(estimate -> !Double.isNaN(estimate.getDistinctValuesCount()));
    }

    @Test
    public void testSecureFilterDomainIsPushedDownButNotReported()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG_PUSHDOWN, "tiny", "nation"),
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("length(comment) <> " + POLICY_KEY + " AND name <> '" + POLICY_TOKEN + "'")
                        .secure(true)
                        .build());
        // a secure mask: its output must be treated like the column the policy constrains
        accessControl.columnMask(
                new QualifiedObjectName(MOCK_CATALOG_PUSHDOWN, "tiny", "nation"),
                "nationkey",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("CAST(nationkey * 1 AS bigint)")
                        .secure(true)
                        .build());

        String query = "SELECT count(*) FROM mockpushdown.tiny.nation WHERE regionkey = 1";
        assertThat(assertions.query(query)).matches("VALUES BIGINT '5'");

        for (String explain : ImmutableList.of("EXPLAIN", "EXPLAIN (TYPE LOGICAL)", "EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON)", "EXPLAIN ANALYZE")) {
            String plan = (String) assertions.getQueryRunner().execute(SESSION, explain + " " + query).getOnlyValue();
            // the user's own domain still shows on the scan, the secure ones do not
            assertThat(plan)
                    .contains("[REDACTED]")
                    .contains("mockpushdown.tiny.nation")
                    .contains("regionkey")
                    .contains("[[1]]")
                    .doesNotContain("RedactedTableHandle", "RedactedColumnHandle")
                    .doesNotContain(POLICY_TOKEN)
                    .doesNotContain(Long.toString(POLICY_KEY));
        }
        String ioPlan = (String) assertions.getQueryRunner().execute(SESSION, "EXPLAIN (TYPE IO) " + query).getOnlyValue();
        assertThat(ioPlan)
                .contains("regionkey")
                .doesNotContain(POLICY_TOKEN)
                .doesNotContain(Long.toString(POLICY_KEY));

        // the table constraints recorded per stage in QueryInfo (query JSON, Web UI) keep only the user's own domain
        StandaloneQueryRunner runner = (StandaloneQueryRunner) assertions.getQueryRunner();
        MaterializedResultWithPlan result = runner.executeWithPlan(SESSION, query);
        QueryInfo queryInfo = runner.getCoordinator().getQueryManager().getFullQueryInfo(result.queryId());
        List<String> constrainedColumns = queryInfo.getStages().orElseThrow().getStages().stream()
                .flatMap(stage -> stage.tables().values().stream())
                .flatMap(table -> table.predicate().getDomains().orElseThrow().keySet().stream())
                .map(column -> ((MockConnectorColumnHandle) column).name())
                .collect(toImmutableList());
        assertThat(constrainedColumns).containsExactly("regionkey");
        assertThat(queryInfo.getReferencedTables().stream()
                .flatMap(table -> table.getColumns().stream())
                .map(ColumnInfo::getColumn))
                .containsExactly("regionkey");
        // the functions a policy calls are part of its text and must not appear in the query's routines
        assertThat(queryInfo.getRoutines())
                .extracting(RoutineInfo::getRoutine)
                .contains("count")
                .doesNotContain("length");

        // connector statistics reflect the pushed domain, so the statistics of a masked or constrained column are withheld
        MaterializedRow nationKeyStats = nationKeyStats();
        assertThat(nationKeyStats.getField(2)).isNull();
        assertThat(nationKeyStats.getField(5)).isNull();
        assertThat(nationKeyStats.getField(6)).isNull();

        accessControl.reset();
        nationKeyStats = nationKeyStats();
        assertThat(nationKeyStats.getField(5)).isEqualTo(Long.toString(POLICY_KEY));
        assertThat(nationKeyStats.getField(6)).isEqualTo(Long.toString(POLICY_KEY));
    }

    @Test
    public void testSecurePolicyOnlyReferencedColumns()
    {
        accessControl.reset();
        for (QualifiedObjectName table : ImmutableList.of(
                new QualifiedObjectName(MOCK_CATALOG_PUSHDOWN, "tiny", "nation"),
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation_with_optional_column"),
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "nation"))) {
            for (String identity : ImmutableList.of(USER, VIEW_OWNER)) {
                accessControl.rowFilter(
                        table,
                        identity,
                        ViewExpression.builder().identity(identity).expression("length(comment) > 0").secure(true).build());
            }
        }

        StandaloneQueryRunner runner = (StandaloneQueryRunner) assertions.getQueryRunner();
        ImmutableMap.<String, List<String>>builder()
                .put("SELECT name FROM mockpushdown.tiny.nation", ImmutableList.of("name"))
                .put("SELECT count(*) FROM mockpushdown.tiny.nation", ImmutableList.of())
                .put("SELECT name, comment FROM mockpushdown.tiny.nation", ImmutableList.of("name", "comment"))
                .put("SELECT * FROM mockpushdown.tiny.nation", ImmutableList.of("nationkey", "name", "regionkey", "comment"))
                .put("SELECT public_name FROM (SELECT name AS public_name FROM mockpushdown.tiny.nation) t", ImmutableList.of("name"))
                .put("SELECT n.name FROM mockpushdown.tiny.nation n JOIN mockpushdown.tiny.nation r USING (comment)", ImmutableList.of("name", "comment"))
                .put("SELECT name FROM TABLE(system.builtin.exclude_columns(INPUT => TABLE(mockpushdown.tiny.nation), COLUMNS => DESCRIPTOR(nationkey)))",
                        ImmutableList.of("name", "regionkey", "comment"))
                .put("SELECT name FROM mock.tiny.nation_with_optional_column FOR VERSION AS OF 'dev'", ImmutableList.of("name"))
                .put("SELECT name FROM mock.default.nation_view", ImmutableList.of("nationkey", "name"))
                .buildOrThrow()
                .forEach((query, expectedColumns) -> {
                    MaterializedResultWithPlan result = runner.executeWithPlan(SESSION, query);
                    QueryInfo info = runner.getCoordinator().getQueryManager().getFullQueryInfo(result.queryId());
                    assertThat(info.getReferencedTables().stream()
                            .flatMap(table -> table.getColumns().stream())
                            .map(ColumnInfo::getColumn)
                            .distinct())
                            .as(query)
                            .containsExactlyInAnyOrderElementsOf(expectedColumns);
                });

        Session disabled = Session.builder(SESSION)
                .setSystemProperty(SECURE_EXPRESSION_REDACTION_ENABLED, "false")
                .build();
        MaterializedResultWithPlan result = runner.executeWithPlan(disabled, "SELECT name FROM mockpushdown.tiny.nation");
        QueryInfo info = runner.getCoordinator().getQueryManager().getFullQueryInfo(result.queryId());
        assertThat(info.getReferencedTables().stream()
                .flatMap(table -> table.getColumns().stream())
                .map(ColumnInfo::getColumn))
                .containsExactlyInAnyOrder("name", "comment");
    }

    @Test
    public void testReportedSecureConjunctCountDoesNotDependOnPolicyBound()
    {
        // A query must be reported identically under two policy bounds, otherwise EXPLAIN alone can search
        // for the bound. Each predicate leaves a non-empty domain under both bounds: an empty domain replaces
        // the scan with an empty Values node, which stays visible.
        List<String> queries = ImmutableList.of(
                "SELECT count(*) FROM mockpushdown.tiny.nation",
                "SELECT count(*) FROM mockpushdown.tiny.nation WHERE nationkey < 3",
                "SELECT count(*) FROM mockpushdown.tiny.nation WHERE nationkey < " + POLICY_KEY,
                "SELECT count(*) FROM mockpushdown.tiny.nation WHERE nationkey <= " + POLICY_KEY,
                "SELECT count(*) FROM mockpushdown.tiny.nation WHERE nationkey < " + (POLICY_KEY + 1),
                "SELECT count(*) FROM mockpushdown.tiny.nation WHERE nationkey >= " + POLICY_KEY,
                "SELECT count(*) FROM mockpushdown.tiny.nation WHERE nationkey BETWEEN 5 AND " + POLICY_KEY,
                "SELECT count(*) FROM mockpushdown.tiny.nation WHERE nationkey IN (1, 2, " + (POLICY_KEY + 5) + ")",
                "SELECT count(*) FROM mockpushdown.tiny.nation WHERE nationkey % 2 = 0",
                "SELECT count(*) FROM mockpushdown.tiny.nation WHERE regionkey = 1",
                "SELECT count(*) FROM mockpushdown.tiny.nation WHERE regionkey = 1 AND nationkey < " + (POLICY_KEY + 1),
                "SELECT count(*) FROM mockpushdown.tiny.nation WHERE nationkey < " + (POLICY_KEY + 1) + " AND regionkey = 1",
                "SELECT count(*) FROM mockpushdown.tiny.nation n JOIN mockpushdown.tiny.region r ON n.nationkey = r.regionkey");
        long otherBound = POLICY_KEY + 10;

        List<String> reports = reportedPlans(POLICY_KEY, queries);
        List<String> otherReports = reportedPlans(otherBound, queries);
        for (int i = 0; i < queries.size(); i++) {
            assertThat(otherReports.get(i)).as(queries.get(i))
                    .doesNotContain(Long.toString(otherBound))
                    .isEqualTo(reports.get(i));
        }
    }

    private List<String> reportedPlans(long bound, List<String> queries)
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG_PUSHDOWN, "tiny", "nation"),
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("nationkey <= " + bound)
                        .secure(true)
                        .build());
        StandaloneQueryRunner runner = (StandaloneQueryRunner) assertions.getQueryRunner();
        JsonCodec<Expression> expressionCodec = runner.getCoordinator().getInstance(Key.get(JsonCodecFactory.class)).jsonCodec(Expression.class);
        ImmutableList.Builder<String> reports = ImmutableList.builder();
        for (String query : queries) {
            StringBuilder report = new StringBuilder();
            for (String explain : ImmutableList.of("EXPLAIN", "EXPLAIN (TYPE LOGICAL)", "EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON)", "EXPLAIN (TYPE IO)")) {
                report.append(explain).append('\n').append((String) runner.execute(SESSION, explain + " " + query).getOnlyValue()).append('\n');
            }
            // execution statistics differ between runs, so only the lines carrying the marker are compared
            for (String explain : ImmutableList.of("EXPLAIN ANALYZE", "EXPLAIN ANALYZE VERBOSE")) {
                String plan = (String) runner.execute(SESSION, explain + " " + query).getOnlyValue();
                plan.lines().filter(line -> line.contains(REDACTED)).map(String::strip).forEach(line -> report.append(line).append('\n'));
            }
            // the reporting fragments recorded in QueryInfo (query JSON, Web UI, event listeners)
            MaterializedResultWithPlan result = runner.executeWithPlan(SESSION, query);
            QueryInfo queryInfo = runner.getCoordinator().getQueryManager().getFullQueryInfo(result.queryId());
            queryInfo.getStages().orElseThrow().getStages().stream()
                    .flatMap(stage -> PlanNodeSearcher.searchFrom(stage.plan().getRoot()).where(FilterNode.class::isInstance).findAll().stream())
                    .forEach(node -> {
                        Expression predicate = ((FilterNode) node).getPredicate();
                        report.append(predicate).append('\n');
                        report.append(expressionCodec.toJson(predicate)).append('\n');
                    });
            // a logical expression is reported with one marker however many secure terms it holds
            report.toString().lines().forEach(line ->
                    assertThat(Splitter.on(REDACTED).splitToStream(line).count() - 1).as("%s: %s", query, line).isLessThanOrEqualTo(1));
            reports.add(report.toString());
        }
        return reports.build();
    }

    private MaterializedRow nationKeyStats()
    {
        return assertions.getQueryRunner().execute(SESSION, "SHOW STATS FOR mockpushdown.tiny.nation").getMaterializedRows().stream()
                .filter(row -> "nationkey".equals(row.getField(0)))
                .collect(onlyElement());
    }

    @Test
    public void testShowStatsDoesNotExposePolicyBoundsThroughJoinOrUnion()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG_PUSHDOWN, "tiny", "nation"),
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("nationkey <= " + POLICY_KEY)
                        .secure(true)
                        .build());

        for (String query : ImmutableList.of(
                "SELECT r.nationkey FROM mockpushdown.tiny.nation n JOIN mockpushdown.tiny.region r ON n.nationkey = r.nationkey",
                "SELECT nationkey FROM mockpushdown.tiny.nation UNION ALL SELECT nationkey FROM mockpushdown.tiny.region")) {
            assertThat(assertions.getQueryRunner().execute(SESSION, "SHOW STATS FOR (" + query + ")").getMaterializedRows())
                    .filteredOn(row -> row.getField(0) != null)
                    .isNotEmpty()
                    .allSatisfy(row -> {
                        assertThat(row.getField(5)).isNull();
                        assertThat(row.getField(6)).isNull();
                    });
        }
    }

    @Test
    public void testTrimRoutineInSecureFilterIsNotReported()
    {
        StandaloneQueryRunner runner = (StandaloneQueryRunner) assertions.getQueryRunner();
        for (boolean secure : ImmutableList.of(false, true)) {
            accessControl.reset();
            accessControl.rowFilter(
                    new QualifiedObjectName(LOCAL_CATALOG, "tiny", "nation"),
                    USER,
                    ViewExpression.builder()
                            .identity(USER)
                            .expression("trim(name) <> '" + POLICY_TOKEN + "'")
                            .secure(secure)
                            .build());
            MaterializedResultWithPlan result = runner.executeWithPlan(SESSION, "SELECT count(*) FROM nation");
            assertThat(result.result().getOnlyValue()).isEqualTo(25L);
            assertThat(runner.getCoordinator().getQueryManager().getFullQueryInfo(result.queryId()).getRoutines().stream()
                    .anyMatch(routine -> routine.getRoutine().equals("trim")))
                    .isEqualTo(!secure);
        }
    }

    @Test
    public void testNonSecureFilterIsNotRedacted()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("clerk <> concat(CAST(random() AS VARCHAR), '" + POLICY_TOKEN + "')")
                        .build());

        String plan = (String) assertions.getQueryRunner()
                .execute(SESSION, "EXPLAIN (TYPE LOGICAL) SELECT count(*) FROM orders")
                .getOnlyValue();
        assertThat(plan).contains(POLICY_TOKEN).doesNotContain("[REDACTED]");
    }

    @Test
    public void testSecureFilterBehindDefinerViewIsRedactedWithSessionOptIn()
    {
        // the view body is analyzed under a view session without the property, so the decision must come from the root session
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "nation"),
                VIEW_OWNER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("name <> '" + POLICY_TOKEN + "'")
                        .secure(true)
                        .build());

        Session session = Session.builder(SESSION)
                .setIdentity(Identity.forUser(RUN_AS_USER).build())
                .build();
        String plan = (String) assertions.getQueryRunner()
                .execute(session, "EXPLAIN (TYPE LOGICAL) SELECT name FROM mock.default.nation_view")
                .getOnlyValue();
        assertThat(plan).contains("[REDACTED]").doesNotContain(POLICY_TOKEN);
    }

    @Test
    public void testSecureFilterWithSubqueryIsRejected()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("orderkey IN (SELECT nationkey FROM nation WHERE name <> '" + POLICY_TOKEN + "')")
                        .secure(true)
                        .build());
        assertThat(assertions.query("SELECT count(*) FROM orders"))
                .failure()
                .hasErrorCode(INVALID_ROW_FILTER)
                .hasMessageContaining("Invalid row filter for 'local.tiny.orders': [REDACTED]");
    }

    @Test
    public void testSecureFilterErrorsAreRedacted()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder().identity(USER).expression("row_policy_parse_secret $$$").secure(true).build());
        assertThat(assertions.query("SELECT count(*) FROM orders"))
                .failure()
                .hasMessage("line 1:22: Invalid row filter for 'local.tiny.orders': [REDACTED]");

        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder().identity(USER).expression("row_policy_unknown_column = 1").secure(true).build());
        assertThat(assertions.query("SELECT count(*) FROM orders"))
                .failure()
                .hasMessage("line 1:22: Invalid row filter for 'local.tiny.orders': [REDACTED]");

        // the aggregation check names the offending function, which must be redacted as well
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder().identity(USER).expression("count(*) > 0").secure(true).build());
        assertThat(assertions.query("SELECT count(*) FROM orders"))
                .failure()
                .hasErrorCode(EXPRESSION_NOT_SCALAR)
                .hasMessage("line 1:22: Invalid row filter for 'local.tiny.orders': [REDACTED]");

        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "nation"),
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("CAST(name || '-row-policy-runtime-secret' AS BIGINT) > 0")
                        .secure(true)
                        .build());
        assertThat(assertions.query("SELECT count(*) FROM nation"))
                .failure()
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("[REDACTED]");
    }

    @Test
    public void testSimpleFilter()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder().expression("orderkey < 10").build());
        assertThat(assertions.query("SELECT count(*) FROM orders")).matches("VALUES BIGINT '7'");

        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder().expression("NULL").build());
        assertThat(assertions.query("SELECT count(*) FROM orders")).matches("VALUES BIGINT '0'");
    }

    private void assertRedactedPlan(String sql)
    {
        String plan = (String) assertions.getQueryRunner().execute(SESSION, sql).getOnlyValue();
        assertThat(plan).contains("[REDACTED]").doesNotContain(POLICY_TOKEN);
    }

    @Test
    public void testSimpleFilterOnBranch()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation_with_optional_column"),
                USER,
                ViewExpression.builder().expression("nationkey < 5").build());
        assertThat(assertions.query("SELECT count(*) FROM mock.tiny.nation_with_optional_column FOR VERSION AS OF 'dev'")).matches("VALUES BIGINT '5'");

        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation_with_optional_column"),
                USER,
                ViewExpression.builder().expression("NULL").build());
        assertThat(assertions.query("SELECT count(*) FROM mock.tiny.nation_with_optional_column FOR VERSION AS OF 'dev'")).matches("VALUES BIGINT '0'");
    }

    @Test
    public void testMultipleFilters()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder().expression("orderkey < 10").build());

        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder().expression("orderkey > 5").build());

        assertThat(assertions.query("SELECT count(*) FROM orders")).matches("VALUES BIGINT '2'");
    }

    @Test
    public void testCorrelatedSubquery()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("EXISTS (SELECT 1 FROM nation WHERE nationkey = orderkey)")
                        .build());
        assertThat(assertions.query("SELECT count(*) FROM orders")).matches("VALUES BIGINT '7'");
    }

    @Test
    public void testView()
    {
        // filter on the underlying table for view owner when running query as different user
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "nation"),
                VIEW_OWNER,
                ViewExpression.builder().expression("nationkey = 1").build());

        assertThat(assertions.query(
                Session.builder(SESSION)
                        .setIdentity(Identity.forUser(RUN_AS_USER).build())
                        .build(),
                "SELECT name FROM mock.default.nation_view"))
                .matches("VALUES CAST('ARGENTINA' AS VARCHAR(25))");

        // filter on the underlying table for view owner when running as themselves
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "nation"),
                VIEW_OWNER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("nationkey = 1")
                        .build());

        assertThat(assertions.query(
                Session.builder(SESSION)
                        .setIdentity(Identity.forUser(VIEW_OWNER).build())
                        .build(),
                "SELECT name FROM mock.default.nation_view"))
                .matches("VALUES CAST('ARGENTINA' AS VARCHAR(25))");

        // filter on the underlying table for user running the query (different from view owner) should not be applied
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "nation"),
                RUN_AS_USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("nationkey = 1")
                        .build());

        Session session = Session.builder(SESSION)
                .setIdentity(Identity.forUser(RUN_AS_USER).build())
                .build();

        assertThat(assertions.query(session, "SELECT count(*) FROM mock.default.nation_view")).matches("VALUES BIGINT '25'");

        // filter on the view
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "default", "nation_view"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("nationkey = 1")
                        .build());
        assertThat(assertions.query("SELECT name FROM mock.default.nation_view")).matches("VALUES CAST('ARGENTINA' AS VARCHAR(25))");
    }

    @Test
    public void testTableReferenceInWithClause()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder().expression("orderkey = 1").build());
        assertThat(assertions.query("WITH t AS (SELECT count(*) FROM orders) SELECT * FROM t")).matches("VALUES BIGINT '1'");
    }

    @Test
    public void testOtherSchema()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("sf1") // Filter is TRUE only if evaluating against sf1.customer
                        .expression("(SELECT count(*) FROM customer) = 150000")
                        .build());
        assertThat(assertions.query("SELECT count(*) FROM orders")).matches("VALUES BIGINT '15000'");
    }

    @Test
    public void testDifferentIdentity()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                RUN_AS_USER,
                ViewExpression.builder()
                        .identity(RUN_AS_USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("orderkey = 1")
                        .build());

        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .identity(RUN_AS_USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny").expression("orderkey IN (SELECT orderkey FROM orders)")
                        .build());

        assertThat(assertions.query("SELECT count(*) FROM orders")).matches("VALUES BIGINT '1'");
    }

    @Test
    public void testRecursion()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("orderkey IN (SELECT orderkey FROM orders)")
                        .build());

        assertThat(assertions.query("SELECT count(*) FROM orders"))
                .failure()
                .hasErrorCode(INVALID_ROW_FILTER)
                .hasMessageMatching(".*\\QRow filter for 'local.tiny.orders' is recursive\\E.*");

        // different reference style to same table
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("orderkey IN (SELECT local.tiny.orderkey FROM orders)")
                        .build());
        assertThat(assertions.query("SELECT count(*) FROM orders"))
                .failure()
                .hasErrorCode(INVALID_ROW_FILTER)
                .hasMessageMatching(".*\\QRow filter for 'local.tiny.orders' is recursive\\E.*");

        // mutual recursion
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                RUN_AS_USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("orderkey IN (SELECT orderkey FROM orders)")
                        .build());

        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("orderkey IN (SELECT orderkey FROM orders)")
                        .build());

        assertThat(assertions.query("SELECT count(*) FROM orders"))
                .failure()
                .hasErrorCode(INVALID_ROW_FILTER)
                .hasMessageMatching(".*\\QRow filter for 'local.tiny.orders' is recursive\\E.*");
    }

    @Test
    public void testLimitedScope()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "customer"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("orderkey = 1")
                        .build());
        assertThat(assertions.query(
                "SELECT (SELECT min(name) FROM customer WHERE customer.custkey = orders.custkey) FROM orders"))
                .failure()
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessage("line 1:31: Invalid row filter for 'local.tiny.customer': Column 'orderkey' cannot be resolved");
    }

    @Test
    public void testSqlInjection()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "nation"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("regionkey IN (SELECT regionkey FROM region WHERE name = 'ASIA')")
                        .build());
        assertThat(assertions.query(
                "WITH region(regionkey, name) AS (VALUES (0, 'ASIA'), (1, 'ASIA'), (2, 'ASIA'), (3, 'ASIA'), (4, 'ASIA'))" +
                        "SELECT name FROM nation ORDER BY name LIMIT 1"))
                .matches("VALUES CAST('CHINA' AS VARCHAR(25))"); // if sql-injection would work then query would return ALGERIA
    }

    @Test
    public void testInvalidFilter()
    {
        // parse error
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("$$$")
                        .build());

        assertThat(assertions.query("SELECT count(*) FROM orders"))
                .failure()
                .hasErrorCode(INVALID_ROW_FILTER)
                .hasMessage("line 1:22: Invalid row filter for 'local.tiny.orders': mismatched input '$'. Expecting: <expression>");

        // unknown column
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("unknown_column")
                        .build());

        assertThat(assertions.query("SELECT count(*) FROM orders"))
                .failure()
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessage("line 1:22: Invalid row filter for 'local.tiny.orders': Column 'unknown_column' cannot be resolved");

        // invalid type
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("1")
                        .build());

        assertThat(assertions.query("SELECT count(*) FROM orders"))
                .failure()
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:22: Expected row filter for 'local.tiny.orders' to be of type BOOLEAN, but was integer");

        // aggregation
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("count(*) > 0")
                        .build());

        assertThat(assertions.query("SELECT count(*) FROM orders"))
                .failure()
                .hasErrorCode(EXPRESSION_NOT_SCALAR)
                .hasMessage("line 1:10: Row filter for 'local.tiny.orders' cannot contain aggregations, window functions or grouping operations: [count(*)]");

        // window function
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("row_number() OVER () > 0")
                        .build());

        assertThat(assertions.query("SELECT count(*) FROM orders"))
                .failure()
                .hasErrorCode(EXPRESSION_NOT_SCALAR)
                .hasMessage("line 1:22: Row filter for 'local.tiny.orders' cannot contain aggregations, window functions or grouping operations: [row_number() OVER ()]");

        // window function
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("grouping(orderkey) = 0")
                        .build());

        assertThat(assertions.query("SELECT count(*) FROM orders"))
                .failure()
                .hasErrorCode(EXPRESSION_NOT_SCALAR)
                .hasMessage("line 1:20: Row filter for 'local.tiny.orders' cannot contain aggregations, window functions or grouping operations: [GROUPING (orderkey)]");
    }

    @Test
    public void testShowStats()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .identity(RUN_AS_USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("orderkey = 0")
                        .build());

        assertThat(assertions.query("SHOW STATS FOR (SELECT * FROM tiny.orders)"))
                .containsAll(
                        "VALUES " +
                                "(VARCHAR 'orderkey', 0e1, 0e1, 1e0, CAST(NULL AS double), CAST(NULL AS varchar), CAST(NULL AS varchar))," +
                                "(VARCHAR 'custkey', 0e1, 0e1, 1e0, CAST(NULL AS double), CAST(NULL AS varchar), CAST(NULL AS varchar))," +
                                "(NULL, NULL, NULL, NULL, 0e1, NULL, NULL)");
    }

    /**
     * @see #testMergeDelete()
     */
    @Test
    public void testDelete()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation"),
                USER,
                ViewExpression.builder().expression("nationkey < 10").build());

        // Within allowed row filter
        assertions.query("DELETE FROM mock.tiny.nation WHERE nationkey < 3")
                .assertThat()
                .matches("SELECT BIGINT '3'");
        assertions.query("DELETE FROM mock.tiny.nation WHERE nationkey IN (1, 2, 3)")
                .assertThat()
                .matches("SELECT BIGINT '3'");

        // Outside allowed row filter, only readable rows were dropped
        assertions.query("DELETE FROM mock.tiny.nation")
                .assertThat()
                .matches("SELECT BIGINT '10'");
        assertions.query("DELETE FROM mock.tiny.nation WHERE nationkey IN (1, 11)")
                .assertThat()
                .matches("SELECT BIGINT '1'");
        assertions.query("DELETE FROM mock.tiny.nation WHERE nationkey >= 10")
                .assertThat()
                .matches("SELECT BIGINT '0'");
    }

    /**
     * Like {@link #testDelete()} but using the MERGE statement.
     */
    @Test
    public void testMergeDelete()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation"),
                USER,
                ViewExpression.builder().expression("nationkey < 10").build());

        // Within allowed row filter
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 1,2) t(x) ON nationkey = x
                WHEN MATCHED THEN DELETE
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");

        // Outside allowed row filter
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 1,2,3,4,5) t(x) ON regionkey = x
                WHEN MATCHED THEN DELETE
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 1,11) t(x) ON nationkey = x
                WHEN MATCHED THEN DELETE
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 11,12,13,14,15) t(x) ON nationkey = x
                WHEN MATCHED THEN DELETE
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
    }

    /**
     * @see #testMergeUpdate()
     */
    @Test
    public void testUpdate()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation"),
                USER,
                ViewExpression.builder().expression("nationkey < 10").build());

        // Within allowed row filter
        assertThat(assertions.query("UPDATE mock.tiny.nation SET regionkey = regionkey * 2 WHERE nationkey < 3"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");
        assertThat(assertions.query("UPDATE mock.tiny.nation SET regionkey = regionkey * 2 WHERE nationkey IN (1, 2, 3)"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");

        // Outside allowed row filter
        assertThat(assertions.query("UPDATE mock.tiny.nation SET regionkey = regionkey * 2"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");
        assertThat(assertions.query("UPDATE mock.tiny.nation SET regionkey = regionkey * 2 WHERE nationkey IN (1, 11)"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");

        assertThat(assertions.query("UPDATE mock.tiny.nation SET regionkey = regionkey * 2 WHERE nationkey = 11"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");

        // Within allowed row filter, but updated rows are outside the row filter
        assertThat(assertions.query("UPDATE mock.tiny.nation SET nationkey = 10 WHERE nationkey < 3"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");
        assertThat(assertions.query("UPDATE mock.tiny.nation SET nationkey = null WHERE nationkey < 3"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");

        // Outside allowed row filter, and updated rows are outside the row filter
        assertThat(assertions.query("UPDATE mock.tiny.nation SET nationkey = 10 WHERE nationkey = 10"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");
        assertThat(assertions.query("UPDATE mock.tiny.nation SET nationkey = null WHERE nationkey = null "))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");
    }

    /**
     * Like {@link #testUpdate()} but using the MERGE statement.
     */
    @Test
    public void testMergeUpdate()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation"),
                USER,
                ViewExpression.builder().expression("nationkey < 10").build());

        // Within allowed row filter
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 5) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET regionkey = regionkey * 2
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");

        // Outside allowed row filter
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 1,2,3,4,5,6) t(x) ON regionkey = x
                WHEN MATCHED THEN UPDATE SET regionkey = regionkey * 2
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 1, 11) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET regionkey = regionkey * 2
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 11) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET regionkey = regionkey * 2
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");

        // Within allowed row filter, but updated rows are outside the row filter
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 1,2,3) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET nationkey = 10
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 1,2,3) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET nationkey = NULL
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");

        // Outside allowed row filter, but updated rows are outside the row filter
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 10) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET nationkey = 13
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 10) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET nationkey = NULL
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 10) t(x) ON nationkey IS NULL
                WHEN MATCHED THEN UPDATE SET nationkey = 13
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
    }

    /**
     * @see #testMergeInsert()
     */
    @Test
    public void testInsert()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation"),
                USER,
                ViewExpression.builder().expression("nationkey > 100").build());

        // Within allowed row filter
        assertions.query("INSERT INTO mock.tiny.nation VALUES (101, 'POLAND', 0, 'No comment')")
                .assertThat()
                .skippingTypesCheck()
                .matches("SELECT BIGINT '1'");

        // Outside allowed row filter
        assertThat(assertions.query("INSERT INTO mock.tiny.nation VALUES (26, 'POLAND', 0, 'No comment')"))
                .failure()
                .hasErrorCode(PERMISSION_DENIED)
                .hasMessage("Access Denied: Cannot insert row that does not match a row filter");
        assertThat(assertions.query("INSERT INTO mock.tiny.nation VALUES "
                + "(26, 'POLAND', 0, 'No comment'),"
                + "(27, 'HOLLAND', 0, 'A comment')"))
                .failure()
                .hasErrorCode(PERMISSION_DENIED)
                .hasMessage("Access Denied: Cannot insert row that does not match a row filter");
        assertThat(assertions.query("INSERT INTO mock.tiny.nation(nationkey) VALUES (null)"))
                .failure()
                .hasErrorCode(PERMISSION_DENIED)
                .hasMessage("Access Denied: Cannot insert row that does not match a row filter");
        assertThat(assertions.query("INSERT INTO mock.tiny.nation(regionkey) VALUES (0)"))
                .failure()
                .hasErrorCode(PERMISSION_DENIED)
                .hasMessage("Access Denied: Cannot insert row that does not match a row filter");
    }

    /**
     * Like {@link #testInsert()} but using the MERGE statement.
     */
    @Test
    public void testMergeInsert()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation"),
                USER,
                ViewExpression.builder().expression("nationkey > 100").build());

        // Within allowed row filter
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 42) t(dummy) ON false
                WHEN NOT MATCHED THEN INSERT VALUES (101, 'POLAND', 0, 'No comment')
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");

        // Outside allowed row filter
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 42) t(dummy) ON false
                WHEN NOT MATCHED THEN INSERT VALUES (26, 'POLAND', 0, 'No comment')
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES (26, 'POLAND', 0, 'No comment'), (27, 'HOLLAND', 0, 'A comment')) t(a,b,c,d) ON nationkey = a
                WHEN NOT MATCHED THEN INSERT VALUES (a,b,c,d)
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");

        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 42) t(dummy) ON false
                WHEN NOT MATCHED THEN INSERT (nationkey) VALUES (NULL)
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 42) t(dummy) ON false
                WHEN NOT MATCHED THEN INSERT (nationkey) VALUES (0)
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
    }

    @Test
    public void testRowFilterWithHiddenColumns()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation_with_hidden_column"),
                USER,
                ViewExpression.builder().expression("nationkey < 1").build());

        assertions.query("SELECT * FROM mock.tiny.nation_with_hidden_column")
                .assertThat()
                .skippingTypesCheck()
                .matches("VALUES (BIGINT '0', 'ALGERIA', BIGINT '0', ' haggle. carefully final deposits detect slyly agai')");
        assertThat(assertions.query("INSERT INTO mock.tiny.nation_with_hidden_column VALUES (101, 'POLAND', 0, 'No comment')"))
                .failure()
                .hasErrorCode(PERMISSION_DENIED)
                .hasMessage("Access Denied: Cannot insert row that does not match a row filter");
        assertions.query("INSERT INTO mock.tiny.nation_with_hidden_column VALUES (0, 'POLAND', 0, 'No comment')")
                .assertThat()
                .skippingTypesCheck()
                .matches("VALUES BIGINT '1'");
        assertThat(assertions.query("UPDATE mock.tiny.nation_with_hidden_column SET name = 'POLAND'"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("Updating a table with a row filter is not supported");
        assertions.query("DELETE FROM mock.tiny.nation_with_hidden_column WHERE regionkey < 5")
                .assertThat()
                .skippingTypesCheck()
                .matches("SELECT BIGINT '1'");
        assertions.query("DELETE FROM mock.tiny.nation_with_hidden_column WHERE \"$hidden\" IS NOT NULL")
                .assertThat()
                .skippingTypesCheck()
                .matches("SELECT BIGINT '1'");
    }

    @Test
    public void testRowFilterOnHiddenColumn()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation_with_hidden_column"),
                USER,
                ViewExpression.builder().expression("\"$hidden\" < 1").build());

        assertions.query("SELECT count(*) FROM mock.tiny.nation_with_hidden_column")
                .assertThat()
                .skippingTypesCheck()
                .matches("VALUES BIGINT '25'");
        // TODO https://github.com/trinodb/trino/issues/10006 - support insert into a table with row filter that is using hidden columns
        assertThat(assertions.query("INSERT INTO mock.tiny.nation_with_hidden_column VALUES (101, 'POLAND', 0, 'No comment')"))
                // TODO this should be TrinoException (assertTrinoExceptionThrownBy)
                .nonTrinoExceptionFailure()
                .hasStackTraceContaining("ArrayIndexOutOfBoundsException: Index 4 out of bounds for length 4");
        assertThat(assertions.query("UPDATE mock.tiny.nation_with_hidden_column SET name = 'POLAND'"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("Updating a table with a row filter is not supported");
        assertions.query("DELETE FROM mock.tiny.nation_with_hidden_column WHERE regionkey < 5")
                .assertThat()
                .skippingTypesCheck()
                .matches("SELECT BIGINT '25'");
    }

    @Test
    public void testRowFilterOnOptionalColumn()
    {
        accessControl.reset();

        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG_MISSING_COLUMNS, "tiny", "nation_with_optional_column"),
                USER,
                ViewExpression.builder().expression("length(optional) > 2").build());

        assertions.query("INSERT INTO mockmissingcolumns.tiny.nation_with_optional_column(nationkey, name, regionkey, comment, optional) VALUES (0, 'POLAND', 0, 'No comment', 'some string')")
                .assertThat()
                .skippingTypesCheck()
                .matches("VALUES BIGINT '1'");

        assertThat(assertions.query("INSERT INTO mockmissingcolumns.tiny.nation_with_optional_column(nationkey, name, regionkey, comment, optional) VALUES (0, 'POLAND', 0, 'No comment', 'so')"))
                .failure()
                .hasErrorCode(PERMISSION_DENIED)
                .hasMessage("Access Denied: Cannot insert row that does not match a row filter");

        assertThat(assertions.query("INSERT INTO mockmissingcolumns.tiny.nation_with_optional_column(nationkey, name, regionkey, comment, optional) VALUES (0, 'POLAND', 0, 'No comment', null)"))
                .failure()
                .hasErrorCode(PERMISSION_DENIED)
                .hasMessage("Access Denied: Cannot insert row that does not match a row filter");
    }

    public static final class DeprecatedFunctions
    {
        private DeprecatedFunctions() {}

        @Deprecated
        @ScalarFunction("deprecated_identity")
        @SqlType(StandardTypes.BIGINT)
        public static long deprecatedIdentity(@SqlType(StandardTypes.BIGINT) long value)
        {
            return value;
        }
    }
}
