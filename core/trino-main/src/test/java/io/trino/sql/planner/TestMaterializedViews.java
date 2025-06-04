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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.StaticConnectorFactory;
import io.trino.metadata.MaterializedViewDefinition;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.metadata.ViewColumn;
import io.trino.spi.RefreshType;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.OperatorType;
import io.trino.spi.security.Identity;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeParameter;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.testing.PlanTester;
import io.trino.testing.TestingAccessControlManager;
import io.trino.testing.TestingMetadata;
import io.trino.type.DateTimes;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.execution.warnings.WarningCollector.NOOP;
import static io.trino.spi.RefreshType.FULL;
import static io.trino.spi.RefreshType.INCREMENTAL;
import static io.trino.spi.connector.SaveMode.FAIL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampWithTimeZoneParametricType.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableWriter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingMetadata.STALE_MV_STALENESS;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMaterializedViews
        extends BasePlanTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));

    private static final String SCHEMA = "tiny";

    private final TestingMetadata testingConnectorMetadata = new TestingMetadata();

    @Override
    protected PlanTester createPlanTester()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema(SCHEMA)
                .setSystemProperty("task_concurrency", "1"); // these tests don't handle exchanges from local parallel

        PlanTester planTester = PlanTester.create(sessionBuilder.build());
        planTester.createCatalog(TEST_CATALOG_NAME, new StaticConnectorFactory("test", new TestMaterializedViewConnector(testingConnectorMetadata)), ImmutableMap.of());

        TypeManager typeManager = new TestingTypeManager();

        Metadata metadata = planTester.getPlannerContext().getMetadata();
        SchemaTableName testTable = new SchemaTableName(SCHEMA, "test_table");
        planTester.inTransaction(session -> {
            metadata.createTable(
                    session,
                    TEST_CATALOG_NAME,
                    new ConnectorTableMetadata(
                            testTable,
                            ImmutableList.of(
                                    new ColumnMetadata("a", BIGINT),
                                    new ColumnMetadata("b", BIGINT))),
                    FAIL);
            return null;
        });

        SchemaTableName storageTable = new SchemaTableName(SCHEMA, "storage_table");
        planTester.inTransaction(session -> {
            metadata.createTable(
                    session,
                    TEST_CATALOG_NAME,
                    new ConnectorTableMetadata(
                            storageTable,
                            ImmutableList.of(
                                    new ColumnMetadata("a", BIGINT),
                                    new ColumnMetadata("b", BIGINT))),
                    FAIL);
            return null;
        });

        SchemaTableName storageTableWithCasts = new SchemaTableName(SCHEMA, "storage_table_with_casts");
        planTester.inTransaction(session -> {
            metadata.createTable(
                    session,
                    TEST_CATALOG_NAME,
                    new ConnectorTableMetadata(
                            storageTableWithCasts,
                            ImmutableList.of(
                                    new ColumnMetadata("a", TINYINT),
                                    new ColumnMetadata("b", VARCHAR))),
                    FAIL);
            return null;
        });

        Type timestampWithTimezone3 = TIMESTAMP_WITH_TIME_ZONE.createType(typeManager, ImmutableList.of(TypeParameter.of(3)));
        SchemaTableName timestampTest = new SchemaTableName(SCHEMA, "timestamp_test");
        planTester.inTransaction(session -> {
            metadata.createTable(
                    session,
                    TEST_CATALOG_NAME,
                    new ConnectorTableMetadata(
                            timestampTest,
                            ImmutableList.of(
                                    new ColumnMetadata("id", BIGINT),
                                    new ColumnMetadata("ts", timestampWithTimezone3))),
                    FAIL);
            return null;
        });

        SchemaTableName timestampTestStorage = new SchemaTableName(SCHEMA, "timestamp_test_storage");
        planTester.inTransaction(session -> {
            metadata.createTable(
                    session,
                    TEST_CATALOG_NAME,
                    new ConnectorTableMetadata(
                            timestampTestStorage,
                            ImmutableList.of(
                                    new ColumnMetadata("id", BIGINT),
                                    new ColumnMetadata("ts", VARCHAR))),
                    FAIL);
            return null;
        });

        QualifiedObjectName freshMaterializedView = new QualifiedObjectName(TEST_CATALOG_NAME, SCHEMA, "fresh_materialized_view");
        MaterializedViewDefinition materializedViewDefinition = new MaterializedViewDefinition(
                "SELECT a, b FROM test_table",
                Optional.of(TEST_CATALOG_NAME),
                Optional.of(SCHEMA),
                ImmutableList.of(new ViewColumn("a", BIGINT.getTypeId(), Optional.empty()), new ViewColumn("b", BIGINT.getTypeId(), Optional.empty())),
                Optional.of(STALE_MV_STALENESS.plusHours(1)),
                Optional.empty(),
                Identity.ofUser("some user"),
                ImmutableList.of(),
                Optional.of(new CatalogSchemaTableName(TEST_CATALOG_NAME, SCHEMA, "storage_table")));
        planTester.inTransaction(session -> {
            metadata.createMaterializedView(
                    session,
                    freshMaterializedView,
                    materializedViewDefinition,
                    ImmutableMap.of(),
                    false,
                    false);
            return null;
        });
        testingConnectorMetadata.markMaterializedViewIsFresh(freshMaterializedView.asSchemaTableName());

        QualifiedObjectName notFreshMaterializedView = new QualifiedObjectName(TEST_CATALOG_NAME, SCHEMA, "not_fresh_materialized_view");
        planTester.inTransaction(session -> {
            metadata.createMaterializedView(
                    session,
                    notFreshMaterializedView,
                    materializedViewDefinition,
                    ImmutableMap.of(),
                    false,
                    false);
            return null;
        });

        MaterializedViewDefinition materializedViewDefinitionWithCasts = new MaterializedViewDefinition(
                "SELECT a, b FROM test_table",
                Optional.of(TEST_CATALOG_NAME),
                Optional.of(SCHEMA),
                ImmutableList.of(new ViewColumn("a", BIGINT.getTypeId(), Optional.empty()), new ViewColumn("b", BIGINT.getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                Identity.ofUser("some user"),
                ImmutableList.of(),
                Optional.of(new CatalogSchemaTableName(TEST_CATALOG_NAME, SCHEMA, "storage_table_with_casts")));
        QualifiedObjectName materializedViewWithCasts = new QualifiedObjectName(TEST_CATALOG_NAME, SCHEMA, "materialized_view_with_casts");
        planTester.inTransaction(session -> {
            metadata.createMaterializedView(
                    session,
                    materializedViewWithCasts,
                    materializedViewDefinitionWithCasts,
                    ImmutableMap.of(),
                    false,
                    false);
            return null;
        });
        testingConnectorMetadata.markMaterializedViewIsFresh(materializedViewWithCasts.asSchemaTableName());

        planTester.inTransaction(session -> {
            metadata.createMaterializedView(
                    session,
                    new QualifiedObjectName(TEST_CATALOG_NAME, SCHEMA, "stale_materialized_view_with_casts"),
                    materializedViewDefinitionWithCasts,
                    ImmutableMap.of(),
                    false,
                    false);
            return null;
        });

        MaterializedViewDefinition materializedViewDefinitionWithTimestamp = new MaterializedViewDefinition(
                "SELECT id, ts FROM timestamp_test",
                Optional.of(TEST_CATALOG_NAME),
                Optional.of(SCHEMA),
                ImmutableList.of(new ViewColumn("id", BIGINT.getTypeId(), Optional.empty()), new ViewColumn("ts", timestampWithTimezone3.getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                Identity.ofUser("some user"),
                ImmutableList.of(),
                Optional.of(new CatalogSchemaTableName(TEST_CATALOG_NAME, SCHEMA, "timestamp_test_storage")));
        QualifiedObjectName materializedViewWithTimestamp = new QualifiedObjectName(TEST_CATALOG_NAME, SCHEMA, "timestamp_mv_test");
        planTester.inTransaction(session -> {
            metadata.createMaterializedView(
                    session,
                    materializedViewWithTimestamp,
                    materializedViewDefinitionWithTimestamp,
                    ImmutableMap.of(),
                    false,
                    false);
            return null;
        });

        testingConnectorMetadata.markMaterializedViewIsFresh(materializedViewWithTimestamp.asSchemaTableName());

        return planTester;
    }

    private void createMaterializedView(String materializedViewName, String query)
    {
        Metadata metadata = getPlanTester().getPlannerContext().getMetadata();
        QualifiedObjectName matViewName = new QualifiedObjectName(TEST_CATALOG_NAME, SCHEMA, materializedViewName);
        MaterializedViewDefinition matViewDefinition = new MaterializedViewDefinition(
                query,
                Optional.of(TEST_CATALOG_NAME),
                Optional.of(SCHEMA),
                ImmutableList.of(new ViewColumn("a", BIGINT.getTypeId(), Optional.empty()), new ViewColumn("b", BIGINT.getTypeId(), Optional.empty())),
                Optional.of(STALE_MV_STALENESS.plusHours(1)),
                Optional.empty(),
                Identity.ofUser("some user"),
                ImmutableList.of(),
                Optional.of(new CatalogSchemaTableName(TEST_CATALOG_NAME, SCHEMA, "storage_table")));
        getPlanTester().inTransaction(session -> {
            metadata.createMaterializedView(
                    session,
                    matViewName,
                    matViewDefinition,
                    ImmutableMap.of(),
                    false,
                    false);
            return null;
        });
    }

    @Test
    public void testFreshMaterializedView()
    {
        assertPlan("SELECT * FROM fresh_materialized_view",
                anyTree(
                        tableScan("storage_table")));
    }

    @Test
    public void testNotFreshMaterializedView()
    {
        Session defaultSession = getPlanTester().getDefaultSession();
        Session futureSession = Session.builder(defaultSession)
                .setStart(Instant.now().plus(1, ChronoUnit.DAYS))
                .build();

        assertPlan(
                "SELECT * FROM not_fresh_materialized_view",
                defaultSession,
                anyTree(
                        tableScan("storage_table")));

        assertPlan(
                "SELECT * FROM not_fresh_materialized_view",
                futureSession,
                anyTree(
                        tableScan("test_table")));
    }

    @Test
    public void testRefreshTypes()
    {
        createMaterializedView("simple_materialized_view", "SELECT a as new_name, b FROM test_table WHERE a is not null and b > 1");
        Optional<RefreshType> refreshType = getRefreshType("simple_materialized_view");
        assertThat(refreshType).isPresent();
        assertThat(refreshType.get()).isEqualTo(INCREMENTAL);

        createMaterializedView("aggregation_materialized_view", "SELECT a, count(*) FROM test_table GROUP BY a");
        refreshType = getRefreshType("aggregation_materialized_view");
        assertThat(refreshType).isPresent();
        assertThat(refreshType.get()).isEqualTo(FULL);

        createMaterializedView("join_materialized_view", "SELECT a.a, b.b FROM test_table a JOIN test_table b on a.a = b.a");
        refreshType = getRefreshType("join_materialized_view");
        assertThat(refreshType).isPresent();
        assertThat(refreshType.get()).isEqualTo(FULL);

        createMaterializedView("distinct_materialized_view", "SELECT distinct a, b FROM test_table");
        refreshType = getRefreshType("distinct_materialized_view");
        assertThat(refreshType).isPresent();
        assertThat(refreshType.get()).isEqualTo(FULL);

        createMaterializedView("table_subquery_materialized_view", "SELECT a, b FROM (SELECT a, b FROM (VALUES (1, 2), (3, 4)) t(a, b))");
        refreshType = getRefreshType("table_subquery_materialized_view");
        assertThat(refreshType).isPresent();
        assertThat(refreshType.get()).isEqualTo(FULL);

        createMaterializedView("where_subquery_materialized_view", "SELECT a, b FROM test_table WHERE b in (SELECT b FROM test_table WHERE a < 0)");
        refreshType = getRefreshType("where_subquery_materialized_view");
        assertThat(refreshType).isPresent();
        assertThat(refreshType.get()).isEqualTo(FULL);

        createMaterializedView("union_view", "SELECT a, b FROM test_table a WHERE a.b in (6, 9) UNION ALL SELECT a, b FROM test_table b WHERE b.b in (1, 5)");
        refreshType = getRefreshType("union_view");
        assertThat(refreshType).isPresent();
        assertThat(refreshType.get()).isEqualTo(FULL);
    }

    @Test
    public void testMaterializedViewWithCasts()
    {
        TestingAccessControlManager accessControl = getPlanTester().getAccessControl();
        accessControl.columnMask(
                new QualifiedObjectName(TEST_CATALOG_NAME, SCHEMA, "materialized_view_with_casts"),
                "a",
                "user",
                ViewExpression.builder().expression("a + 1").build());
        assertPlan("SELECT * FROM materialized_view_with_casts",
                anyTree(
                        project(
                                ImmutableMap.of(
                                        "A_CAST", expression(new Call(ADD_BIGINT, ImmutableList.of(new Cast(new Reference(BIGINT, "A"), BIGINT), new Constant(BIGINT, 1L)))),
                                        "B_CAST", expression(new Cast(new Reference(BIGINT, "B"), BIGINT))),
                                tableScan("storage_table_with_casts", ImmutableMap.of("A", "a", "B", "b")))));
    }

    @Test
    public void testRefreshMaterializedViewWithCasts()
    {
        assertPlan("REFRESH MATERIALIZED VIEW stale_materialized_view_with_casts",
                anyTree(
                        tableWriter(List.of("A_CAST", "B_CAST"), List.of("a", "b"),
                                exchange(LOCAL,
                                        project(Map.of(
                                                        "A_CAST", expression(new Cast(new Reference(BIGINT, "A"), TINYINT)),
                                                        "B_CAST", expression(new Cast(new Reference(BIGINT, "B"), VARCHAR))),
                                                tableScan("test_table", Map.of("A", "a", "B", "b")))))));

        // No-op REFRESH
        assertPlan("REFRESH MATERIALIZED VIEW materialized_view_with_casts",
                output(
                        values(List.of("rows"), List.of(List.of(new Constant(BIGINT, 0L))))));
    }

    @Test
    public void testMaterializedViewWithTimestamp()
    {
        assertPlan("SELECT * FROM timestamp_mv_test WHERE ts < TIMESTAMP '2024-01-01 00:00:00.000 America/New_York'",
                anyTree(
                        project(ImmutableMap.of("ts_0", expression(new Cast(new Reference(TIMESTAMP_TZ_MILLIS, "ts"), TIMESTAMP_TZ_MILLIS))),
                                filter(
                                        new Comparison(LESS_THAN, new Cast(new Reference(TIMESTAMP_TZ_MILLIS, "ts"), TIMESTAMP_TZ_MILLIS), new Constant(createTimestampWithTimeZoneType(3), DateTimes.parseTimestampWithTimeZone(3, "2024-01-01 00:00:00.000 America/New_York"))),
                                        tableScan("timestamp_test_storage", ImmutableMap.of("ts", "ts", "id", "id"))))));
    }

    private Optional<RefreshType> getRefreshType(String matViewTable)
    {
        PlanTester planTester = getPlanTester();
        Session session = planTester.getDefaultSession();

        String queryId = planTester.inTransaction(session, transactionSession -> {
            planTester.createPlan(
                    transactionSession,
                    "REFRESH MATERIALIZED VIEW " + matViewTable,
                    planTester.getPlanOptimizers(true),
                    OPTIMIZED_AND_VALIDATED,
                    NOOP,
                    createPlanOptimizersStatsCollector());
            return transactionSession.getQueryId().toString();
        });

        return testingConnectorMetadata.getRefreshType(queryId);
    }

    private static class TestMaterializedViewConnector
            implements Connector
    {
        private final ConnectorMetadata metadata;

        public TestMaterializedViewConnector(ConnectorMetadata metadata)
        {
            this.metadata = metadata;
        }

        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
        {
            return new ConnectorTransactionHandle() {};
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction)
        {
            return metadata;
        }
    }
}
