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
import io.trino.SystemSessionProperties;
import io.trino.connector.StaticConnectorFactory;
import io.trino.metadata.MaterializedViewDefinition;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ViewColumn;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeParameter;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.PlanTester;
import io.trino.testing.TestingAccessControlManager;
import io.trino.testing.TestingMetadata;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.connector.SaveMode.FAIL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampWithTimeZoneParametricType.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.dataType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableWriter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingMetadata.STALE_MV_STALENESS;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestMaterializedViews
        extends BasePlanTest
{
    private static final String SCHEMA = "tiny";

    @Override
    protected PlanTester createPlanTester()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema(SCHEMA)
                .setSystemProperty("task_concurrency", "1"); // these tests don't handle exchanges from local parallel

        TestingMetadata testingConnectorMetadata = new TestingMetadata();

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
        Session legacyGracePeriod = Session.builder(defaultSession)
                .setSystemProperty(SystemSessionProperties.LEGACY_MATERIALIZED_VIEW_GRACE_PERIOD, "true")
                .build();
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
                legacyGracePeriod,
                anyTree(
                        tableScan("test_table")));
        assertPlan(
                "SELECT * FROM not_fresh_materialized_view",
                futureSession,
                anyTree(
                        tableScan("test_table")));
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
                                        "A_CAST", expression(new ArithmeticBinaryExpression(ADD, new Cast(new SymbolReference("A"), dataType("bigint")), new GenericLiteral("BIGINT", "1"))),
                                        "B_CAST", expression(new Cast(new SymbolReference("B"), dataType("bigint")))),
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
                                                        "A_CAST", expression(new Cast(new SymbolReference("A"), dataType("tinyint"))),
                                                        "B_CAST", expression(new Cast(new SymbolReference("B"), dataType("varchar")))),
                                                tableScan("test_table", Map.of("A", "a", "B", "b")))))));

        // No-op REFRESH
        assertPlan("REFRESH MATERIALIZED VIEW materialized_view_with_casts",
                output(
                        values(List.of("rows"), List.of(List.of(new GenericLiteral("BIGINT", "0"))))));
    }

    @Test
    public void testMaterializedViewWithTimestamp()
    {
        assertPlan("SELECT * FROM timestamp_mv_test WHERE ts < TIMESTAMP '2024-01-01 00:00:00.000 America/New_York'",
                anyTree(
                        project(ImmutableMap.of("ts_0", expression(new Cast(new SymbolReference("ts"), dataType("timestamp(3) with time zone")))),
                                filter(
                                        new ComparisonExpression(LESS_THAN, new Cast(new SymbolReference("ts"), dataType("timestamp(3) with time zone")), new GenericLiteral("TIMESTAMP", "2024-01-01 00:00:00.000 America/New_York")),
                                        tableScan("timestamp_test_storage", ImmutableMap.of("ts", "ts", "id", "id"))))));
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
