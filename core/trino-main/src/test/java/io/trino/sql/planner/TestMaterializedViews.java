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
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.tree.GenericLiteral;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.TestingAccessControlManager;
import io.trino.testing.TestingMetadata;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableWriter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingMetadata.STALE_MV_STALENESS;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestMaterializedViews
        extends BasePlanTest
{
    private static final String SCHEMA = "tiny";

    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema(SCHEMA)
                .setSystemProperty("task_concurrency", "1"); // these tests don't handle exchanges from local parallel

        TestingMetadata testingConnectorMetadata = new TestingMetadata();

        LocalQueryRunner queryRunner = LocalQueryRunner.create(sessionBuilder.build());
        queryRunner.createCatalog(TEST_CATALOG_NAME, new StaticConnectorFactory("test", new TestMaterializedViewConnector(testingConnectorMetadata)), ImmutableMap.of());

        Metadata metadata = queryRunner.getMetadata();
        SchemaTableName testTable = new SchemaTableName(SCHEMA, "test_table");
        queryRunner.inTransaction(session -> {
            metadata.createTable(
                    session,
                    TEST_CATALOG_NAME,
                    new ConnectorTableMetadata(
                            testTable,
                            ImmutableList.of(
                                    new ColumnMetadata("a", BIGINT),
                                    new ColumnMetadata("b", BIGINT))),
                    false);
            return null;
        });

        SchemaTableName storageTable = new SchemaTableName(SCHEMA, "storage_table");
        queryRunner.inTransaction(session -> {
            metadata.createTable(
                    session,
                    TEST_CATALOG_NAME,
                    new ConnectorTableMetadata(
                            storageTable,
                            ImmutableList.of(
                                    new ColumnMetadata("a", BIGINT),
                                    new ColumnMetadata("b", BIGINT))),
                    false);
            return null;
        });

        SchemaTableName storageTableWithCasts = new SchemaTableName(SCHEMA, "storage_table_with_casts");
        queryRunner.inTransaction(session -> {
            metadata.createTable(
                    session,
                    TEST_CATALOG_NAME,
                    new ConnectorTableMetadata(
                            storageTableWithCasts,
                            ImmutableList.of(
                                    new ColumnMetadata("a", TINYINT),
                                    new ColumnMetadata("b", VARCHAR))),
                    false);
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
                Optional.of(new CatalogSchemaTableName(TEST_CATALOG_NAME, SCHEMA, "storage_table")),
                ImmutableMap.of());
        queryRunner.inTransaction(session -> {
            metadata.createMaterializedView(
                    session,
                    freshMaterializedView,
                    materializedViewDefinition,
                    false,
                    false);
            return null;
        });
        testingConnectorMetadata.markMaterializedViewIsFresh(freshMaterializedView.asSchemaTableName());

        QualifiedObjectName notFreshMaterializedView = new QualifiedObjectName(TEST_CATALOG_NAME, SCHEMA, "not_fresh_materialized_view");
        queryRunner.inTransaction(session -> {
            metadata.createMaterializedView(
                    session,
                    notFreshMaterializedView,
                    materializedViewDefinition,
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
                Optional.of(new CatalogSchemaTableName(TEST_CATALOG_NAME, SCHEMA, "storage_table_with_casts")),
                ImmutableMap.of());
        QualifiedObjectName materializedViewWithCasts = new QualifiedObjectName(TEST_CATALOG_NAME, SCHEMA, "materialized_view_with_casts");
        queryRunner.inTransaction(session -> {
            metadata.createMaterializedView(
                    session,
                    materializedViewWithCasts,
                    materializedViewDefinitionWithCasts,
                    false,
                    false);
            return null;
        });
        testingConnectorMetadata.markMaterializedViewIsFresh(materializedViewWithCasts.asSchemaTableName());

        queryRunner.inTransaction(session -> {
            metadata.createMaterializedView(
                    session,
                    new QualifiedObjectName(TEST_CATALOG_NAME, SCHEMA, "stale_materialized_view_with_casts"),
                    materializedViewDefinitionWithCasts,
                    false,
                    false);
            return null;
        });

        return queryRunner;
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
        Session defaultSession = getQueryRunner().getDefaultSession();
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
        TestingAccessControlManager accessControl = getQueryRunner().getAccessControl();
        accessControl.columnMask(
                new QualifiedObjectName(TEST_CATALOG_NAME, SCHEMA, "materialized_view_with_casts"),
                "a",
                "user",
                new ViewExpression(Optional.empty(), Optional.empty(), Optional.empty(), "a + 1"));
        assertPlan("SELECT * FROM materialized_view_with_casts",
                anyTree(
                        project(
                                ImmutableMap.of(
                                        "A_CAST", expression("CAST(A as BIGINT) + BIGINT '1'"),
                                        "B_CAST", expression("CAST(B as BIGINT)")),
                                tableScan("storage_table_with_casts", ImmutableMap.of("A", "a", "B", "b")))));
    }

    @Test
    public void testRefreshMaterializedViewWithCasts()
    {
        assertPlan("REFRESH MATERIALIZED VIEW stale_materialized_view_with_casts",
                anyTree(
                        tableWriter(List.of("A_CAST", "B_CAST"), List.of("a", "b"),
                                exchange(LOCAL,
                                        project(Map.of("A_CAST", expression("CAST(A AS tinyint)"), "B_CAST", expression("CAST(B AS varchar)")),
                                                tableScan("test_table", Map.of("A", "a", "B", "b")))))));

        // No-op REFRESH
        assertPlan("REFRESH MATERIALIZED VIEW materialized_view_with_casts",
                output(
                        values(List.of("rows"), List.of(List.of(new GenericLiteral("BIGINT", "0"))))));
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
