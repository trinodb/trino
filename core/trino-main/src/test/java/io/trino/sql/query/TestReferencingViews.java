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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.ReferencingView;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.BigintType;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import io.trino.testing.TestingAccessControlManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.trino.connector.MockConnectorEntities.TPCH_NATION_DATA;
import static io.trino.connector.MockConnectorEntities.TPCH_NATION_SCHEMA;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestReferencingViews
{
    private static final String MOCK_CATALOG = "mock";
    private static final String OTHER_CATALOG = "other";
    private static final String USER = "user";
    private static final String VIEW_OWNER = "view-owner";

    private static final Session SESSION = testSessionBuilder()
            .setCatalog(MOCK_CATALOG)
            .setSchema("default")
            .setIdentity(Identity.forUser(USER).build())
            .build();

    private record Load(String table, List<ReferencingView> referencingViews) {}

    private final List<Load> tableLoads = new CopyOnWriteArrayList<>();
    private final List<Load> viewLoads = new CopyOnWriteArrayList<>();
    private final QueryAssertions assertions;
    private final TestingAccessControlManager accessControl;

    public TestReferencingViews()
    {
        QueryRunner runner = new StandaloneQueryRunner(SESSION);

        // outer_view is DEFINER, inner_view is INVOKER, and outer_view reads from inner_view
        ConnectorViewDefinition outerView = view("SELECT nationkey FROM mock.default.inner_view", Optional.of(VIEW_OWNER));
        ConnectorViewDefinition innerView = view("SELECT nationkey FROM mock.default.t", Optional.empty());
        ConnectorViewDefinition crossCatalogView = view("SELECT nationkey FROM mock.default.t", Optional.of(VIEW_OWNER));
        ConnectorViewDefinition viewOverCrossCatalogView = view("SELECT nationkey FROM other.default.cross_view", Optional.empty());

        runner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withName(MOCK_CATALOG)
                .withGetViews((_, _) -> ImmutableMap.of(
                        new SchemaTableName("default", "outer_view"), outerView,
                        new SchemaTableName("default", "inner_view"), innerView,
                        new SchemaTableName("default", "view_over_cross_view"), viewOverCrossCatalogView))
                .withGetColumns(_ -> TPCH_NATION_SCHEMA)
                .withData(_ -> TPCH_NATION_DATA)
                .withGetTableHandleObserver((table, views) -> tableLoads.add(new Load(table.getTableName(), ImmutableList.copyOf(views))))
                .withGetViewObserver((view, views) -> viewLoads.add(new Load(view.getTableName(), ImmutableList.copyOf(views))))
                .build()));
        runner.createCatalog(MOCK_CATALOG, "mock", ImmutableMap.of());

        runner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withName(OTHER_CATALOG)
                .withGetViews((_, _) -> ImmutableMap.of(new SchemaTableName("default", "cross_view"), crossCatalogView))
                .build()));
        runner.createCatalog(OTHER_CATALOG, "other", ImmutableMap.of());

        assertions = new QueryAssertions(runner);
        accessControl = assertions.getQueryRunner().getAccessControl();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testDirectReference()
    {
        assertThat(tableLoadsFor("SELECT * FROM t")).containsExactly(new Load("t", ImmutableList.of()));
    }

    @Test
    public void testView()
    {
        assertThat(tableLoadsFor("SELECT * FROM inner_view")).containsExactly(new Load("t", ImmutableList.of(invoker("mock", "inner_view"))));
    }

    @Test
    public void testNestedViewsAreOutermostFirstRegardlessOfSecurityMode()
    {
        assertThat(tableLoadsFor("SELECT * FROM outer_view")).containsExactly(new Load("t", ImmutableList.of(definer("mock", "outer_view"), invoker("mock", "inner_view"))));
    }

    @Test
    public void testViewFromOtherCatalog()
    {
        // views from all catalogs are passed, the connector decides which ones it understands
        assertThat(tableLoadsFor("SELECT * FROM other.default.cross_view")).containsExactly(new Load("t", ImmutableList.of(definer("other", "cross_view"))));
        assertThat(tableLoadsFor("SELECT * FROM view_over_cross_view")).containsExactly(new Load("t", ImmutableList.of(invoker("mock", "view_over_cross_view"), definer("other", "cross_view"))));
    }

    @Test
    public void testViewLoadsCarryEnclosingViews()
    {
        assertThat(viewLoadsFor("SELECT * FROM outer_view")).contains(
                new Load("outer_view", ImmutableList.of()),
                new Load("inner_view", ImmutableList.of(definer("mock", "outer_view"))));
    }

    @Test
    public void testJoinOfViewsOverSameTable()
    {
        assertThat(tableLoadsFor("SELECT * FROM outer_view JOIN inner_view USING (nationkey)")).containsExactlyInAnyOrder(
                new Load("t", ImmutableList.of(definer("mock", "outer_view"), invoker("mock", "inner_view"))),
                new Load("t", ImmutableList.of(invoker("mock", "inner_view"))));
    }

    @Test
    public void testRowFilterTableIsReportedWithoutViews()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "default", "t"),
                USER,
                ViewExpression.builder().expression("nationkey IN (SELECT nationkey FROM mock.default.t2)").build());
        try {
            assertThat(tableLoadsFor("SELECT * FROM t")).containsExactlyInAnyOrder(
                    new Load("t", ImmutableList.of()),
                    new Load("t2", ImmutableList.of()));
        }
        finally {
            accessControl.reset();
        }
    }

    @Test
    public void testColumnMaskTableIsReportedWithoutViews()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(MOCK_CATALOG, "default", "t"),
                "nationkey",
                USER,
                ViewExpression.builder().expression("(SELECT max(nationkey) FROM mock.default.t2)").build());
        try {
            assertThat(tableLoadsFor("SELECT * FROM inner_view")).containsExactlyInAnyOrder(
                    new Load("t", ImmutableList.of(invoker("mock", "inner_view"))),
                    new Load("t2", ImmutableList.of()));
        }
        finally {
            accessControl.reset();
        }
    }

    private List<Load> tableLoadsFor(String sql)
    {
        tableLoads.clear();
        assertions.execute(sql);
        return ImmutableList.copyOf(tableLoads);
    }

    private List<Load> viewLoadsFor(String sql)
    {
        viewLoads.clear();
        assertions.execute(sql);
        return ImmutableList.copyOf(viewLoads);
    }

    private static ReferencingView definer(String catalog, String view)
    {
        return new ReferencingView(new CatalogSchemaTableName(catalog, "default", view), false);
    }

    private static ReferencingView invoker(String catalog, String view)
    {
        return new ReferencingView(new CatalogSchemaTableName(catalog, "default", view), true);
    }

    private static ConnectorViewDefinition view(String sql, Optional<String> owner)
    {
        return new ConnectorViewDefinition(
                sql,
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(new ConnectorViewDefinition.ViewColumn("nationkey", BigintType.BIGINT.getTypeId(), Optional.empty())),
                Optional.empty(),
                owner,
                owner.isEmpty(),
                ImmutableList.of());
    }
}
