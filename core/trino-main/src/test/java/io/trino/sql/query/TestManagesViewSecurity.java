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
import io.trino.spi.connector.ConnectorViewDefinition;
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
public class TestManagesViewSecurity
{
    private static final String MANAGING_CATALOG = "managing";
    private static final String PLAIN_CATALOG = "plain";
    private static final String USER = "user";
    private static final String VIEW_OWNER = "view-owner";
    private static final String FILTER_OWNER = "filter-owner";

    private static final Session SESSION = testSessionBuilder()
            .setCatalog(MANAGING_CATALOG)
            .setSchema("default")
            .setIdentity(Identity.forUser(USER).build())
            .build();

    private final List<String> managingUsers = new CopyOnWriteArrayList<>();
    private final List<String> plainUsers = new CopyOnWriteArrayList<>();
    private final QueryAssertions assertions;
    private final TestingAccessControlManager accessControl;

    public TestManagesViewSecurity()
    {
        QueryRunner runner = new StandaloneQueryRunner(SESSION);

        ConnectorViewDefinition definerView = view(MANAGING_CATALOG);
        ConnectorViewDefinition plainDefinerView = view(PLAIN_CATALOG);

        runner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withName(MANAGING_CATALOG)
                .withManagesViewSecurity(true)
                .withSessionObserver(session -> managingUsers.add(session.getUser()))
                .withGetViews((_, _) -> ImmutableMap.of(new SchemaTableName("default", "definer_view"), definerView))
                .withGetColumns(_ -> TPCH_NATION_SCHEMA)
                .withData(_ -> TPCH_NATION_DATA)
                .build()));
        runner.createCatalog(MANAGING_CATALOG, MANAGING_CATALOG, ImmutableMap.of());

        runner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withName(PLAIN_CATALOG)
                .withSessionObserver(session -> plainUsers.add(session.getUser()))
                .withGetViews((_, _) -> ImmutableMap.of(new SchemaTableName("default", "definer_view"), plainDefinerView))
                .withGetColumns(_ -> TPCH_NATION_SCHEMA)
                .withData(_ -> TPCH_NATION_DATA)
                .build()));
        runner.createCatalog(PLAIN_CATALOG, PLAIN_CATALOG, ImmutableMap.of());

        assertions = new QueryAssertions(runner);
        accessControl = assertions.getQueryRunner().getAccessControl();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testManagingConnectorSeesInvokerInDefinerView()
    {
        assertThat(usersSeenBy(managingUsers, "SELECT * FROM managing.default.definer_view"))
                .isNotEmpty()
                .containsOnly(USER);
    }

    @Test
    public void testOtherConnectorSeesViewOwnerInDefinerView()
    {
        assertThat(usersSeenBy(plainUsers, "SELECT * FROM plain.default.definer_view"))
                .contains(VIEW_OWNER);
    }

    @Test
    public void testRowFilterKeepsItsSecurityIdentity()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MANAGING_CATALOG, "default", "t"),
                USER,
                ViewExpression.builder().identity(FILTER_OWNER).expression("nationkey IN (SELECT nationkey FROM managing.default.t2)").build());
        try {
            assertThat(usersSeenBy(managingUsers, "SELECT * FROM managing.default.t"))
                    .contains(USER, FILTER_OWNER);
        }
        finally {
            accessControl.reset();
        }
    }

    private List<String> usersSeenBy(List<String> users, String sql)
    {
        users.clear();
        assertions.execute(sql);
        return ImmutableList.copyOf(users);
    }

    private static ConnectorViewDefinition view(String catalogName)
    {
        return new ConnectorViewDefinition(
                "SELECT nationkey FROM %s.default.t".formatted(catalogName),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(new ConnectorViewDefinition.ViewColumn("nationkey", BigintType.BIGINT.getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.of(VIEW_OWNER),
                false,
                ImmutableList.of());
    }
}
