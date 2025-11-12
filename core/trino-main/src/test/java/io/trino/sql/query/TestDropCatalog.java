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

import io.airlift.units.Duration;
import io.trino.client.NodeVersion;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.execution.QueryStateMachine;
import io.trino.execution.warnings.WarningCollector;
import io.trino.spi.TrinoException;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.testing.TestingSession.testSession;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestDropCatalog
{
    private QueryAssertions queryAssertions;

    @BeforeAll
    public void setUp()
    {
        Duration catalogPruneInterval = new Duration(1, SECONDS); // lowest allowed
        QueryRunner queryRunner = new StandaloneQueryRunner(TEST_SESSION,
                server -> server.addProperty("catalog.prune.update-interval", catalogPruneInterval.toString()));
        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withName("connector_with_cleanup_query")
                .withSessionProperty(stringProperty(
                        "baz",
                        "test property",
                        null,
                        false))
                .withGetTableHandle((_, name) -> switch (name.toString()) {
                    case "default.existing" -> new MockConnectorTableHandle(name);
                    default -> {
                        throw new TrinoException(NOT_FOUND, "Table not found: " + name);
                    }
                })
                .withCleanupQuery(session -> {
                    // Increase chances of a bad interleaving of threads for the test to be rather deterministic
                    try {
                        Thread.sleep(catalogPruneInterval.toMillis());
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                    // simulate cleanupQuery that checks session state
                    session.getProperty("baz", String.class);
                })
                .build()));
        this.queryAssertions = new QueryAssertions(queryRunner);
    }

    @AfterAll
    public void tearDown()
    {
        if (queryAssertions != null) {
            queryAssertions.close();
        }
        queryAssertions = null;
    }

    @Test
    void testDropCatalogWithCleanupQuery()
    {
        QueryRunner queryRunner = queryAssertions.getQueryRunner();
        String catalogName = "catalog_with_cleanup_query";
        queryRunner.createCatalog(catalogName, "connector_with_cleanup_query");
        assertCatalogExists(catalogName);

        queryRunner.execute("DROP CATALOG " + catalogName);

        assertCatalogDoesNotExist(catalogName);
    }

    private void assertCatalogExists(String catalogName)
    {
        QueryRunner queryRunner = queryAssertions.getQueryRunner();

        QueryStateMachine fakeQuery = createNewQuery();
        assertThat(queryRunner.getPlannerContext().getMetadata().catalogExists(fakeQuery.getSession(), catalogName)).isTrue();

        assertThat(queryAssertions.query("SHOW SCHEMAS FROM " + catalogName))
                .containsAll("VALUES VARCHAR 'information_schema'");

        assertThat(queryAssertions.query("SELECT * FROM " + catalogName + ".default.existing"))
                .returnsEmptyResult();
    }

    private void assertCatalogDoesNotExist(String catalogName)
    {
        QueryRunner queryRunner = queryAssertions.getQueryRunner();

        QueryStateMachine fakeQuery = createNewQuery();
        assertThat(queryRunner.getPlannerContext().getMetadata().catalogExists(fakeQuery.getSession(), catalogName)).isFalse();

        assertThat(queryAssertions.query("SHOW SCHEMAS FROM " + catalogName))
                .failure().hasMessage("Catalog '%s' not found".formatted(catalogName));

        assertThat(queryAssertions.query("SELECT * FROM " + catalogName + ".default.existing"))
                .failure().hasMessageContaining("Catalog '%s' not found".formatted(catalogName));
    }

    private QueryStateMachine createNewQuery()
    {
        QueryRunner queryRunner = queryAssertions.getQueryRunner();
        return QueryStateMachine.begin(
                Optional.empty(),
                "test",
                Optional.empty(),
                testSession(queryRunner.getDefaultSession()),
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                false,
                queryRunner.getTransactionManager(),
                queryRunner.getAccessControl(),
                directExecutor(),
                queryRunner.getPlannerContext().getMetadata(),
                WarningCollector.NOOP,
                createPlanOptimizersStatsCollector(),
                Optional.empty(),
                true,
                Optional.empty(),
                new NodeVersion("test"));
    }
}
