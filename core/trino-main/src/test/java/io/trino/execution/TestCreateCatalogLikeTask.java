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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.trino.client.NodeVersion;
import io.trino.execution.warnings.WarningCollector;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.TrinoException;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.tree.CreateCatalog;
import io.trino.sql.tree.CreateCatalogLike;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSession;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestCreateCatalogLikeTask
{
    private static final String CONNECTOR_NAME = "tpch";

    private QueryRunner queryRunner;

    @BeforeEach
    public void setUp()
    {
        queryRunner = new StandaloneQueryRunner(TEST_SESSION);
        queryRunner.installPlugin(new TpchPlugin());
    }

    @AfterEach
    public void tearDown()
    {
        try (QueryRunner ignored = queryRunner) {
            queryRunner = null;
        }
    }

    @Test
    public void testCreateCatalogLikeNonExisting()
    {
        String catalogA = "catalog_a_" + randomNameSuffix();
        String catalogB = "catalog_b_" + randomNameSuffix();
        assertThat(catalogExists(catalogA)).isFalse();
        assertThat(catalogExists(catalogB)).isFalse();
        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(() -> executeCreateCatalogLike(catalogA, catalogB, false, ImmutableList.of()))
                .withMessage("Catalog '%s' does not exist", catalogA);
    }

    @Test
    public void testCreateCatalogLikeTheSameName()
    {
        String catalog = "catalog_" + randomNameSuffix();
        executeCreateCatalog(catalog, ImmutableList.of());
        assertThat(catalogExists(catalog)).isTrue();
        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(() -> executeCreateCatalogLike(catalog, catalog, false, ImmutableList.of()))
                .withMessage("Catalog '%s' already exists", catalog);
    }

    @Test
    public void testCreateCatalogLikeIfNotExists()
    {
        String catalogA = "catalog_a_" + randomNameSuffix();
        String catalogB = "catalog_b_" + randomNameSuffix();
        executeCreateCatalog(catalogA, ImmutableList.of());
        assertThat(catalogExists(catalogA)).isTrue();
        executeCreateCatalogLike(catalogA, catalogB, false, ImmutableList.of());
        assertThat(catalogExists(catalogB)).isTrue();
        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(() -> executeCreateCatalogLike(catalogA, catalogB, false, ImmutableList.of()))
                .withMessage("Catalog '%s' already exists", catalogB);
        executeCreateCatalogLike(catalogA, catalogB, true, ImmutableList.of());
        assertThat(catalogExists(catalogB)).isTrue();
    }

    @Test
    public void testCreateCatalogLikeWithoutChangingProperties()
    {
        testCreateCatalogLike(
                ImmutableList.of(
                        new Property(new Identifier("property0"), new StringLiteral("value0")),
                        new Property(new Identifier("property1"), new StringLiteral("value1"))),
                """
                           property0 = 'value0',
                           property1 = 'value1'
                        """,
                ImmutableList.of(),
                """
                           property0 = 'value0',
                           property1 = 'value1'
                        """);
    }

    @Test
    public void testAddCatalogProperties()
    {
        testCreateCatalogLike(
                ImmutableList.of(
                        new Property(new Identifier("property1"), new StringLiteral("value1"))),
                """
                           property1 = 'value1'
                        """,
                ImmutableList.of(
                        new Property(new Identifier("property0"), new StringLiteral("value0")),
                        new Property(new Identifier("property2"), new StringLiteral("value2"))),
                """
                           property0 = 'value0',
                           property1 = 'value1',
                           property2 = 'value2'
                        """);
    }

    @Test
    public void testOverrideCatalogProperties()
    {
        testCreateCatalogLike(
                ImmutableList.of(
                        new Property(new Identifier("property0"), new StringLiteral("value0")),
                        new Property(new Identifier("property1"), new StringLiteral("value1"))),
                """
                           property0 = 'value0',
                           property1 = 'value1'
                        """,
                ImmutableList.of(
                        new Property(new Identifier("property0"), new StringLiteral("value000")),
                        new Property(new Identifier("property1"), new StringLiteral("value111"))),
                """
                           property0 = 'value000',
                           property1 = 'value111'
                        """);
    }

    @Test
    public void testRemoveCatalogProperties()
    {
        testCreateCatalogLike(
                ImmutableList.of(
                        new Property(new Identifier("property0"), new StringLiteral("value0")),
                        new Property(new Identifier("property1"), new StringLiteral("value1")),
                        new Property(new Identifier("property2"), new StringLiteral("value2"))),
                """
                           property0 = 'value0',
                           property1 = 'value1',
                           property2 = 'value2'
                        """,
                ImmutableList.of(
                        new Property(new Identifier("property2")),
                        new Property(new Identifier("property0"))),
                """
                           property1 = 'value1'
                        """);
    }

    @Test
    public void testSetComplexCatalogProperties()
    {
        testCreateCatalogLike(
                ImmutableList.of(
                        new Property(new Identifier("property1-to-remove"), new StringLiteral("value-to-remove")),
                        new Property(new Identifier("property2"), new StringLiteral("value not changed")),
                        new Property(new Identifier("property3-to-update"), new StringLiteral("value-old"))),
                """
                           "property1-to-remove" = 'value-to-remove',
                           property2 = 'value not changed',
                           "property3-to-update" = 'value-old'
                        """,
                ImmutableList.of(
                        new Property(new Identifier("property0-added"), new StringLiteral("value-added")),
                        new Property(new Identifier("property1-to-remove")),
                        new Property(new Identifier("property3-to-update"), new StringLiteral("value-updated")),
                        new Property(new Identifier("property4-added"), new StringLiteral("${ENV:foo}")),
                        new Property(new Identifier("property5-added"), new StringLiteral("${FILE:bar}"))),
                """
                           "property0-added" = 'value-added',
                           property2 = 'value not changed',
                           "property3-to-update" = 'value-updated',
                           "property4-added" = '${ENV:foo}',
                           "property5-added" = '${FILE:bar}'
                        """);
    }

    private void testCreateCatalogLike(ImmutableList<Property> initialProperties, String showInitialProperties, List<Property> updatedProperties, String showExpectedProperties)
    {
        String createCatalogSql = """
                CREATE CATALOG %s USING %s
                WITH (
                %s)""";

        String oldCatalog = "catalog_" + randomNameSuffix();
        executeCreateCatalog(oldCatalog, initialProperties);
        assertThat(catalogExists(oldCatalog)).isTrue();
        assertThat((String) queryRunner.execute("SHOW CREATE CATALOG " + oldCatalog).getOnlyValue())
                .isEqualTo(createCatalogSql.formatted(oldCatalog, CONNECTOR_NAME, showInitialProperties));

        String catalog = "catalog_" + randomNameSuffix();
        executeCreateCatalogLike(oldCatalog, catalog, false, updatedProperties);
        assertThat(catalogExists(catalog)).isTrue();
        assertThat((String) queryRunner.execute("SHOW CREATE CATALOG " + catalog).getOnlyValue())
                .isEqualTo(createCatalogSql.formatted(catalog, CONNECTOR_NAME, showExpectedProperties));
    }

    private void executeCreateCatalog(String catalogA, List<Property> catalogProperties)
    {
        Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks = queryRunner.getCoordinator().getInstance(Key.get(new TypeLiteral<>() {}));
        CreateCatalogTask task = (CreateCatalogTask) tasks.get(CreateCatalog.class);
        CreateCatalog statement = new CreateCatalog(new Identifier(catalogA), false, new Identifier(CONNECTOR_NAME), catalogProperties, Optional.empty(), Optional.empty());
        ListenableFuture<Void> future = task.execute(statement, createNewQuery(), emptyList(), WarningCollector.NOOP);
        getFutureValue(future);
    }

    private void executeCreateCatalogLike(String oldCatalog, String catalog, boolean notExists, List<Property> properties)
    {
        Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks = queryRunner.getCoordinator().getInstance(Key.get(new TypeLiteral<>() {}));
        CreateCatalogLikeTask task = (CreateCatalogLikeTask) tasks.get(CreateCatalogLike.class);
        CreateCatalogLike statement = new CreateCatalogLike(new Identifier(oldCatalog), new Identifier(catalog), notExists, properties);
        ListenableFuture<Void> future = task.execute(statement, createNewQuery(), emptyList(), WarningCollector.NOOP);
        getFutureValue(future);
    }

    private boolean catalogExists(String catalogB)
    {
        return queryRunner.getPlannerContext().getMetadata().catalogExists(createNewQuery().getSession(), catalogB);
    }

    private QueryStateMachine createNewQuery()
    {
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
                new NodeVersion("test"));
    }
}
