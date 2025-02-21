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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.trino.client.NodeVersion;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.CatalogManager;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.tree.CreateCatalog;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.SetCatalogProperties;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;

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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestSetCatalogPropertiesTask
{
    private static final String CONNECTOR_NAME = "tpch";

    @Test
    void testAddCatalogProperties()
    {
        testSetProperties(
                ImmutableList.of(
                        new Property(new NodeLocation(1, 1), new Identifier("tpch.column-naming"), new StringLiteral(new NodeLocation(1, 30), "standard"))),
                ImmutableMap.of("tpch.column-naming", "standard"),
                ImmutableList.of(
                        new Property(new NodeLocation(1, 1), new Identifier("tpch.double-type-mapping"), new StringLiteral(new NodeLocation(1, 30), "double")),
                        new Property(new NodeLocation(2, 1), new Identifier("tpch.produce-pages"), new StringLiteral(new NodeLocation(2, 30), "true"))),
                ImmutableMap.of("tpch.double-type-mapping", "double", "tpch.column-naming", "standard", "tpch.produce-pages", "true"));
    }

    @Test
    void testAddDuplicatedCatalogProperties()
    {
        testSetProperties(
                ImmutableList.of(
                        new Property(new NodeLocation(1, 1), new Identifier("tpch.column-naming"), new StringLiteral(new NodeLocation(1, 30), "standard"))),
                ImmutableMap.of("tpch.column-naming", "standard"),
                ImmutableList.of(
                        new Property(new NodeLocation(1, 1), new Identifier("tpch.double-type-mapping"), new StringLiteral(new NodeLocation(1, 30), "double")),
                        new Property(new NodeLocation(2, 1), new Identifier("tpch.double-type-mapping"), new StringLiteral(new NodeLocation(2, 30), "decimal"))),
                ImmutableMap.of("tpch.column-naming", "standard", "tpch.double-type-mapping", "decimal"));
    }

    @Test
    void testOverrideCatalogProperties()
    {
        testSetProperties(
                ImmutableList.of(
                        new Property(new NodeLocation(1, 1), new Identifier("tpch.column-naming"), new StringLiteral(new NodeLocation(1, 30), "standard")),
                        new Property(new NodeLocation(2, 1), new Identifier("tpch.double-type-mapping"), new StringLiteral(new NodeLocation(2, 30), "double"))),
                ImmutableMap.of("tpch.column-naming", "standard", "tpch.double-type-mapping", "double"),
                ImmutableList.of(
                        new Property(new NodeLocation(1, 1), new Identifier("tpch.column-naming"), new StringLiteral(new NodeLocation(1, 30), "simplified")),
                        new Property(new NodeLocation(2, 1), new Identifier("tpch.double-type-mapping"), new StringLiteral(new NodeLocation(2, 30), "decimal"))),
                ImmutableMap.of("tpch.column-naming", "simplified", "tpch.double-type-mapping", "decimal"));
    }

    @Test
    void testRemoveCatalogProperties()
    {
        testSetProperties(
                ImmutableList.of(
                        new Property(new NodeLocation(1, 1), new Identifier("tpch.column-naming"), new StringLiteral(new NodeLocation(1, 30), "standard")),
                        new Property(new NodeLocation(2, 1), new Identifier("tpch.double-type-mapping"), new StringLiteral(new NodeLocation(2, 30), "double")),
                        new Property(new NodeLocation(3, 1), new Identifier("tpch.produce-pages"), new StringLiteral(new NodeLocation(2, 30), "true"))),
                ImmutableMap.of("tpch.column-naming", "standard", "tpch.double-type-mapping", "double", "tpch.produce-pages", "true"),
                ImmutableList.of(
                        new Property(new NodeLocation(1, 1), new Identifier("tpch.produce-pages")),
                        new Property(new NodeLocation(2, 1), new Identifier("tpch.column-naming"))),
                ImmutableMap.of("tpch.double-type-mapping", "double"));
    }

    @Test
    void testAddOrReplaceCatalogFailure()
    {
        MockCatalogStore catalogStore = new MockCatalogStore();
        String catalog = "catalog_" + randomNameSuffix();

        try (QueryRunner queryRunner = new StandaloneQueryRunner(
                TEST_SESSION,
                builder -> builder
                        .setAdditionalModule(new MockCatalogStoreModule(catalogStore))
                        .addProperty("catalog.store", "mock"))) {
            queryRunner.installPlugin(new TpchPlugin());

            executeCreateCatalog(
                    queryRunner,
                    catalog,
                    ImmutableList.of(new Property(new NodeLocation(1, 1), new Identifier("tpch.column-naming"), new StringLiteral(new NodeLocation(1, 30), "standard"))));

            catalogStore.failAddOrReplaceCatalog();

            assertThatThrownBy(
                    () -> executeSetCatalogProperties(
                            queryRunner,
                            catalog,
                            ImmutableList.of(new Property(new NodeLocation(1, 1), new Identifier("tpch.column-naming"), new StringLiteral(new NodeLocation(1, 30), "simplified")))))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Add or replace catalog failed");

            assertThat(getCatalogProperties(queryRunner, catalog)).isEqualTo(ImmutableMap.of("tpch.column-naming", "standard"));
        }
    }

    private void testSetProperties(
            List<Property> initialProperties,
            Map<String, String> expectedInitialProperties,
            List<Property> updatedProperties,
            Map<String, String> expectedUpdatedProperties)
    {
        String catalog = "catalog_" + randomNameSuffix();

        try (QueryRunner queryRunner = new StandaloneQueryRunner(TEST_SESSION)) {
            queryRunner.installPlugin(new TpchPlugin());

            executeCreateCatalog(queryRunner, catalog, initialProperties);
            assertThat(catalogExists(queryRunner, catalog)).isTrue();
            assertThat(getCatalogProperties(queryRunner, catalog)).isEqualTo(expectedInitialProperties);

            executeSetCatalogProperties(queryRunner, catalog, updatedProperties);
            assertThat(catalogExists(queryRunner, catalog)).isTrue();
            assertThat(getCatalogProperties(queryRunner, catalog)).isEqualTo(expectedUpdatedProperties);
        }
    }

    private void executeCreateCatalog(QueryRunner queryRunner, String catalog, List<Property> catalogProperties)
    {
        Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks = queryRunner.getCoordinator().getInstance(Key.get(new TypeLiteral<>() {}));
        CreateCatalogTask task = (CreateCatalogTask) tasks.get(CreateCatalog.class);
        CreateCatalog statement = new CreateCatalog(
                new NodeLocation(1, 1),
                new Identifier(catalog),
                false,
                new Identifier(CONNECTOR_NAME),
                catalogProperties,
                Optional.empty(),
                Optional.empty());
        ListenableFuture<Void> future = task.execute(statement, createNewQuery(queryRunner), emptyList(), WarningCollector.NOOP);
        getFutureValue(future);
    }

    private void executeSetCatalogProperties(QueryRunner queryRunner, String catalogName, List<Property> properties)
    {
        Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks = queryRunner.getCoordinator().getInstance(Key.get(new TypeLiteral<>() {}));
        SetCatalogPropertiesTask task = (SetCatalogPropertiesTask) tasks.get(SetCatalogProperties.class);
        SetCatalogProperties statement = new SetCatalogProperties(new NodeLocation(1, 1), new Identifier(catalogName), properties);
        ListenableFuture<Void> future = task.execute(statement, createNewQuery(queryRunner), emptyList(), WarningCollector.NOOP);
        getFutureValue(future);
    }

    private boolean catalogExists(QueryRunner queryRunner, String catalog)
    {
        return queryRunner.getPlannerContext().getMetadata().catalogExists(createNewQuery(queryRunner).getSession(), catalog);
    }

    private Map<String, String> getCatalogProperties(QueryRunner queryRunner, String catalog)
    {
        CatalogManager catalogStore = queryRunner.getCoordinator().getInstance(Key.get(CatalogManager.class));
        Optional<CatalogProperties> catalogProperties = queryRunner.getPlannerContext().getMetadata().getCatalogHandle(createNewQuery(queryRunner).getSession(), catalog)
                .flatMap(catalogStore::getCatalogProperties);
        assertThat(catalogProperties).isPresent();
        return catalogProperties.get().properties();
    }

    private QueryStateMachine createNewQuery(QueryRunner queryRunner)
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
                Optional.empty(),
                new NodeVersion("test"));
    }
}
