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
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.RenameCatalog;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;

import java.net.URI;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestRenameCatalogTask
{
    @Test
    void testRenameCatalog()
    {
        String catalogA = "catalog_a_" + randomNameSuffix();
        String catalogB = "catalog_b_" + randomNameSuffix();

        try (QueryRunner queryRunner = createQueryRunner()) {
            executeCreateCatalog(queryRunner, catalogA);
            assertThat(catalogExists(queryRunner, catalogA)).isTrue();

            executeRenameCatalog(queryRunner, catalogA, catalogB);

            assertThat(catalogExists(queryRunner, catalogA)).isFalse();
            assertThat(catalogExists(queryRunner, catalogB)).isTrue();
        }
    }

    @Test
    void testRenameCatalogTwice()
    {
        String catalogA = "catalog_a_" + randomNameSuffix();
        String catalogB = "catalog_b_" + randomNameSuffix();
        String catalogC = "catalog_c_" + randomNameSuffix();

        try (QueryRunner queryRunner = createQueryRunner()) {
            executeCreateCatalog(queryRunner, catalogA);
            assertThat(catalogExists(queryRunner, catalogA)).isTrue();

            executeRenameCatalog(queryRunner, catalogA, catalogB);

            assertThat(catalogExists(queryRunner, catalogA)).isFalse();
            assertThat(catalogExists(queryRunner, catalogB)).isTrue();
            assertThat(catalogExists(queryRunner, catalogC)).isFalse();

            executeRenameCatalog(queryRunner, catalogB, catalogC);

            assertThat(catalogExists(queryRunner, catalogA)).isFalse();
            assertThat(catalogExists(queryRunner, catalogB)).isFalse();
            assertThat(catalogExists(queryRunner, catalogC)).isTrue();
        }
    }

    @Test
    void testRenameCatalogToTheSameName()
    {
        String catalogA = "catalog_a_" + randomNameSuffix();

        try (QueryRunner queryRunner = createQueryRunner()) {
            executeCreateCatalog(queryRunner, catalogA);
            assertThat(catalogExists(queryRunner, catalogA)).isTrue();

            assertThatExceptionOfType(TrinoException.class)
                    .isThrownBy(() -> executeRenameCatalog(queryRunner, catalogA, catalogA))
                    .withMessageMatching("Catalog .* already exists");

            assertThat(catalogExists(queryRunner, catalogA)).isTrue();
        }
    }

    @Test
    void testRenameNonExistingCatalog()
    {
        String catalogA = "catalog_a_" + randomNameSuffix();
        String catalogB = "catalog_b_" + randomNameSuffix();

        try (QueryRunner queryRunner = createQueryRunner()) {
            assertThat(catalogExists(queryRunner, catalogA)).isFalse();

            assertThatExceptionOfType(TrinoException.class)
                    .isThrownBy(() -> executeRenameCatalog(queryRunner, catalogA, catalogB))
                    .withMessageMatching("Catalog .* does not exist");

            assertThat(catalogExists(queryRunner, catalogA)).isFalse();
            assertThat(catalogExists(queryRunner, catalogB)).isFalse();
        }
    }

    @Test
    void testRenameSameCatalogTwice()
    {
        String catalogA = "catalog_a_" + randomNameSuffix();
        String catalogB = "catalog_b_" + randomNameSuffix();

        try (QueryRunner queryRunner = createQueryRunner()) {
            executeCreateCatalog(queryRunner, catalogA);
            assertThat(catalogExists(queryRunner, catalogA)).isTrue();

            executeRenameCatalog(queryRunner, catalogA, catalogB);
            assertThatExceptionOfType(TrinoException.class)
                    .isThrownBy(() -> executeRenameCatalog(queryRunner, catalogA, catalogB))
                    .withMessageMatching("Catalog .* does not exist");

            assertThat(catalogExists(queryRunner, catalogA)).isFalse();
            assertThat(catalogExists(queryRunner, catalogB)).isTrue();
        }
    }

    @Test
    void testAddOrReplaceCatalogFailure()
    {
        MockCatalogStore catalogStore = new MockCatalogStore();
        String catalogA = "catalog_a_" + randomNameSuffix();
        String catalogB = "catalog_b_" + randomNameSuffix();

        try (QueryRunner queryRunner = new StandaloneQueryRunner(
                TEST_SESSION,
                builder -> builder
                        .setAdditionalModule(new MockCatalogStoreModule(catalogStore))
                        .addProperty("catalog.store", "mock"))) {
            queryRunner.installPlugin(new TpchPlugin());

            executeCreateCatalog(queryRunner, catalogA);

            catalogStore.failAddOrReplaceCatalog();

            assertThatThrownBy(() -> executeRenameCatalog(queryRunner, catalogA, catalogB))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Add or replace catalog failed");

            assertThat(catalogExists(queryRunner, catalogA)).isTrue();
            assertThat(catalogExists(queryRunner, catalogB)).isFalse();
        }
    }

    @Test
    void testRemoveCatalogFailure()
    {
        MockCatalogStore catalogStore = new MockCatalogStore();
        String catalogA = "catalog_a_" + randomNameSuffix();
        String catalogB = "catalog_b_" + randomNameSuffix();

        try (QueryRunner queryRunner = new StandaloneQueryRunner(
                TEST_SESSION,
                builder -> builder
                        .setAdditionalModule(new MockCatalogStoreModule(catalogStore))
                        .addProperty("catalog.store", "mock"))) {
            queryRunner.installPlugin(new TpchPlugin());

            executeCreateCatalog(queryRunner, catalogA);

            catalogStore.failRemoveCatalog();

            assertThatThrownBy(() -> executeRenameCatalog(queryRunner, catalogA, catalogB))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Remove catalog failed");

            assertThat(catalogExists(queryRunner, catalogA)).isTrue();
            assertThat(catalogExists(queryRunner, catalogB)).isTrue();
        }
    }

    private void executeCreateCatalog(QueryRunner queryRunner, String catalog)
    {
        Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks = queryRunner.getCoordinator().getInstance(Key.get(new TypeLiteral<>() {}));
        CreateCatalogTask task = (CreateCatalogTask) tasks.get(CreateCatalog.class);
        CreateCatalog statement = new CreateCatalog(
                new NodeLocation(1, 1),
                new Identifier(catalog),
                false,
                new Identifier("tpch"),
                ImmutableList.of(new Property(new Identifier("tpch.partitioning-enabled"), new StringLiteral("false"))),
                Optional.empty(),
                Optional.empty());
        ListenableFuture<Void> future = task.execute(statement, createNewQuery(queryRunner), emptyList(), WarningCollector.NOOP);
        getFutureValue(future);
    }

    private void executeRenameCatalog(QueryRunner queryRunner, String catalogA, String catalogB)
    {
        Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks = queryRunner.getCoordinator().getInstance(Key.get(new TypeLiteral<>() {}));
        RenameCatalogTask task = (RenameCatalogTask) tasks.get(RenameCatalog.class);
        RenameCatalog statement = new RenameCatalog(new NodeLocation(1, 1), new Identifier(catalogA), new Identifier(catalogB));
        ListenableFuture<Void> future = task.execute(statement, createNewQuery(queryRunner), emptyList(), WarningCollector.NOOP);
        getFutureValue(future);
    }

    private boolean catalogExists(QueryRunner queryRunner, String catalog)
    {
        return queryRunner.getPlannerContext().getMetadata().catalogExists(createNewQuery(queryRunner).getSession(), catalog);
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

    private static QueryRunner createQueryRunner()
    {
        QueryRunner queryRunner = new StandaloneQueryRunner(TEST_SESSION);
        queryRunner.installPlugin(new TpchPlugin());
        return queryRunner;
    }
}
