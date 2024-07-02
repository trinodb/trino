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
import io.trino.sql.tree.Property;
import io.trino.sql.tree.RenameCatalog;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

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
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestRenameCatalogTask
{
    private static final ImmutableList<Property> TPCH_PROPERTIES = ImmutableList.of(new Property(new Identifier("tpch.partitioning-enabled"), new StringLiteral("false")));

    protected QueryRunner queryRunner;

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
    public void testRenameCatalog()
    {
        String catalogA = "catalog_a_" + randomNameSuffix();
        String catalogB = "catalog_b_" + randomNameSuffix();

        executeCreateCatalog(catalogA);
        assertThat(catalogExists(catalogA)).isTrue();

        executeRenameCatalog(catalogA, catalogB);

        assertThat(catalogExists(catalogA)).isFalse();
        assertThat(catalogExists(catalogB)).isTrue();
    }

    @Test
    public void testRenameCatalogTwice()
    {
        String catalogA = "catalog_a_" + randomNameSuffix();
        String catalogB = "catalog_b_" + randomNameSuffix();
        String catalogC = "catalog_c_" + randomNameSuffix();

        executeCreateCatalog(catalogA);
        assertThat(catalogExists(catalogA)).isTrue();

        executeRenameCatalog(catalogA, catalogB);

        assertThat(catalogExists(catalogA)).isFalse();
        assertThat(catalogExists(catalogB)).isTrue();
        assertThat(catalogExists(catalogC)).isFalse();

        executeRenameCatalog(catalogB, catalogC);

        assertThat(catalogExists(catalogA)).isFalse();
        assertThat(catalogExists(catalogB)).isFalse();
        assertThat(catalogExists(catalogC)).isTrue();
    }

    @Test
    public void testRenameCatalogToTheSameName()
    {
        String catalogA = "catalog_a_" + randomNameSuffix();

        executeCreateCatalog(catalogA);
        assertThat(catalogExists(catalogA)).isTrue();

        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(() -> executeRenameCatalog(catalogA, catalogA))
                .withMessageMatching("Catalog .* already exists");

        assertThat(catalogExists(catalogA)).isTrue();
    }

    @Test
    public void testRenameNonExistingCatalog()
    {
        String catalogA = "catalog_a_" + randomNameSuffix();
        String catalogB = "catalog_b_" + randomNameSuffix();

        assertThat(catalogExists(catalogA)).isFalse();

        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(() -> executeRenameCatalog(catalogA, catalogB))
                .withMessageMatching("Catalog .* does not exist");

        assertThat(catalogExists(catalogA)).isFalse();
        assertThat(catalogExists(catalogB)).isFalse();
    }

    @Test
    public void testRenameSameCatalogTwice()
    {
        String catalogA = "catalog_a_" + randomNameSuffix();
        String catalogB = "catalog_b_" + randomNameSuffix();

        executeCreateCatalog(catalogA);
        assertThat(catalogExists(catalogA)).isTrue();

        executeRenameCatalog(catalogA, catalogB);
        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(() -> executeRenameCatalog(catalogA, catalogB))
                .withMessageMatching("Catalog .* does not exist");

        assertThat(catalogExists(catalogA)).isFalse();
        assertThat(catalogExists(catalogB)).isTrue();
    }

    private void executeCreateCatalog(String catalogA)
    {
        Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks = queryRunner.getCoordinator().getInstance(Key.get(new TypeLiteral<>() {}));
        CreateCatalogTask task = (CreateCatalogTask) tasks.get(CreateCatalog.class);
        CreateCatalog statement = new CreateCatalog(new Identifier(catalogA), false, new Identifier("tpch"), TPCH_PROPERTIES, Optional.empty(), Optional.empty());
        ListenableFuture<Void> future = task.execute(statement, createNewQuery(), emptyList(), WarningCollector.NOOP);
        getFutureValue(future);
    }

    private void executeRenameCatalog(String catalogA, String catalogB)
    {
        Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks = queryRunner.getCoordinator().getInstance(Key.get(new TypeLiteral<>() {}));
        RenameCatalogTask task = (RenameCatalogTask) tasks.get(RenameCatalog.class);
        RenameCatalog statement = new RenameCatalog(new Identifier(catalogA), new Identifier(catalogB));
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
