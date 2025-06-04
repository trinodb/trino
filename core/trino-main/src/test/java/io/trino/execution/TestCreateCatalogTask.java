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
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogStoreManager;
import io.trino.connector.InMemoryCatalogStore;
import io.trino.connector.MockConnectorPlugin;
import io.trino.execution.warnings.WarningCollector;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.catalog.CatalogStoreFactory;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorName;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.tree.CreateCatalog;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeLocation;
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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static java.util.Collections.emptyList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class TestCreateCatalogTask
{
    private static final String TEST_CATALOG = "test_catalog";
    private static final List<Property> TPCH_PROPERTIES = ImmutableList.of(new Property(new Identifier("tpch.partitioning-enabled"), new StringLiteral("false")));

    protected QueryRunner queryRunner;
    private QueryStateMachine queryStateMachine;
    private CreateCatalogTask task;

    @BeforeEach
    public void setUp()
    {
        QueryRunner queryRunner = new StandaloneQueryRunner(TEST_SESSION);
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.installPlugin(new MockConnectorPlugin(new FailConnectorFactory()));
        Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks = queryRunner.getCoordinator().getInstance(new Key<>() {});
        task = (CreateCatalogTask) tasks.get(CreateCatalog.class);
        queryStateMachine = QueryStateMachine.begin(
                Optional.empty(),
                "test",
                Optional.empty(),
                queryRunner.getDefaultSession(),
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

        this.queryRunner = queryRunner;
    }

    @AfterEach
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
        }
        queryRunner = null;
    }

    @Test
    public void testDuplicatedCreateCatalog()
    {
        CreateCatalog statement = new CreateCatalog(new NodeLocation(1, 1), new Identifier(TEST_CATALOG), false, new Identifier("tpch"), TPCH_PROPERTIES, Optional.empty(), Optional.empty());
        getFutureValue(task.execute(statement, queryStateMachine, emptyList(), WarningCollector.NOOP));
        assertThat(queryRunner.getPlannerContext().getMetadata().catalogExists(queryStateMachine.getSession(), TEST_CATALOG)).isTrue();
        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(() -> getFutureValue(task.execute(statement, queryStateMachine, emptyList(), WarningCollector.NOOP)))
                .withMessage("Catalog '%s' already exists", TEST_CATALOG);
    }

    @Test
    public void testCaseInsensitiveDuplicatedCreateCatalog()
    {
        CreateCatalog statement = new CreateCatalog(new NodeLocation(1, 1), new Identifier(TEST_CATALOG.toUpperCase(ENGLISH)), false, new Identifier("tpch"), TPCH_PROPERTIES, Optional.empty(), Optional.empty());
        getFutureValue(task.execute(statement, queryStateMachine, emptyList(), WarningCollector.NOOP));
        assertThat(queryRunner.getPlannerContext().getMetadata().catalogExists(queryStateMachine.getSession(), TEST_CATALOG)).isTrue();
        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(() -> getFutureValue(task.execute(statement, queryStateMachine, emptyList(), WarningCollector.NOOP)))
                .withMessage("Catalog '%s' already exists", TEST_CATALOG);
    }

    @Test
    public void testDuplicatedCreateCatalogIfNotExists()
    {
        CreateCatalog statement = new CreateCatalog(new NodeLocation(1, 1), new Identifier(TEST_CATALOG), true, new Identifier("tpch"), TPCH_PROPERTIES, Optional.empty(), Optional.empty());
        getFutureValue(task.execute(statement, queryStateMachine, emptyList(), WarningCollector.NOOP));
        assertThat(queryRunner.getPlannerContext().getMetadata().catalogExists(queryStateMachine.getSession(), TEST_CATALOG)).isTrue();
        getFutureValue(task.execute(statement, queryStateMachine, emptyList(), WarningCollector.NOOP));
        assertThat(queryRunner.getPlannerContext().getMetadata().catalogExists(queryStateMachine.getSession(), TEST_CATALOG)).isTrue();
    }

    @Test
    public void failCreateCatalog()
    {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> getFutureValue(task.execute(
                        new CreateCatalog(
                                new NodeLocation(1, 1),
                                new Identifier(TEST_CATALOG),
                                true,
                                new Identifier("fail"),
                                ImmutableList.of(),
                                Optional.empty(),
                                Optional.empty()),
                        queryStateMachine,
                        emptyList(),
                        WarningCollector.NOOP)))
                .withMessageContaining("TEST create catalog fail: " + TEST_CATALOG);
    }

    @Test
    public void testAddOrReplaceCatalogFail()
    {
        try (QueryRunner queryRunner = new StandaloneQueryRunner(
                TEST_SESSION,
                builder -> builder
                        .setAdditionalModule(new FailingAddOrReplaceCatalogStoreModule())
                        .addProperty("catalog.store", "failing_add_or_replace"))) {
            queryRunner.installPlugin(new TpchPlugin());
            Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks = queryRunner.getCoordinator().getInstance(new Key<>() {});
            CreateCatalogTask task = (CreateCatalogTask) tasks.get(CreateCatalog.class);
            QueryStateMachine queryStateMachine = QueryStateMachine.begin(
                    Optional.empty(),
                    "test",
                    Optional.empty(),
                    queryRunner.getDefaultSession(),
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

            CreateCatalog statement = new CreateCatalog(
                    new NodeLocation(1, 1),
                    new Identifier(TEST_CATALOG),
                    true,
                    new Identifier("tpch"),
                    TPCH_PROPERTIES,
                    Optional.empty(),
                    Optional.empty());

            assertThatThrownBy(() -> getFutureValue(task.execute(statement, queryStateMachine, emptyList(), WarningCollector.NOOP)))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Add or replace catalog failed");
            assertThat(queryRunner.getPlannerContext().getMetadata().catalogExists(queryStateMachine.getSession(), TEST_CATALOG)).isFalse();
        }
    }

    private static class FailConnectorFactory
            implements ConnectorFactory
    {
        @Override
        public String getName()
        {
            return "fail";
        }

        @Override
        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
        {
            throw new IllegalArgumentException("TEST create catalog fail: " + catalogName);
        }
    }

    private static class FailingAddOrReplaceCatalogStoreModule
            implements Module
    {
        @Override
        public void configure(Binder binder) {}

        @Provides
        @Singleton
        public CatalogStoreFactory createCatalogStoreFactory(CatalogStoreManager catalogStoreManager)
        {
            FailingAddOrReplaceCatalogStoreFactory factory = new FailingAddOrReplaceCatalogStoreFactory();
            catalogStoreManager.addCatalogStoreFactory(factory);
            return factory;
        }
    }

    private static class FailingAddOrReplaceCatalogStoreFactory
            implements CatalogStoreFactory
    {
        @Override
        public String getName()
        {
            return "failing_add_or_replace";
        }

        @Override
        public CatalogStore create(Map<String, String> config)
        {
            return new FailingAddOrReplaceCatalogStore(new InMemoryCatalogStore());
        }
    }

    private static class FailingAddOrReplaceCatalogStore
            implements CatalogStore
    {
        private final CatalogStore delegate;

        FailingAddOrReplaceCatalogStore(CatalogStore delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public Collection<StoredCatalog> getCatalogs()
        {
            return delegate.getCatalogs();
        }

        @Override
        public CatalogProperties createCatalogProperties(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties)
        {
            return delegate.createCatalogProperties(catalogName, connectorName, properties);
        }

        @Override
        public void addOrReplaceCatalog(CatalogProperties catalogProperties)
        {
            throw new RuntimeException("Add or replace catalog failed");
        }

        @Override
        public void removeCatalog(CatalogName catalogName)
        {
            delegate.removeCatalog(catalogName);
        }
    }
}
