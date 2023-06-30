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
import io.trino.client.NodeVersion;
import io.trino.execution.warnings.WarningCollector;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.tree.CreateCatalog;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.StringLiteral;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestCreateCatalogTask
{
    private static final String TEST_CATALOG = "test_catalog";
    private static final ImmutableList<Property> TPCH_PROPERTIES = ImmutableList.of(new Property(new Identifier("tpch.partitioning-enabled"), new StringLiteral("false")));

    protected LocalQueryRunner queryRunner;
    private QueryStateMachine queryStateMachine;

    @BeforeMethod
    public void setUp()
    {
        queryRunner = LocalQueryRunner.create(TEST_SESSION);
        queryRunner.registerCatalogFactory(new TpchConnectorFactory());
        queryRunner.registerCatalogFactory(new FailConnectorFactory());
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
                queryRunner.getMetadata(),
                WarningCollector.NOOP,
                createPlanOptimizersStatsCollector(),
                Optional.empty(),
                true,
                new NodeVersion("test"));
    }

    @AfterMethod(alwaysRun = true)
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
        CreateCatalogTask task = getCreateCatalogTask();
        CreateCatalog statement = new CreateCatalog(new Identifier(TEST_CATALOG), false, new Identifier("tpch"), TPCH_PROPERTIES, Optional.empty(), Optional.empty());
        getFutureValue(task.execute(statement, queryStateMachine, emptyList(), WarningCollector.NOOP));
        assertTrue(queryRunner.getMetadata().catalogExists(queryStateMachine.getSession(), TEST_CATALOG));
        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(() -> getFutureValue(task.execute(statement, queryStateMachine, emptyList(), WarningCollector.NOOP)))
                .withMessage("Catalog '%s' already exists", TEST_CATALOG);
    }

    @Test
    public void testDuplicatedCreateCatalogIfNotExists()
    {
        CreateCatalogTask task = getCreateCatalogTask();
        CreateCatalog statement = new CreateCatalog(new Identifier(TEST_CATALOG), true, new Identifier("tpch"), TPCH_PROPERTIES, Optional.empty(), Optional.empty());
        getFutureValue(task.execute(statement, queryStateMachine, emptyList(), WarningCollector.NOOP));
        assertTrue(queryRunner.getMetadata().catalogExists(queryStateMachine.getSession(), TEST_CATALOG));
        getFutureValue(task.execute(statement, queryStateMachine, emptyList(), WarningCollector.NOOP));
        assertTrue(queryRunner.getMetadata().catalogExists(queryStateMachine.getSession(), TEST_CATALOG));
    }

    @Test
    public void failCreateCatalog()
    {
        CreateCatalogTask task = getCreateCatalogTask();
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> getFutureValue(task.execute(
                        new CreateCatalog(
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

    private CreateCatalogTask getCreateCatalogTask()
    {
        return new CreateCatalogTask(queryRunner.getPlannerContext(), new AllowAllAccessControl(), queryRunner.getCatalogManager());
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
}
