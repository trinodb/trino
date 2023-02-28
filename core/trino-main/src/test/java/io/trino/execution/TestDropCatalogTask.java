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

import com.google.common.collect.ImmutableMap;
import io.trino.client.NodeVersion;
import io.trino.execution.warnings.WarningCollector;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.tree.DropCatalog;
import io.trino.sql.tree.Identifier;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestDropCatalogTask
{
    private static final String TEST_CATALOG = "test_catalog";

    protected LocalQueryRunner queryRunner;

    @BeforeMethod
    public void setUp()
    {
        queryRunner = LocalQueryRunner.create(TEST_SESSION);
        queryRunner.registerCatalogFactory(new TpchConnectorFactory());
    }

    @AfterMethod
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
        queryRunner.createCatalog(TEST_CATALOG, "tpch", ImmutableMap.of());
        assertTrue(queryRunner.getMetadata().catalogExists(createNewQuery().getSession(), TEST_CATALOG));

        DropCatalogTask task = getCreateCatalogTask();
        DropCatalog statement = new DropCatalog(new Identifier(TEST_CATALOG), false, false);
        getFutureValue(task.execute(statement, createNewQuery(), emptyList(), WarningCollector.NOOP));
        assertFalse(queryRunner.getMetadata().catalogExists(createNewQuery().getSession(), TEST_CATALOG));
        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(() -> getFutureValue(task.execute(statement, createNewQuery(), emptyList(), WarningCollector.NOOP)))
                .withMessage("Catalog '%s' does not exist", TEST_CATALOG);
    }

    @Test
    public void testDuplicatedCreateCatalogIfNotExists()
    {
        queryRunner.createCatalog(TEST_CATALOG, "tpch", ImmutableMap.of());
        assertTrue(queryRunner.getMetadata().catalogExists(createNewQuery().getSession(), TEST_CATALOG));

        DropCatalogTask task = getCreateCatalogTask();
        DropCatalog statement = new DropCatalog(new Identifier(TEST_CATALOG), true, false);
        getFutureValue(task.execute(statement, createNewQuery(), emptyList(), WarningCollector.NOOP));
        assertFalse(queryRunner.getMetadata().catalogExists(createNewQuery().getSession(), TEST_CATALOG));
        getFutureValue(task.execute(statement, createNewQuery(), emptyList(), WarningCollector.NOOP));
        assertFalse(queryRunner.getMetadata().catalogExists(createNewQuery().getSession(), TEST_CATALOG));
    }

    private DropCatalogTask getCreateCatalogTask()
    {
        return new DropCatalogTask(queryRunner.getCatalogManager(), new AllowAllAccessControl());
    }

    private QueryStateMachine createNewQuery()
    {
        return QueryStateMachine.begin(
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
                Optional.empty(),
                true,
                new NodeVersion("test"));
    }
}
