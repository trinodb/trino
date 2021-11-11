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
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.analyzer.FeaturesConfig;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.PathElement;
import io.trino.sql.tree.PathSpecification;
import io.trino.sql.tree.SetPath;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestSetPathTask
{
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final Metadata metadata;

    private ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));

    public TestSetPathTask()
    {
        CatalogManager catalogManager = new CatalogManager();
        transactionManager = createTestTransactionManager(catalogManager);
        accessControl = new AllowAllAccessControl();

        metadata = createTestMetadataManager(transactionManager, new FeaturesConfig());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
    }

    @Test
    public void testSetPath()
    {
        PathSpecification pathSpecification = new PathSpecification(Optional.empty(), ImmutableList.of(
                new PathElement(Optional.empty(), new Identifier("foo"))));

        QueryStateMachine stateMachine = createQueryStateMachine("SET PATH foo");
        executeSetPathTask(pathSpecification, stateMachine);

        assertEquals(stateMachine.getSetPath(), "foo");
    }

    @Test
    public void testSetPathInvalidCatalog()
    {
        PathSpecification invalidPathSpecification = new PathSpecification(Optional.empty(), ImmutableList.of(
                new PathElement(Optional.of(new Identifier("invalidCatalog")), new Identifier("thisDoesNotMatter"))));

        QueryStateMachine stateMachine = createQueryStateMachine("SET PATH invalidCatalog.thisDoesNotMatter");

        assertThatThrownBy(() -> executeSetPathTask(invalidPathSpecification, stateMachine))
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching("Catalog '.*' does not exist");
    }

    private QueryStateMachine createQueryStateMachine(String query)
    {
        return QueryStateMachine.begin(
                query,
                Optional.empty(),
                TEST_SESSION,
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                false,
                transactionManager,
                accessControl,
                executor,
                metadata,
                WarningCollector.NOOP,
                Optional.empty());
    }

    private void executeSetPathTask(PathSpecification pathSpecification, QueryStateMachine stateMachine)
    {
        getFutureValue(new SetPathTask().execute(
                new SetPath(pathSpecification),
                transactionManager,
                metadata,
                accessControl,
                stateMachine,
                emptyList(),
                WarningCollector.NOOP));
    }
}
