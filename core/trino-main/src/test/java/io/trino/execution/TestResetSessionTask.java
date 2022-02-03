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
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.SessionPropertyManager;
import io.trino.security.AccessControl;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.ResetSession;
import io.trino.testing.LocalQueryRunner;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

public class TestResetSessionTask
{
    private static final String CATALOG_NAME = "my_catalog";
    private ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final Metadata metadata;
    private final SessionPropertyManager sessionPropertyManager;

    public TestResetSessionTask()
    {
        LocalQueryRunner queryRunner = LocalQueryRunner.builder(TEST_SESSION)
                .withExtraSystemSessionProperties(ImmutableSet.of(() -> ImmutableList.of(
                        stringProperty(
                                "foo",
                                "test property",
                                null,
                                false))))
                .build();

        queryRunner.createCatalog(
                CATALOG_NAME,
                MockConnectorFactory.builder()
                        .withSessionProperty(stringProperty(
                                "baz",
                                "test property",
                                null,
                                false))
                        .build(),
                ImmutableMap.of());

        transactionManager = queryRunner.getTransactionManager();
        accessControl = queryRunner.getAccessControl();
        metadata = queryRunner.getMetadata();
        sessionPropertyManager = queryRunner.getSessionPropertyManager();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
    }

    @Test
    public void test()
    {
        Session session = testSessionBuilder(sessionPropertyManager)
                .setSystemProperty("foo", "bar")
                .setCatalogSessionProperty(CATALOG_NAME, "baz", "blah")
                .build();

        QueryStateMachine stateMachine = QueryStateMachine.begin(
                Optional.empty(),
                "reset foo",
                Optional.empty(),
                session,
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                false,
                transactionManager,
                accessControl,
                executor,
                metadata,
                WarningCollector.NOOP,
                Optional.empty());

        getFutureValue(new ResetSessionTask(metadata, sessionPropertyManager).execute(
                new ResetSession(QualifiedName.of(CATALOG_NAME, "baz")),
                stateMachine,
                emptyList(),
                WarningCollector.NOOP));

        Set<String> sessionProperties = stateMachine.getResetSessionProperties();
        assertEquals(sessionProperties, ImmutableSet.of(CATALOG_NAME + ".baz"));
    }
}
