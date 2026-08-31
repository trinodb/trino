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
import com.google.common.collect.ImmutableSet;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.spi.Node;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

// public to allow reflection to recordInvocation method
public final class TestDistributedCallTask
        extends AbstractTestQueryFramework
{
    private static final String CATALOG = "distributed_call_catalog";
    private static final AtomicBoolean CALLED_ON_WORKER = new AtomicBoolean();
    private static final AtomicBoolean CALLED_ON_COORDINATOR = new AtomicBoolean();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder().setCatalog(CATALOG).setSchema("default").build())
                .setWorkerCount(2)
                .build();
        queryRunner.installPlugin(new MockConnectorPlugin(new RecordingConnectorFactory()));
        queryRunner.createCatalog(CATALOG, "recording_mock");
        return queryRunner;
    }

    @Test
    void testProcedureRunsOnWorker()
    {
        CALLED_ON_WORKER.set(false);
        CALLED_ON_COORDINATOR.set(false);

        getQueryRunner().execute("CALL test_worker_procedure()");

        assertThat(CALLED_ON_WORKER.get()).isTrue();
        assertThat(CALLED_ON_COORDINATOR.get()).isFalse();
    }

    @UsedByGeneratedCode
    public static void recordInvocation(Node currentNode)
    {
        if (currentNode.isCoordinator()) {
            CALLED_ON_COORDINATOR.set(true);
        }
        else {
            CALLED_ON_WORKER.set(true);
        }
    }

    private static class RecordingConnectorFactory
            implements ConnectorFactory
    {
        private final ConnectorFactory delegateFactory = MockConnectorFactory.builder().build();

        @Override
        public String getName()
        {
            return "recording_mock";
        }

        @Override
        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
        {
            Node currentNode = context.getCurrentNode();
            MethodHandle methodHandle = methodHandle(TestDistributedCallTask.class, "recordInvocation", Node.class).bindTo(currentNode);
            Procedure procedure = new Procedure("default", "test_worker_procedure", ImmutableList.of(), methodHandle, false, true);
            return new RecordingConnector(delegateFactory.create(catalogName, config, context), procedure);
        }
    }

    private static class RecordingConnector
            implements Connector
    {
        private final Connector delegate;
        private final Procedure procedure;

        RecordingConnector(Connector delegate, Procedure procedure)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.procedure = requireNonNull(procedure, "procedure is null");
        }

        @Override
        public Set<Procedure> getProcedures()
        {
            return ImmutableSet.of(procedure);
        }

        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
        {
            return delegate.beginTransaction(isolationLevel, readOnly, autoCommit);
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
        {
            return delegate.getMetadata(session, transactionHandle);
        }

        @Override
        public void shutdown()
        {
            delegate.shutdown();
        }
    }
}
