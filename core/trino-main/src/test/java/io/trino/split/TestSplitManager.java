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
package io.trino.split;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.execution.QueryManagerConfig;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableHandle.ResolvingIdentity;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.Identity;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TestingTransactionHandle.create;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSplitManager
{
    private static final Identity CALLER = Identity.forUser("caller")
            .withGroups(Set.of("callers"))
            .withExtraCredentials(Map.of("caller_credential", "value"))
            .build();

    @Test
    public void testTableResolvingIdentity()
    {
        ConnectorIdentity owner = ConnectorIdentity.forUser("view_owner")
                .withGroups(Set.of("owners"))
                .build();
        assertConnectorSessions(ResolvingIdentity.from(owner), owner);
    }

    @Test
    public void testTableResolvedAsSessionIdentity()
    {
        ConnectorIdentity caller = CALLER.toConnectorIdentity(TEST_CATALOG_HANDLE.getCatalogName().toString());
        assertConnectorSessions(ResolvingIdentity.from(caller), caller);
    }

    private static void assertConnectorSessions(ResolvingIdentity tableIdentity, ConnectorIdentity expected)
    {
        Session session = testSessionBuilder().setIdentity(CALLER).build();
        // The connector handle cannot repair an incorrectly forwarded identity.
        TableHandle table = new TableHandle(TEST_CATALOG_HANDLE, new TestingTableHandle(), create(), tableIdentity);
        RecordingSplitManager connector = new RecordingSplitManager();
        SplitManager splitManager = new SplitManager(
                CatalogServiceProvider.singleton(TEST_CATALOG_HANDLE, connector),
                OpenTelemetry.noop().getTracer("test"),
                new QueryManagerConfig());
        try {
            try (SplitSource ignored = splitManager.getSplits(session, Span.getInvalid(), table, DynamicFilter.EMPTY, Constraint.alwaysTrue())) {
                assertThat(connector.invocations)
                        .extracting(Invocation::operation)
                        .containsExactly("getSplits");
            }
            assertThat(connector.invocations).allSatisfy(invocation -> {
                ConnectorSession actual = invocation.session();
                assertThat(actual.getIdentity().getUser()).as(invocation.operation()).isEqualTo(expected.getUser());
                assertThat(actual.getIdentity().getGroups()).as(invocation.operation()).isEqualTo(expected.getGroups());
                assertThat(actual.getIdentity().getExtraCredentials()).as(invocation.operation()).isEqualTo(expected.getExtraCredentials());
                assertThat(actual.getQueryId()).isEqualTo(session.getQueryId().toString());
                assertThat(actual.getStart()).isEqualTo(session.getStart());
            });
        }
        finally {
            splitManager.shutdown();
        }
    }

    private record TestingTableHandle()
            implements ConnectorTableHandle {}

    private record Invocation(String operation, ConnectorSession session) {}

    private static class RecordingSplitManager
            implements ConnectorSplitManager
    {
        private final List<Invocation> invocations = new ArrayList<>();

        @Override
        public ConnectorSplitSource getSplits(
                ConnectorTransactionHandle transaction,
                ConnectorSession session,
                ConnectorTableHandle table,
                Set<ColumnHandle> dynamicFilterColumns,
                Constraint constraint)
        {
            invocations.add(new Invocation("getSplits", session));
            return new FixedSplitSource(List.of());
        }
    }
}
