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

import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableHandle.ResolvingIdentity;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.Identity;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TestingSplit.createLocalSplit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPageSourceManager
{
    private static final Identity CALLER = Identity.forUser("caller")
            .withGroups(Set.of("callers"))
            .withExtraCredentials(Map.of("caller_credential", "value"))
            .build();

    @Test
    public void testSharedMemoryReleasedWithLastReference()
    {
        AtomicReference<MemoryContext> sharedMemoryContext = new AtomicReference<>();
        AggregatedMemoryContext scanMemoryContext = newSimpleAggregatedMemoryContext();
        PageSourceProvider provider = createPageSourceProvider(sharedMemoryContext, scanMemoryContext);

        provider.retain();
        provider.retain();
        sharedMemoryContext.get().setBytes(1024);
        assertThat(scanMemoryContext.getBytes()).isEqualTo(1024);

        provider.release();
        provider.release();
        assertThat(scanMemoryContext.getBytes()).isEqualTo(1024);

        // the reference held by the operator factory
        provider.release();
        assertThat(scanMemoryContext.getBytes()).isEqualTo(0);
    }

    @Test
    public void testReleaseWithoutReference()
    {
        PageSourceProvider provider = createPageSourceProvider(new AtomicReference<>(), newSimpleAggregatedMemoryContext());

        provider.release();
        assertThatThrownBy(provider::release)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Reference has already been freed");
    }

    @Test
    public void testTableResolvingIdentity()
    {
        ConnectorIdentity owner = ConnectorIdentity.forUser("view_owner")
                .withGroups(Set.of("owners"))
                .build();
        ConnectorIdentity actual = pageSourceIdentity(ResolvingIdentity.from(owner));

        assertThat(actual.getUser()).isEqualTo(owner.getUser());
        assertThat(actual.getGroups()).isEqualTo(owner.getGroups());
        // extra credentials belong to the session identity only
        assertThat(actual.getExtraCredentials()).isEmpty();
    }

    @Test
    public void testTableResolvedAsSessionIdentity()
    {
        ConnectorIdentity actual = pageSourceIdentity(ResolvingIdentity.from(CALLER.toConnectorIdentity(TEST_CATALOG_NAME)));

        assertThat(actual.getUser()).isEqualTo(CALLER.getUser());
        assertThat(actual.getGroups()).isEqualTo(CALLER.getGroups());
        assertThat(actual.getExtraCredentials()).isEqualTo(CALLER.getExtraCredentials());
    }

    private static PageSourceProvider createPageSourceProvider(AtomicReference<MemoryContext> sharedMemoryContext, AggregatedMemoryContext scanMemoryContext)
    {
        ConnectorPageSourceProviderFactory factory = memoryContext -> {
            sharedMemoryContext.set(memoryContext);
            return new ConnectorPageSourceProvider() {};
        };
        return new PageSourceManager(CatalogServiceProvider.singleton(TEST_CATALOG_HANDLE, factory))
                .createPageSourceProvider(TEST_CATALOG_HANDLE, scanMemoryContext);
    }

    private static ConnectorIdentity pageSourceIdentity(ResolvingIdentity resolvingIdentity)
    {
        Session session = testSessionBuilder().setIdentity(CALLER).build();
        TableHandle table = new TableHandle(TEST_CATALOG_HANDLE, TEST_TABLE_HANDLE.connectorHandle(), TEST_TABLE_HANDLE.transaction(), resolvingIdentity);
        RecordingPageSourceProvider connector = new RecordingPageSourceProvider();
        PageSourceManager pageSourceManager = new PageSourceManager(CatalogServiceProvider.singleton(TEST_CATALOG_HANDLE, _ -> connector));

        pageSourceManager.createPageSourceProvider(TEST_CATALOG_HANDLE, newSimpleAggregatedMemoryContext())
                .createPageSource(session, new Split(TEST_CATALOG_HANDLE, createLocalSplit()), table, Optional.empty(), List.of(), DynamicFilter.EMPTY, MemoryContext.NO_LIMIT);

        assertThat(connector.session.getQueryId()).isEqualTo(session.getQueryId().toString());
        return connector.session.getIdentity();
    }

    private static class RecordingPageSourceProvider
            implements ConnectorPageSourceProvider
    {
        private ConnectorSession session;

        @Override
        public ConnectorPageSource createPageSource(
                ConnectorTransactionHandle transaction,
                ConnectorSession session,
                ConnectorSplit split,
                ConnectorTableHandle table,
                Optional<ConnectorTableCredentials> tableCredentials,
                List<ColumnHandle> columns,
                DynamicFilter dynamicFilter)
        {
            this.session = session;
            return new EmptyPageSource();
        }
    }
}
