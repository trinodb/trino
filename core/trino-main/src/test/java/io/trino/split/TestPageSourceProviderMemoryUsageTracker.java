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
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import io.trino.split.PageSourceProvider.MemoryUsageTracker;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static org.assertj.core.api.Assertions.assertThat;

final class TestPageSourceProviderMemoryUsageTracker
{
    @Test
    void testSharedMemoryUsageReportedByAtMostOneTracker()
    {
        AtomicLong memoryUsage = new AtomicLong(1000);
        PageSourceProvider provider = createPageSourceProvider(memoryUsage);

        MemoryUsageTracker first = provider.createMemoryUsageTracker();
        MemoryUsageTracker second = provider.createMemoryUsageTracker();

        // the first tracker to poll becomes the reporter
        assertThat(first.getMemoryUsage()).isEqualTo(1000);
        // other trackers do not count the shared state again
        assertThat(second.getMemoryUsage()).isEqualTo(0);
        assertThat(first.getMemoryUsage()).isEqualTo(1000);

        // usage changes are visible to the reporter
        memoryUsage.set(2000);
        assertThat(first.getMemoryUsage()).isEqualTo(2000);
        assertThat(second.getMemoryUsage()).isEqualTo(0);
    }

    @Test
    void testReportingRolePassesOnClose()
    {
        PageSourceProvider provider = createPageSourceProvider(new AtomicLong(1000));

        MemoryUsageTracker first = provider.createMemoryUsageTracker();
        MemoryUsageTracker second = provider.createMemoryUsageTracker();
        assertThat(first.getMemoryUsage()).isEqualTo(1000);
        assertThat(second.getMemoryUsage()).isEqualTo(0);

        first.close();
        // the next tracker to poll takes over reporting
        assertThat(second.getMemoryUsage()).isEqualTo(1000);
        // a closed tracker never reports, even if it polls again
        assertThat(first.getMemoryUsage()).isEqualTo(0);

        // close is idempotent, and a new tracker can take over after the reporter is closed
        second.close();
        second.close();
        MemoryUsageTracker third = provider.createMemoryUsageTracker();
        assertThat(third.getMemoryUsage()).isEqualTo(1000);
    }

    @Test
    void testCloseOfNonReportingTracker()
    {
        PageSourceProvider provider = createPageSourceProvider(new AtomicLong(1000));

        MemoryUsageTracker first = provider.createMemoryUsageTracker();
        MemoryUsageTracker second = provider.createMemoryUsageTracker();
        assertThat(first.getMemoryUsage()).isEqualTo(1000);

        // closing a non-reporting tracker does not affect the reporter
        second.close();
        assertThat(first.getMemoryUsage()).isEqualTo(1000);
    }

    @Test
    void testDefaultTrackerDelegatesToProvider()
    {
        PageSourceProvider provider = new PageSourceProvider()
        {
            @Override
            public ConnectorPageSource createPageSource(
                    Session session,
                    Split split,
                    TableHandle table,
                    Optional<ConnectorTableCredentials> tableCredentials,
                    List<ColumnHandle> columns,
                    DynamicFilter dynamicFilter,
                    MemoryContext memoryContext)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getMemoryUsage()
            {
                return 42;
            }
        };

        assertThat(provider.createMemoryUsageTracker().getMemoryUsage()).isEqualTo(42);
    }

    private static PageSourceProvider createPageSourceProvider(AtomicLong memoryUsage)
    {
        ConnectorPageSourceProvider connectorPageSourceProvider = new ConnectorPageSourceProvider()
        {
            @Override
            public long getMemoryUsage()
            {
                return memoryUsage.get();
            }
        };
        return new PageSourceManager(_ -> () -> connectorPageSourceProvider)
                .createPageSourceProvider(TEST_CATALOG_HANDLE);
    }
}
