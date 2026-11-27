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
package io.trino.plugin.blackhole;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.airlift.units.Duration;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SourcePage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

final class TestBlackHolePageSourceProvider
{
    @Test
    void testReportsMemoryUsageToMemoryContext()
            throws IOException
    {
        ListeningScheduledExecutorService executorService = listeningDecorator(newSingleThreadScheduledExecutor());
        try {
            BlackHolePageSourceProvider pageSourceProvider = new BlackHolePageSourceProvider(executorService);
            BlackHoleColumnHandle column = new BlackHoleColumnHandle("value", BIGINT);
            BlackHoleTableHandle table = new BlackHoleTableHandle("default", "test", ImmutableList.of(column), 1, 2, 100, 16, new Duration(0, SECONDS));

            AtomicLong reportedBytes = new AtomicLong(-1);
            ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                    BlackHoleTransactionHandle.INSTANCE,
                    SESSION,
                    BlackHoleSplit.INSTANCE,
                    table,
                    Optional.empty(),
                    ImmutableList.of(column),
                    DynamicFilter.EMPTY,
                    reportedBytes::set);
            SourcePage page = pageSource.getNextSourcePage();
            assertThat(page.getPositionCount()).isEqualTo(100);
            assertThat(reportedBytes).hasValue(page.getSizeInBytes());

            pageSource.close();
            assertThat(reportedBytes).hasValue(0);
        }
        finally {
            executorService.shutdownNow();
        }
    }
}
