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
package io.trino.plugin.tpch;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

final class TestTpchPageSourceProvider
{
    // the page source provider does not use the session
    private static final ConnectorSession SESSION = null;

    @Test
    void testReportsMemoryUsageToMemoryContext()
            throws IOException
    {
        TpchPageSourceProvider pageSourceProvider = new TpchPageSourceProvider(8192, DecimalTypeMapping.DOUBLE);
        TpchColumnHandle column = new TpchColumnHandle("orderkey", BIGINT);

        AtomicLong reportedBytes = new AtomicLong(-1);
        try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                new TpchTransactionHandle(),
                SESSION,
                new TpchSplit(0, 1, ImmutableList.of()),
                new TpchTableHandle("tiny", "orders", 0.01),
                Optional.empty(),
                ImmutableList.of(column),
                DynamicFilter.EMPTY,
                reportedBytes::set)) {
            long emptyPageBuilderBytes = reportedBytes.get();
            assertThat(emptyPageBuilderBytes).isPositive();

            // a single call reads fewer rows than a page holds, and buffers them in the page builder
            assertThat(pageSource.getNextSourcePage()).isNull();
            long bufferedRowsBytes = reportedBytes.get();
            assertThat(bufferedRowsBytes).isGreaterThan(emptyPageBuilderBytes);

            assertThat(pageSource.getNextSourcePage().getPositionCount()).isEqualTo(8192);
            assertThat(reportedBytes.get()).isLessThan(bufferedRowsBytes);
        }
    }
}
