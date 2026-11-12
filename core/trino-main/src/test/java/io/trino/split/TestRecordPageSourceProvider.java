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

import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordPageSource;
import io.trino.spi.connector.RecordSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static org.assertj.core.api.Assertions.assertThat;

class TestRecordPageSourceProvider
{
    @Test
    public void testMemoryContextLifecycle()
            throws IOException
    {
        InMemoryRecordSet recordSet = new InMemoryRecordSet(List.of(BIGINT), List.of(List.of(42L)));
        RecordPageSourceProvider provider = new RecordPageSourceProvider(new ConnectorRecordSetProvider()
        {
            @Override
            public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns)
            {
                return recordSet;
            }
        });

        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        LocalMemoryContext sourceMemoryContext = memoryContext.newLocalMemoryContext("test");
        try (ConnectorPageSource source = provider.createPageSource(
                TEST_TABLE_HANDLE.transaction(),
                SESSION,
                new ConnectorSplit() {},
                TEST_TABLE_HANDLE.connectorHandle(),
                Optional.empty(),
                List.of(),
                DynamicFilter.EMPTY,
                sourceMemoryContext::setBytes)) {
            assertThat(source).isInstanceOf(RecordPageSource.class);
            assertThat(source.getMemoryUsage()).isZero();
            assertThat(memoryContext.getBytes()).isPositive();
            assertThat(source.getNextSourcePage().getPositionCount()).isEqualTo(1);
            assertThat(source.isFinished()).isTrue();
        }
        // Closing the source preserves accounting for the retained builder buffers.
        assertThat(memoryContext.getBytes()).isPositive();
        sourceMemoryContext.close();
        assertThat(memoryContext.getBytes()).isZero();
    }
}
