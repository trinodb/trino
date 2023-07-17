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
package io.trino.connector.alternatives;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class MockPlanAlternativeSplitManager
        implements ConnectorSplitManager
{
    private final ConnectorSplitManager delegate;

    public MockPlanAlternativeSplitManager(ConnectorSplitManager delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle table, DynamicFilter dynamicFilter, Constraint constraint)
    {
        return new PlanAlternativeConnectorSplitSource(delegate.getSplits(transaction, session, table, dynamicFilter, constraint));
    }

    private static class PlanAlternativeConnectorSplitSource
            implements ConnectorSplitSource
    {
        private final ConnectorSplitSource delegate;
        private final AtomicInteger nextSplitNumber;

        private PlanAlternativeConnectorSplitSource(ConnectorSplitSource delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.nextSplitNumber = new AtomicInteger(0);
        }

        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
        {
            CompletableFuture<ConnectorSplitBatch> delegateBatch = delegate.getNextBatch(maxSize);
            if (delegateBatch.isCompletedExceptionally()) {
                return delegateBatch;
            }
            return delegateBatch.thenApply(batch -> new ConnectorSplitBatch(
                    batch.getSplits().stream()
                            .map(split -> new MockPlanAlternativeSplit(split, nextSplitNumber.getAndIncrement()))
                            .collect(toImmutableList()),
                    batch.isNoMoreSplits()));
        }

        @Override
        public void close()
        {
            delegate.close();
        }

        @Override
        public boolean isFinished()
        {
            return delegate.isFinished();
        }

        @Override
        public Optional<List<Object>> getTableExecuteSplitsInfo()
        {
            return delegate.getTableExecuteSplitsInfo();
        }
    }
}
