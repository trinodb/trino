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
package io.prestosql.plugin.blackhole;

import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public final class BlackHoleSplitManager
        implements ConnectorSplitManager
{
    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            SplitSchedulingStrategy splitSchedulingStrategy)
    {
        BlackHoleTableHandle tableHandle = (BlackHoleTableHandle) connectorTableHandle;
        int splitCount = tableHandle.getSplitCount();
        List<BlackHoleSplit> splits = nCopies(splitCount, BlackHoleSplit.INSTANCE);
        if (tableHandle.isCloseSplitSource()) {
            return new FixedSplitSource(splits);
        }
        return new NeverEndingSplitSource(splits);
    }

    public class NeverEndingSplitSource
            implements ConnectorSplitSource
    {
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final List<ConnectorSplit> splits;
        private int offset;

        public NeverEndingSplitSource(Iterable<? extends ConnectorSplit> splits)
        {
            requireNonNull(splits, "splits is null");
            List<ConnectorSplit> splitsList = new ArrayList<>();
            for (ConnectorSplit split : splits) {
                splitsList.add(split);
            }
            this.splits = Collections.unmodifiableList(splitsList);
        }

        @SuppressWarnings("ObjectEquality")
        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
        {
            checkArgument(partitionHandle.equals(NOT_PARTITIONED), "partitionHandle must be NOT_PARTITIONED");

            int remainingSplits = splits.size() - offset;
            int size = Math.min(remainingSplits, maxSize);
            List<ConnectorSplit> results = splits.subList(offset, offset + size);
            offset += size;
            return completedFuture(new ConnectorSplitBatch(results, offset >= size));
        }

        @Override
        public boolean isFinished()
        {
            return offset >= splits.size() && closed.get();
        }

        @Override
        public void close()
        {
            closed.set(true);
        }
    }
}
