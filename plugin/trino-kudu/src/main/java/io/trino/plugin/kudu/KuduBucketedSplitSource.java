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
package io.trino.plugin.kudu;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorPartitionHandle;
import io.trino.spi.connector.ConnectorSplitSource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.groupingBy;

public class KuduBucketedSplitSource
        implements ConnectorSplitSource
{
    private final Map<Integer, List<KuduSplit>> groupedSplits;

    KuduBucketedSplitSource(List<KuduSplit> splits)
    {
        Map<Integer, List<KuduSplit>> map = splits.stream()
                .collect(groupingBy(KuduSplit::getBucketNumber));
        this.groupedSplits = new ConcurrentHashMap<>(map);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        KuduPartitionHandle kuduPartitionHandle = (KuduPartitionHandle) partitionHandle;
        List<KuduSplit> kuduSplits = groupedSplits.remove(kuduPartitionHandle.getBucket());
        return completedFuture(new ConnectorSplitBatch(kuduSplits == null ? ImmutableList.of() : ImmutableList.copyOf(kuduSplits), true));
    }

    @Override
    public void close()
    {
    }

    @Override
    public boolean isFinished()
    {
        return groupedSplits.isEmpty();
    }
}
