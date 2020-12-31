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
package io.prestosql.plugin.kudu;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.connector.ConnectorSplitSource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class KuduBucketedSplitSource
        implements ConnectorSplitSource
{
    private final Map<Integer, KuduSplit> groupedSplits;

    KuduBucketedSplitSource(List<KuduSplit> splits)
    {
        this.groupedSplits = new ConcurrentHashMap<>();
        splits.forEach(split ->
                groupedSplits.put(split.getBucketNumber(), split));
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        KuduPartitionHandle kuduPartitionHandle = (KuduPartitionHandle) partitionHandle;
        KuduSplit kuduSplit = groupedSplits.remove(kuduPartitionHandle.getBucket());
        return completedFuture(new ConnectorSplitBatch(kuduSplit == null ? ImmutableList.of() : ImmutableList.of(kuduSplit), true));
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
