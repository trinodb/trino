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
package io.trino.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.node.NodeInfo;
import io.trino.metadata.Split;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.split.SplitSource;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.plugin.memory.MemoryCacheManager.canonicalizePlanSignature;
import static java.util.Objects.requireNonNull;

/**
 * Assigns addresses provided by {@link CacheManager} to splits that
 * are to be cached.
 */
public class CacheSplitSource
        implements SplitSource
{
    private final ConnectorSplitManager splitManager;
    private final SplitSource delegate;
    private final ConsistentHashingAddressProvider addressProvider;
    private final String canonicalSignature;

    public CacheSplitSource(
            PlanSignature signature,
            ConnectorSplitManager splitManager,
            SplitSource delegate,
            ConnectorAwareAddressProvider connectorAwareAddressProvider,
            NodeInfo nodeInfo,
            boolean schedulerIncludeCoordinator)
    {
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.addressProvider = connectorAwareAddressProvider.getAddressProvider(nodeInfo, delegate.getCatalogHandle(), schedulerIncludeCoordinator);
        addressProvider.refreshHashRingIfNeeded();
        this.canonicalSignature = canonicalizePlanSignature(signature).toString();
    }

    @Override
    public CatalogHandle getCatalogHandle()
    {
        return delegate.getCatalogHandle();
    }

    @Override
    public ListenableFuture<SplitBatch> getNextBatch(int maxSize)
    {
        return transform(delegate.getNextBatch(maxSize), this::assignAddresses, directExecutor());
    }

    private SplitBatch assignAddresses(SplitBatch batch)
    {
        ImmutableList.Builder<Split> newBatch = ImmutableList.builder();
        for (Split split : batch.getSplits()) {
            Optional<CacheSplitId> splitId = splitManager.getCacheSplitId(split.getConnectorSplit());
            if (!split.getAddresses().isEmpty() || splitId.isEmpty()) {
                newBatch.add(
                        new Split(
                                split.getCatalogHandle(),
                                split.getConnectorSplit(),
                                splitId,
                                // do not override connector provided split addresses
                                Optional.empty(),
                                Optional.empty(),
                                split.getFailoverHappened()));
            }
            else {
                newBatch.add(
                        new Split(
                                split.getCatalogHandle(),
                                split.getConnectorSplit(),
                                splitId,
                                Optional.of(false),
                                Optional.of(ImmutableList.of(addressProvider.getPreferredAddress(canonicalSignature + splitId))),
                                split.getFailoverHappened()));
            }
        }
        return new SplitBatch(newBatch.build(), batch.isLastBatch());
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
