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

import com.google.common.cache.CacheBuilder;
import com.google.inject.Inject;
import io.airlift.node.NodeInfo;
import io.trino.connector.ConnectorAwareNodeManager;
import io.trino.metadata.InternalNodeManager;
import io.trino.spi.connector.CatalogHandle;

import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static java.util.Objects.requireNonNull;

public class ConnectorAwareAddressProvider
{
    private final NonEvictableCache<CatalogHandle, ConsistentHashingAddressProvider> catalogAddressProvider;
    private final InternalNodeManager nodeManager;

    @Inject
    public ConnectorAwareAddressProvider(InternalNodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.catalogAddressProvider = buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(1000));
    }

    public ConsistentHashingAddressProvider getAddressProvider(NodeInfo nodeInfo, CatalogHandle catalogHandle, boolean schedulerIncludeCoordinator)
    {
        return uncheckedCacheGet(
                catalogAddressProvider,
                catalogHandle,
                () -> new ConsistentHashingAddressProvider(new ConnectorAwareNodeManager(nodeManager, nodeInfo.getEnvironment(), catalogHandle, schedulerIncludeCoordinator)));
    }
}
