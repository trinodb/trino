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
package io.trino.plugin.varada.dispatcher.cache;

import io.trino.spi.NodeManager;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.PlanSignature;

public class CoordinatorCacheManager
        implements CacheManager
{
    @Override
    public SplitCache getSplitCache(PlanSignature signature)
    {
        throw new UnsupportedOperationException("getSplitCache should read only in worker");
    }

    @SuppressWarnings("deprecation")
    @Override
    public PreferredAddressProvider getPreferredAddressProvider(PlanSignature signature, NodeManager nodeManager)
    {
        return new WarpPreferredAddressProvider(nodeManager);
    }

    @Override
    public long revokeMemory(long bytesToRevoke)
    {
        return 0;
    }
}
