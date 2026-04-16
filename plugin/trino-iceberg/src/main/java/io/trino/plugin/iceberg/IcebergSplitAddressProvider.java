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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.filesystem.cache.CachingHostAddressProvider;
import io.trino.plugin.iceberg.functions.tablechanges.TableChangesSplit;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitAddressProvider;

import java.util.List;

import static io.trino.filesystem.cache.CachingHostAddressProvider.getSplitKey;
import static java.util.Objects.requireNonNull;

public class IcebergSplitAddressProvider
        implements ConnectorSplitAddressProvider
{
    private final CachingHostAddressProvider cachingHostAddressProvider;

    @Inject
    public IcebergSplitAddressProvider(CachingHostAddressProvider cachingHostAddressProvider)
    {
        this.cachingHostAddressProvider = requireNonNull(cachingHostAddressProvider, "cachingHostAddressProvider is null");
    }

    @Override
    public List<HostAddress> getAddresses(ConnectorSplit split)
    {
        switch (split) {
            case IcebergSplit icebergSplit -> {
                return cachingHostAddressProvider.getHosts(
                        getSplitKey(icebergSplit.getPath(), icebergSplit.getStart(), icebergSplit.getLength()),
                        ImmutableList.of());
            }
            case TableChangesSplit tableChangesSplit -> {
                return ImmutableList.of();
            }

            default -> throw new IllegalStateException("Unexpected value: " + split);
        }
    }

    @Override
    public boolean isRemotelyAccessible(ConnectorSplit split)
    {
        return true;
    }
}
