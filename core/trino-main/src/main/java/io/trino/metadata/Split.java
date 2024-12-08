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
package io.trino.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public final class Split
{
    private static final int INSTANCE_SIZE = instanceSize(Split.class);

    private final CatalogHandle catalogHandle;
    private final ConnectorSplit connectorSplit;
    private final Optional<CacheSplitId> cacheSplitId;
    private final Optional<List<HostAddress>> addresses;
    /**
     * true if the split is executed on its preferred node (from its {@link ConnectorSplit#getAddresses()}.
     */
    private final boolean splitAddressEnforced;

    public Split(CatalogHandle catalogHandle, ConnectorSplit connectorSplit)
    {
        this(catalogHandle, connectorSplit, Optional.empty(), Optional.empty(), false);
    }

    @JsonCreator
    public Split(
            @JsonProperty("catalogHandle") CatalogHandle catalogHandle,
            @JsonProperty("connectorSplit") ConnectorSplit connectorSplit,
            @JsonProperty("cacheSplitId") Optional<CacheSplitId> cacheSplitId,
            @JsonProperty("splitAddressEnforced") boolean splitAddressEnforced)
    {
        this(catalogHandle, connectorSplit, cacheSplitId, Optional.empty(), splitAddressEnforced);
    }

    public Split(
            CatalogHandle catalogHandle,
            ConnectorSplit connectorSplit,
            Optional<CacheSplitId> cacheSplitId,
            Optional<List<HostAddress>> addresses,
            boolean splitAddressEnforced)
    {
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.connectorSplit = requireNonNull(connectorSplit, "connectorSplit is null");
        this.cacheSplitId = requireNonNull(cacheSplitId, "cacheSplitId is null");
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.splitAddressEnforced = splitAddressEnforced;
    }

    @JsonProperty
    public CatalogHandle getCatalogHandle()
    {
        return catalogHandle;
    }

    @JsonProperty
    public ConnectorSplit getConnectorSplit()
    {
        return connectorSplit;
    }

    @JsonProperty
    public Optional<CacheSplitId> getCacheSplitId()
    {
        return cacheSplitId;
    }

    @JsonIgnore
    public Map<String, String> getInfo()
    {
        return firstNonNull(connectorSplit.getSplitInfo(), ImmutableMap.of());
    }

    // do not serialize addresses as they are not needed on workers
    @JsonIgnore
    public List<HostAddress> getAddresses()
    {
        return addresses.orElse(connectorSplit.getAddresses());
    }

    // do not serialize remotelyAccessible flag as it is not needed on workers
    @JsonIgnore
    public boolean isRemotelyAccessible()
    {
        return connectorSplit.isRemotelyAccessible();
    }

    @JsonProperty
    public boolean isSplitAddressEnforced()
    {
        return splitAddressEnforced;
    }

    public SplitWeight getSplitWeight()
    {
        return connectorSplit.getSplitWeight();
    }

    public Split withSplitAddressEnforced(boolean splitAddressEnforced)
    {
        return new Split(this.catalogHandle, this.connectorSplit, this.cacheSplitId, this.addresses, splitAddressEnforced);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogHandle", catalogHandle)
                .add("connectorSplit", connectorSplit)
                .add("cacheSplitId", cacheSplitId)
                .add("addresses", addresses)
                .add("splitAddressEnforced", splitAddressEnforced)
                .toString();
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + catalogHandle.getRetainedSizeInBytes()
                + connectorSplit.getRetainedSizeInBytes()
                + sizeOf(cacheSplitId, CacheSplitId::getRetainedSizeInBytes)
                + sizeOf(addresses, value -> estimatedSizeOf(value, HostAddress::getRetainedSizeInBytes))
                + sizeOf(splitAddressEnforced);
    }
}
