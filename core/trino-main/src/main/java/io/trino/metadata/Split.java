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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public record Split(CatalogHandle catalogHandle, ConnectorSplit connectorSplit)
{
    private static final int INSTANCE_SIZE = instanceSize(Split.class);

    public Split
    {
        requireNonNull(catalogHandle, "catalogHandle is null");
        requireNonNull(connectorSplit, "connectorSplit is null");
    }

    @JsonIgnore
    public Map<String, String> getInfo()
    {
        return firstNonNull(connectorSplit.getSplitInfo(), ImmutableMap.of());
    }

    @JsonIgnore
    public List<HostAddress> getAddresses()
    {
        return connectorSplit.getAddresses();
    }

    @JsonIgnore
    public boolean isRemotelyAccessible()
    {
        return connectorSplit.isRemotelyAccessible();
    }

    @JsonIgnore
    public SplitWeight getSplitWeight()
    {
        return connectorSplit.getSplitWeight();
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + catalogHandle.getRetainedSizeInBytes()
                + connectorSplit.getRetainedSizeInBytes();
    }
}
