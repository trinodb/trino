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
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public final class Split
{
    private static final int INSTANCE_SIZE = instanceSize(Split.class);

    private final CatalogHandle catalogHandle;
    private final ConnectorSplit connectorSplit;

    @JsonCreator
    public Split(
            @JsonProperty("catalogHandle") CatalogHandle catalogHandle,
            @JsonProperty("connectorSplit") ConnectorSplit connectorSplit)
    {
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.connectorSplit = requireNonNull(connectorSplit, "connectorSplit is null");
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

    public Object getInfo()
    {
        return connectorSplit.getInfo();
    }

    public List<HostAddress> getAddresses()
    {
        return connectorSplit.getAddresses();
    }

    public boolean isRemotelyAccessible()
    {
        return connectorSplit.isRemotelyAccessible();
    }

    public SplitWeight getSplitWeight()
    {
        return connectorSplit.getSplitWeight();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogHandle", catalogHandle)
                .add("connectorSplit", connectorSplit)
                .toString();
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + catalogHandle.getRetainedSizeInBytes()
                + connectorSplit.getRetainedSizeInBytes();
    }
}
