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
import io.trino.connector.CatalogName;
import io.trino.execution.Lifespan;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class Split
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(Split.class).instanceSize();

    private final CatalogName catalogName;
    private final ConnectorSplit connectorSplit;
    private final Lifespan lifespan;

    @JsonCreator
    public Split(
            @JsonProperty("catalogName") CatalogName catalogName,
            @JsonProperty("connectorSplit") ConnectorSplit connectorSplit,
            @JsonProperty("lifespan") Lifespan lifespan)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.connectorSplit = requireNonNull(connectorSplit, "connectorSplit is null");
        this.lifespan = requireNonNull(lifespan, "lifespan is null");
    }

    @JsonProperty
    public CatalogName getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    public ConnectorSplit getConnectorSplit()
    {
        return connectorSplit;
    }

    @JsonProperty
    public Lifespan getLifespan()
    {
        return lifespan;
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
                .add("catalogName", catalogName)
                .add("connectorSplit", connectorSplit)
                .add("lifespan", lifespan)
                .toString();
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + catalogName.getRetainedSizeInBytes()
                + connectorSplit.getRetainedSizeInBytes()
                + lifespan.getRetainedSizeInBytes();
    }
}
