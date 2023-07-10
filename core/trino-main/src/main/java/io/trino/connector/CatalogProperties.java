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
package io.trino.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.CatalogHandle;

import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CatalogProperties
{
    private final CatalogHandle catalogHandle;
    private final ConnectorName connectorName;
    private final Map<String, String> properties;

    @JsonCreator
    public CatalogProperties(
            @JsonProperty("catalogHandle") CatalogHandle catalogHandle,
            @JsonProperty("connectorName") ConnectorName connectorName,
            @JsonProperty("properties") Map<String, String> properties)
    {
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
        this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
    }

    @JsonProperty
    public CatalogHandle getCatalogHandle()
    {
        return catalogHandle;
    }

    @JsonProperty
    public ConnectorName getConnectorName()
    {
        return connectorName;
    }

    @JsonProperty
    public Map<String, String> getProperties()
    {
        return properties;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogHandle", catalogHandle)
                .add("connectorName", connectorName)
                .toString();
    }
}
