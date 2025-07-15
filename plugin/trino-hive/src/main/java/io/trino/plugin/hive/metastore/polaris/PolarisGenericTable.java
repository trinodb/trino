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
package io.trino.plugin.hive.metastore.polaris;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Represents a generic table in Polaris (non-Iceberg tables like Delta Lake, CSV, etc.).
 * Based on the Polaris Generic Table API specification.
 */
public class PolarisGenericTable
{
    private final String name;
    private final String format;
    private final Optional<String> baseLocation;
    private final Optional<String> doc;
    private final Map<String, String> properties;

    @JsonCreator
    public PolarisGenericTable(
            @JsonProperty("name") String name,
            @JsonProperty("format") String format,
            @JsonProperty("base-location") String baseLocation,
            @JsonProperty("doc") String doc,
            @JsonProperty("properties") Map<String, String> properties)
    {
        this.name = requireNonNull(name, "name is null");
        this.format = requireNonNull(format, "format is null");
        this.baseLocation = Optional.ofNullable(baseLocation);
        this.doc = Optional.ofNullable(doc);
        this.properties = properties != null ? ImmutableMap.copyOf(properties) : ImmutableMap.of();
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getFormat()
    {
        return format;
    }

    @JsonProperty("base-location")
    public Optional<String> getBaseLocation()
    {
        return baseLocation;
    }

    @JsonProperty
    public Optional<String> getDoc()
    {
        return doc;
    }

    @JsonProperty
    public Map<String, String> getProperties()
    {
        return properties;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PolarisGenericTable that = (PolarisGenericTable) obj;
        return Objects.equals(name, that.name) &&
                Objects.equals(format, that.format) &&
                Objects.equals(baseLocation, that.baseLocation) &&
                Objects.equals(doc, that.doc) &&
                Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, format, baseLocation, doc, properties);
    }

    @Override
    public String toString()
    {
        return "PolarisGenericTable{" +
                "name='" + name + '\'' +
                ", format='" + format + '\'' +
                ", baseLocation=" + baseLocation +
                ", doc=" + doc +
                ", properties=" + properties +
                '}';
    }
}
