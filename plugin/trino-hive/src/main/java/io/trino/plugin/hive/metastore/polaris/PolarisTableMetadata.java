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

import static java.util.Objects.requireNonNull;

/**
 * Represents Iceberg table metadata from Polaris.
 * This corresponds to the response from the standard Iceberg REST API.
 */
public class PolarisTableMetadata
{
    private final String location;
    private final Map<String, Object> schema;
    private final Map<String, String> properties;

    @JsonCreator
    public PolarisTableMetadata(
            @JsonProperty("location") String location,
            @JsonProperty("schema") Map<String, Object> schema,
            @JsonProperty("properties") Map<String, String> properties)
    {
        this.location = requireNonNull(location, "location is null");
        this.schema = schema != null ? ImmutableMap.copyOf(schema) : ImmutableMap.of();
        this.properties = properties != null ? ImmutableMap.copyOf(properties) : ImmutableMap.of();
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    @JsonProperty
    public Map<String, Object> getSchema()
    {
        return schema;
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
        PolarisTableMetadata that = (PolarisTableMetadata) obj;
        return Objects.equals(location, that.location) &&
                Objects.equals(schema, that.schema) &&
                Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(location, schema, properties);
    }

    @Override
    public String toString()
    {
        return "PolarisTableMetadata{" +
                "location='" + location + '\'' +
                ", schema=" + schema +
                ", properties=" + properties +
                '}';
    }
}
