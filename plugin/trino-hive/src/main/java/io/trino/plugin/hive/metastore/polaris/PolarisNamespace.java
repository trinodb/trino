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
 * Represents a Polaris namespace (equivalent to a database in Hive).
 */
public class PolarisNamespace
{
    private final String name;
    private final Map<String, String> properties;

    @JsonCreator
    public PolarisNamespace(
            @JsonProperty("name") String name,
            @JsonProperty("properties") Map<String, String> properties)
    {
        this.name = requireNonNull(name, "name is null");
        this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
    }

    @JsonProperty
    public String getName()
    {
        return name;
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
        PolarisNamespace that = (PolarisNamespace) obj;
        return Objects.equals(name, that.name) &&
                Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, properties);
    }

    @Override
    public String toString()
    {
        return "PolarisNamespace{" +
                "name='" + name + '\'' +
                ", properties=" + properties +
                '}';
    }
}
