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

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Represents a table identifier in Polaris (namespace + table name).
 */
public class PolarisTableIdentifier
{
    private final String namespace;
    private final String name;

    @JsonCreator
    public PolarisTableIdentifier(
            @JsonProperty("namespace") String namespace,
            @JsonProperty("name") String name)
    {
        this.namespace = requireNonNull(namespace, "namespace is null");
        this.name = requireNonNull(name, "name is null");
    }

    @JsonProperty
    public String getNamespace()
    {
        return namespace;
    }

    @JsonProperty
    public String getName()
    {
        return name;
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
        PolarisTableIdentifier that = (PolarisTableIdentifier) obj;
        return Objects.equals(namespace, that.namespace) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(namespace, name);
    }

    @Override
    public String toString()
    {
        return "PolarisTableIdentifier{" +
                "namespace='" + namespace + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
