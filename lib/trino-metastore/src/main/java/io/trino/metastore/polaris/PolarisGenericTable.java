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
package io.trino.metastore.polaris;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Represents a generic table in Polaris (non-Iceberg tables like Delta Lake, CSV, etc.).
 * Based on the Polaris Generic Table API specification.
 */
public record PolarisGenericTable(
        @JsonProperty("name") String name,
        @JsonProperty("format") String format,
        @JsonProperty("base-location") Optional<String> baseLocation,
        @JsonProperty("doc") Optional<String> doc,
        @JsonProperty("properties") Map<String, String> properties)
{
    @JsonCreator
    public PolarisGenericTable(
            @JsonProperty("name") String name,
            @JsonProperty("format") String format,
            @JsonProperty("base-location") String baseLocation,
            @JsonProperty("doc") String doc,
            @JsonProperty("properties") Map<String, String> properties)
    {
        this(requireNonNull(name, "name is null"),
                requireNonNull(format, "format is null"),
                Optional.ofNullable(baseLocation),
                Optional.ofNullable(doc),
                properties != null ? ImmutableMap.copyOf(properties) : ImmutableMap.of());
    }
}
