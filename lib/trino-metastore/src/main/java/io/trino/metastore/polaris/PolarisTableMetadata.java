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

import static java.util.Objects.requireNonNull;

/**
 * Represents Iceberg table metadata from Polaris.
 * This corresponds to the response from the standard Iceberg REST API.
 */
public record PolarisTableMetadata(
        @JsonProperty("location") String location,
        @JsonProperty("schema") Map<String, Object> schema,
        @JsonProperty("properties") Map<String, String> properties)
{
    @JsonCreator
    public PolarisTableMetadata
    {
        requireNonNull(location, "location is null");
        schema = schema != null ? ImmutableMap.copyOf(schema) : ImmutableMap.of();
        properties = properties != null ? ImmutableMap.copyOf(properties) : ImmutableMap.of();
    }
}
