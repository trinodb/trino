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
package io.trino.plugin.opa.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.QueryId;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static java.util.Objects.requireNonNull;

@JsonInclude(NON_NULL)
public record OpaQueryContext(
        TrinoIdentity identity,
        OpaPluginContext softwareStack,
        Map<String, String> properties,
        Optional<QueryId> queryId,
        @JsonProperty("requestHeaders")
        Optional<Map<String, List<String>>> requestHeaders)
{
    public OpaQueryContext(
            TrinoIdentity identity,
            OpaPluginContext softwareStack,
            Map<String, String> properties,
            Optional<QueryId> queryId)
    {
        this(identity, softwareStack, properties, queryId, Optional.empty());
    }

    public OpaQueryContext
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(softwareStack, "softwareStack is null");
        properties = ImmutableMap.copyOf(properties);
        requireNonNull(queryId, "queryId is null");
        if (requestHeaders.isPresent()) {
            Map<String, List<String>> headers = new java.util.HashMap<>();
            for (Map.Entry<String, List<String>> entry : requestHeaders.get().entrySet()) {
                headers.put(entry.getKey(), java.util.List.copyOf(entry.getValue()));
            }
            requestHeaders = Optional.of(ImmutableMap.copyOf(headers));
        }
    }
}
