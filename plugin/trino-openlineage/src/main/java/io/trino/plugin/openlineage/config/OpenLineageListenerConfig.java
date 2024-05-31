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
package io.trino.plugin.openlineage.config;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.trino.plugin.openlineage.OpenLineageTransport;
import io.trino.plugin.openlineage.OpenLineageTrinoFacet;
import io.trino.spi.resourcegroups.QueryType;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class OpenLineageListenerConfig
{
    private OpenLineageTransport transport = OpenLineageTransport.CONSOLE;
    private URI trinoURI;
    private List<OpenLineageTrinoFacet> disabledFacets = ImmutableList.of();
    private Optional<String> namespace = Optional.empty();

    private Set<QueryType> includeQueryTypes = ImmutableSet.<QueryType>builder()
            .add(QueryType.ALTER_TABLE_EXECUTE)
            .add(QueryType.DELETE)
            .add(QueryType.INSERT)
            .add(QueryType.MERGE)
            .add(QueryType.UPDATE)
            .add(QueryType.DATA_DEFINITION)
            .build();

    @Config("openlineage-event-listener.transport.type")
    @ConfigDescription("Type of transport used to emit lineage information.")
    public OpenLineageListenerConfig setTransport(OpenLineageTransport transport)
    {
        this.transport = transport;
        return this;
    }

    public OpenLineageTransport getTransport()
    {
        return transport;
    }

    @Config("openlineage-event-listener.trino.uri")
    @ConfigDescription("URI of trino server. Used for namespace rendering.")
    public OpenLineageListenerConfig setTrinoURI(URI trinoURI)
    {
        this.trinoURI = trinoURI;
        return this;
    }

    @NotNull
    public URI getTrinoURI()
    {
        return trinoURI;
    }

    @Config("openlineage-event-listener.trino.include-query-types")
    @ConfigDescription("Which query types emitted by Trino should generate OpenLineage events. Other query types will be filtered out.")
    public OpenLineageListenerConfig setIncludeQueryTypes(List<String> includeQueryTypes)
    {
        this.includeQueryTypes = new HashSet<>(includeQueryTypes.stream()
                .map(String::trim)
                .map(QueryType::valueOf)
                .toList());

        return this;
    }

    public Set<QueryType> getIncludeQueryTypes()
    {
        return includeQueryTypes;
    }

    @Config("openlineage-event-listener.disabled-facets")
    @ConfigDescription("Which facets should be removed from OpenLineage events.")
    public OpenLineageListenerConfig setDisabledFacets(List<String> disabledFacets)
            throws RuntimeException
    {
        this.disabledFacets = disabledFacets.stream()
                .map(String::trim)
                .map(text -> {
                    try {
                        return OpenLineageTrinoFacet.fromText(text);
                    }
                    catch (IllegalArgumentException e) {
                        throw new RuntimeException(e);
                    }
                })
                .toList();

        return this;
    }

    public List<OpenLineageTrinoFacet> getDisabledFacets()
    {
        return disabledFacets;
    }

    @Config("openlineage-event-listener.namespace")
    @ConfigDescription("Override default namespace for job facet.")
    public OpenLineageListenerConfig setNamespace(String namespace)
    {
        this.namespace = Optional.ofNullable(namespace);
        return this;
    }

    public Optional<String> getNamespace()
    {
        return namespace;
    }
}
