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
package io.trino.plugin.openlineage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.openlineage.config.OpenLineageListenerConfig;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.trino.plugin.openlineage.OpenLineageTrinoFacet.TRINO_METADATA;
import static io.trino.plugin.openlineage.OpenLineageTrinoFacet.TRINO_QUERY_STATISTICS;
import static io.trino.spi.resourcegroups.QueryType.ALTER_TABLE_EXECUTE;
import static io.trino.spi.resourcegroups.QueryType.DATA_DEFINITION;
import static io.trino.spi.resourcegroups.QueryType.DELETE;
import static io.trino.spi.resourcegroups.QueryType.INSERT;
import static io.trino.spi.resourcegroups.QueryType.MERGE;
import static io.trino.spi.resourcegroups.QueryType.SELECT;
import static io.trino.spi.resourcegroups.QueryType.UPDATE;

final class TestOpenLineageListenerConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OpenLineageListenerConfig.class)
                .setTransport(OpenLineageTransport.CONSOLE)
                .setTrinoURI(null)
                .setNamespace(null)
                .setDisabledFacets(ImmutableList.of())
                .setIncludeQueryTypes(ImmutableList.of(
                        ALTER_TABLE_EXECUTE.name(),
                        DELETE.name(),
                        INSERT.name(),
                        MERGE.name(),
                        UPDATE.name(),
                        DATA_DEFINITION.name())));
    }

    @Test
    void testExplicitPropertyMappings()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("openlineage-event-listener.transport.type", "HTTP")
                .put("openlineage-event-listener.trino.uri", "http://testtrino")
                .put("openlineage-event-listener.trino.include-query-types", "SELECT,DELETE")
                .put("openlineage-event-listener.disabled-facets", "trino_metadata,trino_query_statistics")
                .put("openlineage-event-listener.namespace", "testnamespace")
                .buildOrThrow();

        OpenLineageListenerConfig expected = new OpenLineageListenerConfig()
                .setTransport(OpenLineageTransport.HTTP)
                .setTrinoURI(new URI("http://testtrino"))
                .setIncludeQueryTypes(ImmutableList.of(SELECT.name(), DELETE.name()))
                .setDisabledFacets(ImmutableList.of(TRINO_METADATA.name(), TRINO_QUERY_STATISTICS.name()))
                .setNamespace("testnamespace");

        assertFullMapping(properties, expected);
    }
}
