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
import java.util.Arrays;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestOpenLineageListenerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OpenLineageListenerConfig.class)
                .setTransport(OpenLineageTransport.CONSOLE)
                .setTrinoURI(null)
                .setNamespace(null)
                .setDisabledFacets(ImmutableList.of())
                .setIncludeQueryTypes(Arrays.stream("ALTER_TABLE_EXECUTE,DELETE,INSERT,MERGE,UPDATE,DATA_DEFINITION".split(",")).toList()));
    }

    @Test
    public void testExplicitPropertyMappings()
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
                .setIncludeQueryTypes(ImmutableList.of("SELECT", "DELETE"))
                .setDisabledFacets(ImmutableList.of("trino_metadata", "trino_query_statistics"))
                .setNamespace("testnamespace");

        assertFullMapping(properties, expected);
    }
}
