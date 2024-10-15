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
package io.trino.plugin.openlineage.http;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.plugin.openlineage.transport.http.OpenLineageHttpTransportConfig;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

final class TestOpenLineageHttpTransportConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OpenLineageHttpTransportConfig.class)
                .setUrl(null)
                .setEndpoint(null)
                .setTimeout(Duration.valueOf("5s"))
                .setApiKey(null)
                .setHeaders(ImmutableList.of())
                .setUrlParams(ImmutableList.of()));
    }

    @Test
    void testExplicitPropertyMappings()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("openlineage-event-listener.transport.url", "http://testurl")
                .put("openlineage-event-listener.transport.endpoint", "/test/endpoint")
                .put("openlineage-event-listener.transport.api-key", "dummy")
                .put("openlineage-event-listener.transport.timeout", "30s")
                .put("openlineage-event-listener.transport.headers", "header1:value1,header2:value2")
                .put("openlineage-event-listener.transport.url-params", "urlParam1:urlVal1,urlParam2:urlVal2")

                .buildOrThrow();

        OpenLineageHttpTransportConfig expected = new OpenLineageHttpTransportConfig()
                .setUrl(new URI("http://testurl"))
                .setEndpoint("/test/endpoint")
                .setApiKey("dummy")
                .setTimeout(Duration.valueOf("30s"))
                .setHeaders(ImmutableList.of("header1:value1", "header2:value2"))
                .setUrlParams(ImmutableList.of("urlParam1:urlVal1", "urlParam2:urlVal2"));

        assertFullMapping(properties, expected);
    }
}
