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
package io.trino.server;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestServerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ServerConfig.class)
                .setCoordinator(true)
                .setConcurrentStartup(false)
                .setIncludeExceptionInResponse(true)
                .setGracePeriod(new Duration(2, MINUTES))
                .setQueryResultsCompressionEnabled(true)
                .setQueryInfoUrlTemplate(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("coordinator", "false")
                .put("experimental.concurrent-startup", "true")
                .put("http.include-exception-in-response", "false")
                .put("shutdown.grace-period", "5m")
                .put("query-results.compression-enabled", "false")
                .put("query.info-url-template", "https://example.com/query/${QUERY_ID}")
                .buildOrThrow();

        ServerConfig expected = new ServerConfig()
                .setCoordinator(false)
                .setConcurrentStartup(true)
                .setIncludeExceptionInResponse(false)
                .setGracePeriod(new Duration(5, MINUTES))
                .setQueryResultsCompressionEnabled(false)
                .setQueryInfoUrlTemplate("https://example.com/query/${QUERY_ID}");

        assertFullMapping(properties, expected);
    }
}
