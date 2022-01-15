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
package io.trino.plugin.httpquery;

import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestHttpQueryListenerConfig
{
    @Test
    public void testDefaults()
            throws Exception
    {
        assertRecordedDefaults(recordDefaults(HttpEventListenerConfig.class)
                .setHttpHeaders(List.of())
                .setIngestUri(null)
                .setRetryCount(0)
                .setRetryDelay(Duration.succinctDuration(1, TimeUnit.SECONDS))
                .setMaxDelay(Duration.succinctDuration(1, TimeUnit.MINUTES))
                .setBackoffBase(2.0)
                .setLogCompleted(false)
                .setLogCreated(false)
                .setLogSplit(false));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws Exception
    {
        Map<String, String> properties = Map.of(
                "http-event-listener.log-created", "true",
                "http-event-listener.log-completed", "true",
                "http-event-listener.log-split", "true",
                "http-event-listener.connect-ingest-uri", "http://example.com:8080/api",
                "http-event-listener.connect-http-headers", "Authorization: Trust Me, Cache-Control: no-cache",
                "http-event-listener.connect-retry-count", "2",
                "http-event-listener.connect-retry-delay", "101s",
                "http-event-listener.connect-backoff-base", "1.5",
                "http-event-listener.connect-max-delay", "10m");

        HttpEventListenerConfig expected = new HttpEventListenerConfig()
                .setLogCompleted(true)
                .setLogCreated(true)
                .setLogSplit(true)
                .setIngestUri("http://example.com:8080/api")
                .setHttpHeaders(List.of("Authorization: Trust Me", "Cache-Control: no-cache"))
                .setRetryCount(2)
                .setRetryDelay(Duration.succinctDuration(101, TimeUnit.SECONDS))
                .setBackoffBase(1.5)
                .setMaxDelay(Duration.succinctDuration(10, TimeUnit.MINUTES));

        assertFullMapping(properties, expected);
    }
}
