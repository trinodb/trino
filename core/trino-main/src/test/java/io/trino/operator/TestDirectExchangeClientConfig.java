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
package io.trino.operator;

import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit;

public class TestDirectExchangeClientConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DirectExchangeClientConfig.class)
                .setMaxBufferSize(DataSize.of(32, Unit.MEGABYTE))
                .setConcurrentRequestMultiplier(3)
                .setMaxErrorDuration(new Duration(1, TimeUnit.MINUTES))
                .setMaxResponseSize(new HttpClientConfig().getMaxContentLength())
                .setPageBufferClientMaxCallbackThreads("25")
                .setClientThreads("25")
                .setAcknowledgePages(true)
                .setDeduplicationBufferSize(DataSize.of(32, Unit.MEGABYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("exchange.max-buffer-size", "1GB")
                .put("exchange.concurrent-request-multiplier", "13")
                .put("exchange.max-error-duration", "33s")
                .put("exchange.max-response-size", "1MB")
                .put("exchange.client-threads", "2")
                .put("exchange.page-buffer-client.max-callback-threads", "16")
                .put("exchange.acknowledge-pages", "false")
                .put("exchange.deduplication-buffer-size", "2MB")
                .buildOrThrow();

        DirectExchangeClientConfig expected = new DirectExchangeClientConfig()
                .setMaxBufferSize(DataSize.of(1, Unit.GIGABYTE))
                .setConcurrentRequestMultiplier(13)
                .setMaxErrorDuration(new Duration(33, TimeUnit.SECONDS))
                .setMaxResponseSize(DataSize.of(1, Unit.MEGABYTE))
                .setClientThreads("2")
                .setPageBufferClientMaxCallbackThreads("16")
                .setAcknowledgePages(false)
                .setDeduplicationBufferSize(DataSize.of(2, Unit.MEGABYTE));

        assertFullMapping(properties, expected);
    }
}
