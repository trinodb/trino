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
package io.trino.plugin.exchange.filesystem.azure;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestExchangeAzureConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ExchangeAzureConfig.class)
                .setAzureStorageEndpoint(null)
                .setAzureStorageConnectionString(null)
                .setAzureStorageBlockSize(DataSize.of(4, MEGABYTE))
                .setMaxErrorRetries(10)
                .setMaxConnections(500)
                .setPendingAcquireMaxCount(1000)
                .setConnectionAcquisitionTimeout(new Duration(1, MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("exchange.azure.connection-string", "connection")
                .put("exchange.azure.endpoint", "endpoint")
                .put("exchange.azure.block-size", "8MB")
                .put("exchange.azure.max-error-retries", "8")
                .put("exchange.azure.max-connections", "200")
                .put("exchange.azure.pending-acquire-max-count", "5000")
                .put("exchange.azure.connection-acquisition-timeout", "30s")
                .buildOrThrow();

        ExchangeAzureConfig expected = new ExchangeAzureConfig()
                .setAzureStorageConnectionString("connection")
                .setAzureStorageEndpoint("endpoint")
                .setAzureStorageBlockSize(DataSize.of(8, MEGABYTE))
                .setMaxErrorRetries(8)
                .setMaxConnections(200)
                .setPendingAcquireMaxCount(5000)
                .setConnectionAcquisitionTimeout(new Duration(30, SECONDS));

        assertFullMapping(properties, expected);
    }
}
