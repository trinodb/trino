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
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestExchangeAzureConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ExchangeAzureConfig.class)
                .setAzureStorageConnectionString(null)
                .setAzureStorageBlockSize(DataSize.of(4, MEGABYTE))
                .setMaxErrorRetries(10));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("exchange.azure.connection-string", "connection")
                .put("exchange.azure.block-size", "8MB")
                .put("exchange.azure.max-error-retries", "8")
                .buildOrThrow();

        ExchangeAzureConfig expected = new ExchangeAzureConfig()
                .setAzureStorageConnectionString("connection")
                .setAzureStorageBlockSize(DataSize.of(8, MEGABYTE))
                .setMaxErrorRetries(8);

        assertFullMapping(properties, expected);
    }
}
