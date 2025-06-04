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
package io.trino.server.protocol.spooling;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

class TestQueryDataEncodingConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(QueryDataEncodingConfig.class)
                .setJsonEnabled(true)
                .setJsonLz4Enabled(true)
                .setJsonZstdEnabled(true)
                .setCompressionThreshold(DataSize.of(8, KILOBYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("protocol.spooling.encoding.json.enabled", "false")
                .put("protocol.spooling.encoding.json+lz4.enabled", "false")
                .put("protocol.spooling.encoding.json+zstd.enabled", "false")
                .put("protocol.spooling.encoding.compression.threshold", "1MB")
                .buildOrThrow();

        QueryDataEncodingConfig expected = new QueryDataEncodingConfig()
                .setJsonEnabled(false)
                .setJsonLz4Enabled(false)
                .setJsonZstdEnabled(false)
                .setCompressionThreshold(DataSize.of(1, MEGABYTE));

        assertFullMapping(properties, expected);
    }
}
