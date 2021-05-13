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
package io.trino.parquet.reader.cache;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestMetadataCacheConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(MetadataCacheConfig.class)
                .setMetadataCacheMaxEntries(0)
                .setMetadataCacheTtl(Duration.valueOf("1h")));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("parquet.metadata-cache.max-entries", "256")
                .put("parquet.metadata-cache.ttl", "100s")
                .build();

        MetadataCacheConfig expected = new MetadataCacheConfig()
                .setMetadataCacheMaxEntries(256)
                .setMetadataCacheTtl(Duration.valueOf("100s"));

        assertFullMapping(properties, expected);
    }
}
