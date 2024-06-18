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
package io.trino.plugin.iceberg.catalog.hms;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS_DEFAULT;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH_DEFAULT;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES_DEFAULT;

public class TestIcebergHiveMetastoreCatalogConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(IcebergHiveMetastoreCatalogConfig.class)
                .setManifestCachingEnabled(false)
                .setMaxManifestCacheSize(DataSize.of(IO_MANIFEST_CACHE_MAX_TOTAL_BYTES_DEFAULT, BYTE))
                .setManifestCacheExpireDuration(new Duration(IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS_DEFAULT, MILLISECONDS))
                .setManifestCacheMaxContentLength(DataSize.of(IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH_DEFAULT, BYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("iceberg.hive.manifest.cache-enabled", "true")
                .put("iceberg.hive.manifest.cache.max-total-size", "1GB")
                .put("iceberg.hive.manifest.cache.expiration-interval-duration", "10m")
                .put("iceberg.hive.manifest.cache.max-content-length", "10MB")
                .buildOrThrow();

        IcebergHiveMetastoreCatalogConfig expected = new IcebergHiveMetastoreCatalogConfig()
                .setManifestCachingEnabled(true)
                .setMaxManifestCacheSize(DataSize.of(1, GIGABYTE))
                .setManifestCacheExpireDuration(new Duration(10, MINUTES))
                .setManifestCacheMaxContentLength(DataSize.of(10, MEGABYTE));

        assertFullMapping(properties, expected);
    }
}
