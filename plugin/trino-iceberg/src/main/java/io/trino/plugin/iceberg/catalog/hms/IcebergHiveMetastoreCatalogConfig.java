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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS_DEFAULT;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH_DEFAULT;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES_DEFAULT;

public class IcebergHiveMetastoreCatalogConfig
{
    private boolean manifestCachingEnabled;
    private DataSize maxManifestCacheSize = DataSize.of(IO_MANIFEST_CACHE_MAX_TOTAL_BYTES_DEFAULT, BYTE);
    private Duration manifestCacheExpireDuration = new Duration(IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS_DEFAULT, MILLISECONDS);
    private DataSize manifestCacheMaxContentLength = DataSize.of(IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH_DEFAULT, BYTE);

    public boolean isManifestCachingEnabled()
    {
        return manifestCachingEnabled;
    }

    @Config("iceberg.hive.manifest.cache-enabled")
    @ConfigDescription("Enable/disable the manifest caching feature in Hive Metastore catalog")
    public IcebergHiveMetastoreCatalogConfig setManifestCachingEnabled(boolean manifestCachingEnabled)
    {
        this.manifestCachingEnabled = manifestCachingEnabled;
        return this;
    }

    public DataSize getMaxManifestCacheSize()
    {
        return maxManifestCacheSize;
    }

    @Config("iceberg.hive.manifest.cache.max-total-size")
    @ConfigDescription("Maximum total amount to cache in the manifest cache of Hive Metastore catalog")
    public IcebergHiveMetastoreCatalogConfig setMaxManifestCacheSize(DataSize maxManifestCacheSize)
    {
        this.maxManifestCacheSize = maxManifestCacheSize;
        return this;
    }

    public Duration getManifestCacheExpireDuration()
    {
        return manifestCacheExpireDuration;
    }

    @Config("iceberg.hive.manifest.cache.expiration-interval-duration")
    @ConfigDescription("Maximum duration for which an entry stays in the manifest cache of Hive Metastore catalog")
    public IcebergHiveMetastoreCatalogConfig setManifestCacheExpireDuration(Duration manifestCacheExpireDuration)
    {
        this.manifestCacheExpireDuration = manifestCacheExpireDuration;
        return this;
    }

    public DataSize getManifestCacheMaxContentLength()
    {
        return manifestCacheMaxContentLength;
    }

    @Config("iceberg.hive.manifest.cache.max-content-length")
    @ConfigDescription("Maximum length of a manifest file to be considered for caching in Hive Metastore catalog")
    public IcebergHiveMetastoreCatalogConfig setManifestCacheMaxContentLength(DataSize manifestCacheMaxContentLength)
    {
        this.manifestCacheMaxContentLength = manifestCacheMaxContentLength;
        return this;
    }
}
