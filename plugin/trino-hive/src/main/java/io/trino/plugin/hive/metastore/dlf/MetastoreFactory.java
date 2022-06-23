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
package io.trino.plugin.hive.metastore.dlf;

import com.aliyun.datalake.metastore.common.CacheDataLakeMetaStore;
import com.aliyun.datalake.metastore.common.CacheDataLakeMetaStoreConfig;
import com.aliyun.datalake.metastore.common.DataLakeConfig;
import com.aliyun.datalake.metastore.common.DefaultDataLakeMetaStore;
import com.aliyun.datalake.metastore.common.IDataLakeMetaStore;
import com.aliyun.teaopenapi.models.Config;
import io.trino.spi.TrinoException;

import java.util.Properties;

import static com.aliyun.datalake.metastore.common.DataLakeConfig.CATALOG_ACCURATE_BATCH_SIZE;
import static com.aliyun.datalake.metastore.common.DataLakeConfig.CATALOG_AK_MODE;
import static com.aliyun.datalake.metastore.common.DataLakeConfig.CATALOG_STS_IS_NEW_MODE;
import static com.aliyun.datalake.metastore.common.DataLakeConfig.CATALOG_TABLE_COL_STATS_PAGE_SIZE;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;

public class MetastoreFactory
{
    public IDataLakeMetaStore getMetaStore(AlibabaDlfMetaStoreConfig dlfConfig) throws TrinoException
    {
        DataLakeConfig.AKMode akMode = dlfConfig.getAkMode();
        String endpoint = dlfConfig.getEndpoint().orElse(null);
        String accessKeyId = dlfConfig.getAccessKeyId().orElse(null);
        String accessKeySecret = dlfConfig.getAccessKeySecret().orElse(null);
        String regionId = dlfConfig.getRegionId();

        Properties extendedConfig = new Properties();
        if (akMode != null) {
            extendedConfig.setProperty(CATALOG_AK_MODE, akMode.name());
        }

        extendedConfig.setProperty(CATALOG_ACCURATE_BATCH_SIZE, String.valueOf(dlfConfig.getBatchSize()));
        extendedConfig.setProperty(CATALOG_STS_IS_NEW_MODE, String.valueOf(dlfConfig.isNewStsMode()));
        extendedConfig.setProperty(CATALOG_TABLE_COL_STATS_PAGE_SIZE, String.valueOf(dlfConfig.getTableColStatsBatchSize()));

        try {
            Config config = new Config();
            config.accessKeyId = accessKeyId;
            config.accessKeySecret = accessKeySecret;
            config.endpoint = endpoint;
            config.regionId = regionId;
            config.readTimeout = dlfConfig.getReadTimeout();
            config.connectTimeout = dlfConfig.getConnTimeout();

            IDataLakeMetaStore dataLakeMetaStore = new DefaultDataLakeMetaStore(config, extendedConfig, null);
            //create cache metastore
            if (isCacheEnabled(dlfConfig)) {
                boolean databaseCacheEnabled = dlfConfig.getDbCacheEnable().orElse(false);
                int dbCacheSize = dlfConfig.getDbCacheSize().orElse(0);
                int dbCacheTtlMins = dlfConfig.getDbCacheTTLMins().orElse(0);
                boolean tableCacheEnabled = dlfConfig.getTbCacheEnable().orElse(false);
                int tbCacheSize = dlfConfig.getTbCacheSize().orElse(0);
                int tbCacheTtlMins = dlfConfig.getTbCacheTTLMins().orElse(0);
                CacheDataLakeMetaStoreConfig cacheConfig = new CacheDataLakeMetaStoreConfig(databaseCacheEnabled, dbCacheSize, dbCacheTtlMins, tableCacheEnabled, tbCacheSize, tbCacheTtlMins);
                return new CacheDataLakeMetaStore(cacheConfig, dataLakeMetaStore);
            }
            return dataLakeMetaStore;
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Initialize DlfMetaStoreClient failed: " + e.getMessage(), e);
        }
    }

    private boolean isCacheEnabled(AlibabaDlfMetaStoreConfig dlfConfig)
    {
        boolean databaseCacheEnabled = dlfConfig.getDbCacheEnable().orElse(false);
        boolean tableCacheEnabled = dlfConfig.getTbCacheEnable().orElse(false);
        return (databaseCacheEnabled || tableCacheEnabled);
    }
}
