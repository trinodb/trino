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

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestAlibabaDlfHiveMetastoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AlibabaDlfMetaStoreConfig.class)
                .setRegionId("cn-hangzhou")
                .setEndpoint(null)
                .setAkMode("MANUAL")
                .setProxyMode("DLF_ONLY")
                .setUserId(null)
                .setAssumeCanonicalPartitionKeys(null)
                .setBatchSize(60)
                .setBatchSizeForGetPartititon(1000)
                .setDbCacheEnable(null)
                .setDbCacheSize(null)
                .setDbCacheTTLMins(null)
                .setEnableBitVector(false)
                .setEnableFileOperation(false)
                .setEnableFileOperationGrayRate(0.0)
                .setEnableRecordActionLog(true)
                .setNewStsMode(true)
                .setPageSize((short) 500)
                .setTbCacheEnable(null)
                .setTbCacheSize(null)
                .setTbCacheTTLMins(null)
                .setTableColStatsBatchSize(50)
                .setDeleteDirWhenDropSchema(true)
                .setAccessKeyId(null)
                .setAccessKeySecret(null)
                .setLogStore(null)
                .setDefaultWarehouseDir("/user/hive/warehouse")
                .setEnableRecordLog(true)
                .setHiveMetastoreAuthenticationType(null)
                .setHiveMetastoreClientKeytab(null)
                .setHiveMetastoreServiceClientPrincipal(null)
                .setHiveMetastoreServicePrincipal(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("alibaba-dlf.catalog.region", "cn-shanghai")
                .put("alibaba-dlf.catalog.endpoint", "dlf.cn-shanghai.aliyuncs.com")
                .put("alibaba-dlf.catalog.akMode", "EMR_AUTO")
                .put("alibaba-dlf.catalog.proxyMode", "METASTORE_DLF_SUCCESS")
                .put("alibaba-dlf.catalog.uid", "12345")
                .put("data.lake.cache.db.enable", "true")
                .put("data.lake.cache.db.size", "1")
                .put("data.lake.cache.db.ttl.mins", "1")
                .put("data.lake.cache.tb.enable", "true")
                .put("data.lake.cache.tb.size", "1")
                .put("data.lake.cache.tb.ttl.mins", "1")
                .put("alibaba-dlf.catalog.accessKeyId", "AAA")
                .put("alibaba-dlf.catalog.accessKeySecret", "BBB")
                .put("alibaba-dlf.catalog.accurate.batchSize", "50")
                .put("alibaba-dlf.catalog.action.log.enabled", "false")
                .put("alibaba-dlf.catalog.assume-canonical-partition-keys", "true")
                .put("alibaba-dlf.catalog.client.col.stats.pageSize", "400")
                .put("alibaba-dlf.catalog.client.table.col.stats.pageSize", "40")
                .put("alibaba-dlf.catalog.default-warehouse-dir", "/user/hadoop/warehouse")
                .put("alibaba-dlf.catalog.delete-dir-when-drop-schema", "false")
                .put("alibaba-dlf.catalog.enable.bit.vector", "true")
                .put("alibaba-dlf.catalog.enable.file.operation", "true")
                .put("alibaba-dlf.catalog.enable.file.operation.gray.rate", "1.0")
                .put("alibaba-dlf.catalog.log.enabled", "false")
                .put("alibaba-dlf.catalog.pageSize", "40")
                .put("alibaba-dlf.catalog.proxyLogStore", "store")
                .put("alibaba-dlf.catalog.sts.isNewMode", "false")
                .put("hive.metastore.authentication.type", "")
                .put("hive.metastore.client.keytab", "")
                .put("hive.metastore.client.principal", "")
                .put("hive.metastore.service.principal", "")
                .buildOrThrow();

        AlibabaDlfMetaStoreConfig expected = new AlibabaDlfMetaStoreConfig()
                .setRegionId("cn-shanghai")
                .setEndpoint("dlf.cn-shanghai.aliyuncs.com")
                .setAkMode("EMR_AUTO")
                .setProxyMode("METASTORE_DLF_SUCCESS")
                .setUserId("12345")
                .setAssumeCanonicalPartitionKeys(true)
                .setBatchSize(50)
                .setBatchSizeForGetPartititon(400)
                .setDbCacheEnable(true)
                .setDbCacheSize(1)
                .setDbCacheTTLMins(1)
                .setEnableBitVector(true)
                .setEnableFileOperation(true)
                .setEnableFileOperationGrayRate(1.0)
                .setEnableRecordActionLog(false)
                .setNewStsMode(false)
                .setPageSize((short) 40)
                .setTbCacheEnable(true)
                .setTbCacheSize(1)
                .setTbCacheTTLMins(1)
                .setTableColStatsBatchSize(40)
                .setDeleteDirWhenDropSchema(false)
                .setAccessKeyId("AAA")
                .setAccessKeySecret("BBB")
                .setLogStore("store")
                .setDefaultWarehouseDir("/user/hadoop/warehouse")
                .setEnableRecordLog(false)
                .setHiveMetastoreAuthenticationType("")
                .setHiveMetastoreClientKeytab("")
                .setHiveMetastoreServiceClientPrincipal("")
                .setHiveMetastoreServicePrincipal("");

        assertFullMapping(properties, expected);
    }
}
