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

import com.aliyun.datalake.metastore.common.DataLakeConfig;
import com.aliyun.datalake.metastore.common.ProxyMode;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.util.Optional;

public class AlibabaDlfMetaStoreConfig
{
    public Optional<String> hiveMetastoreAuthenticationType = Optional.empty();
    public Optional<String> hiveMetastoreServiceClientPrincipal = Optional.empty();
    public Optional<String> hiveMetastoreServicePrincipal = Optional.empty();
    public Optional<String> hiveMetastoreClientKeytab = Optional.empty();
    public Boolean enableBitVector = false;
    public Integer batchSizeForGetPartititon = 1000;
    private String regionId = "cn-hangzhou";
    private Optional<String> endpoint = Optional.empty();
    private Optional<String> logStore = Optional.empty();
    private Optional<String> userId = Optional.empty();
    private Optional<String> accessKeyId = Optional.empty();
    private Optional<String> accessKeySecret = Optional.empty();
    private String defaultWarehouseDir = "/user/hive/warehouse";
    private DataLakeConfig.AKMode akMode = DataLakeConfig.AKMode.MANUAL;
    private boolean isNewStsMode = true;
    private ProxyMode proxyMode = ProxyMode.DLF_ONLY;
    private short pageSize = 500;
    // according to hive default retrieve size, default to 60 is better for large table
    private int batchSize = 60;
    private int readTimeout = 30 * 1000;
    private int connTimeout = 30 * 1000;
    private boolean deleteDirWhenDropSchema = true;
    private Optional<Boolean> dbCacheEnable = Optional.empty();
    private Optional<Integer> dbCacheSize = Optional.empty();
    private Optional<Integer> dbCacheTTLMins = Optional.empty();
    private Optional<Boolean> tbCacheEnable = Optional.empty();
    private Optional<Integer> tbCacheSize = Optional.empty();
    private Optional<Integer> tbCacheTTLMins = Optional.empty();
    private Optional<Boolean> assumeCanonicalPartitionKeys = Optional.empty();

    private Boolean enableFileOperation = false;
    private Double enableFileOperationGrayRate = 0.0d;
    private Boolean enableRecordActionLog = true;
    private Boolean enableRecordLog = true;
    private Integer tableColStatsBatchSize = 50;

    public String getRegionId()
    {
        return regionId;
    }

    @Config("alibaba-dlf.catalog.region")
    @ConfigDescription("Region Id for Data Lake Formation Catalog")
    public AlibabaDlfMetaStoreConfig setRegionId(String regionId)
    {
        this.regionId = regionId;
        return this;
    }

    public Optional<String> getEndpoint()
    {
        return endpoint;
    }

    @Config("alibaba-dlf.catalog.endpoint")
    @ConfigDescription("Region for Data Lake Formation Catalog")
    public AlibabaDlfMetaStoreConfig setEndpoint(String endpoint)
    {
        this.endpoint = Optional.ofNullable(endpoint);
        return this;
    }

    public Boolean getEnableRecordActionLog()
    {
        return enableRecordActionLog;
    }

    @Config("alibaba-dlf.catalog.action.log.enabled")
    @ConfigDescription("enable record action log")
    public AlibabaDlfMetaStoreConfig setEnableRecordActionLog(Boolean enableRecordActionLog)
    {
        this.enableRecordActionLog = enableRecordActionLog;
        return this;
    }

    public Boolean getEnableRecordLog()
    {
        return enableRecordLog;
    }

    @Config("alibaba-dlf.catalog.log.enabled")
    @ConfigDescription("enable record log")
    public AlibabaDlfMetaStoreConfig setEnableRecordLog(Boolean enableRecordLog)
    {
        this.enableRecordLog = enableRecordLog;
        return this;
    }

    public Optional<String> getLogStore()
    {
        return logStore;
    }

    @Config("alibaba-dlf.catalog.proxyLogStore")
    @ConfigDescription("Access Key Id for Data Lake Formation Catalog")
    public AlibabaDlfMetaStoreConfig setLogStore(String logStore)
    {
        if (logStore == null || logStore.isEmpty()) {
            this.logStore = getUserId();
        }
        else {
            this.logStore = Optional.of(logStore);
        }
        return this;
    }

    public Optional<String> getUserId()
    {
        return userId;
    }

    @Config("alibaba-dlf.catalog.uid")
    @ConfigDescription("Access Key Id for Data Lake Formation Catalog")
    public AlibabaDlfMetaStoreConfig setUserId(String userId)
    {
        this.userId = Optional.ofNullable(userId);
        return this;
    }

    public Optional<String> getAccessKeyId()
    {
        return accessKeyId;
    }

    @Config("alibaba-dlf.catalog.accessKeyId")
    @ConfigDescription("Access Key Id for Data Lake Formation Catalog")
    public AlibabaDlfMetaStoreConfig setAccessKeyId(String accessKeyId)
    {
        this.accessKeyId = Optional.ofNullable(accessKeyId);
        return this;
    }

    public Optional<String> getAccessKeySecret()
    {
        return accessKeySecret;
    }

    @Config("alibaba-dlf.catalog.accessKeySecret")
    @ConfigDescription("Access Key Secret for Data Lake Formation Catalog")
    public AlibabaDlfMetaStoreConfig setAccessKeySecret(String accessKeySecret)
    {
        this.accessKeySecret = Optional.ofNullable(accessKeySecret);
        return this;
    }

    public short getPageSize()
    {
        return pageSize;
    }

    @Config("alibaba-dlf.catalog.pageSize")
    @ConfigDescription("Page size when fetch result from catalog")
    public AlibabaDlfMetaStoreConfig setPageSize(short pageSize)
    {
        this.pageSize = pageSize;
        return this;
    }

    public int getBatchSize()
    {
        return batchSize;
    }

    @Config("alibaba-dlf.catalog.accurate.batchSize")
    @ConfigDescription("Batch size when do partition operation to catalog")
    public AlibabaDlfMetaStoreConfig setBatchSize(Integer batchSize)
    {
        if (batchSize != null && batchSize > 0) {
            this.batchSize = batchSize;
        }

        return this;
    }

    public DataLakeConfig.AKMode getAkMode()
    {
        return this.akMode;
    }

    @Config("alibaba-dlf.catalog.akMode")
    @ConfigDescription("The mode of AK settings.")
    public AlibabaDlfMetaStoreConfig setAkMode(String akMode)
    {
        this.akMode = DataLakeConfig.AKMode.valueOf(akMode);
        return this;
    }

    public boolean isNewStsMode()
    {
        return isNewStsMode;
    }

    @Config("alibaba-dlf.catalog.sts.isNewMode")
    @ConfigDescription("The mode of sts settings.")
    public AlibabaDlfMetaStoreConfig setNewStsMode(Boolean newStsMode)
    {
        if (newStsMode != null) {
            this.isNewStsMode = newStsMode;
        }

        return this;
    }

    public String getDefaultWarehouseDir()
    {
        return defaultWarehouseDir;
    }

    @Config("alibaba-dlf.catalog.default-warehouse-dir")
    @ConfigDescription("Dlf Catalog default warehouse directory")
    public AlibabaDlfMetaStoreConfig setDefaultWarehouseDir(String defaultWarehouseDir)
    {
        this.defaultWarehouseDir = defaultWarehouseDir;

        return this;
    }

    public int getReadTimeout()
    {
        return this.readTimeout;
    }

    public AlibabaDlfMetaStoreConfig setReadTimeout(int readTimeout)
    {
        if (readTimeout > DataLakeConfig.MAX_CATALOG_SERVER_READ_TIMEOUT_MILLS || readTimeout < 0) {
            this.readTimeout = DataLakeConfig.DEFAULT_CATALOG_SERVER_READ_TIMEOUT_MILLS;
        }
        else {
            this.readTimeout = readTimeout;
        }
        return this;
    }

    public int getConnTimeout()
    {
        return this.connTimeout;
    }

    public AlibabaDlfMetaStoreConfig setConnTimeout(int connTimeout)
    {
        if (connTimeout > DataLakeConfig.MAX_CATALOG_SERVER_CONN_TIMEOUT_MILLS || connTimeout < 0) {
            this.connTimeout = DataLakeConfig.DEFAULT_CATALOG_SERVER_CONN_TIMEOUT_MILLS;
        }
        else {
            this.connTimeout = connTimeout;
        }
        return this;
    }

    public boolean getDeleteDirWhenDropSchema()
    {
        return deleteDirWhenDropSchema;
    }

    @Config("alibaba-dlf.catalog.delete-dir-when-drop-schema")
    public AlibabaDlfMetaStoreConfig setDeleteDirWhenDropSchema(boolean deleteDirWhenDropSchema)
    {
        this.deleteDirWhenDropSchema = deleteDirWhenDropSchema;
        return this;
    }

    public Optional<Boolean> getDbCacheEnable()
    {
        return dbCacheEnable;
    }

    @Config("data.lake.cache.db.enable")
    @ConfigDescription("datalake database's cache is enabled")
    public AlibabaDlfMetaStoreConfig setDbCacheEnable(Boolean dbCacheEnable)
    {
        this.dbCacheEnable = Optional.ofNullable(dbCacheEnable);
        return this;
    }

    public Optional<Integer> getDbCacheSize()
    {
        return dbCacheSize;
    }

    @Config("data.lake.cache.db.size")
    @ConfigDescription("datalake database's cache size,unit byte")
    public AlibabaDlfMetaStoreConfig setDbCacheSize(Integer dbCacheSize)
    {
        this.dbCacheSize = Optional.ofNullable(dbCacheSize);
        return this;
    }

    public Optional<Integer> getDbCacheTTLMins()
    {
        return dbCacheTTLMins;
    }

    @Config("data.lake.cache.db.ttl.mins")
    @ConfigDescription("datalake database's cache ttl mins,unit min ")
    public AlibabaDlfMetaStoreConfig setDbCacheTTLMins(Integer dbCacheTTLMins)
    {
        this.dbCacheTTLMins = Optional.ofNullable(dbCacheTTLMins);
        return this;
    }

    public Optional<Boolean> getTbCacheEnable()
    {
        return tbCacheEnable;
    }

    @Config("data.lake.cache.tb.enable")
    @ConfigDescription("datalake table's cache is enabled")
    public AlibabaDlfMetaStoreConfig setTbCacheEnable(Boolean tbCacheEnable)
    {
        this.tbCacheEnable = Optional.ofNullable(tbCacheEnable);
        return this;
    }

    public Optional<Integer> getTbCacheSize()
    {
        return tbCacheSize;
    }

    @Config("data.lake.cache.tb.size")
    @ConfigDescription("datalake table's cache size ,unit record ")
    public AlibabaDlfMetaStoreConfig setTbCacheSize(Integer tbCacheSize)
    {
        this.tbCacheSize = Optional.ofNullable(tbCacheSize);
        return this;
    }

    public Optional<Integer> getTbCacheTTLMins()
    {
        return tbCacheTTLMins;
    }

    @Config("data.lake.cache.tb.ttl.mins")
    @ConfigDescription("datalake table's cache ttl mins,unit min ")
    public AlibabaDlfMetaStoreConfig setTbCacheTTLMins(Integer tbCacheTTLMins)
    {
        this.tbCacheTTLMins = Optional.ofNullable(tbCacheTTLMins);
        return this;
    }

    public Optional<String> getHiveMetastoreAuthenticationType()
    {
        return hiveMetastoreAuthenticationType;
    }

    @Config("hive.metastore.authentication.type")
    @ConfigDescription("hive matastore authentication type ")
    public AlibabaDlfMetaStoreConfig setHiveMetastoreAuthenticationType(String hiveMetastoreAuthenticationType)
    {
        this.hiveMetastoreAuthenticationType = Optional.ofNullable(hiveMetastoreAuthenticationType);
        return this;
    }

    public Optional<String> getHiveMetastoreServiceClientPrincipal()
    {
        return hiveMetastoreServiceClientPrincipal;
    }

    @Config("hive.metastore.client.principal")
    @ConfigDescription("hive metastore servcie client principal")
    public AlibabaDlfMetaStoreConfig setHiveMetastoreServiceClientPrincipal(String hiveMetastoreServiceClientPrincipal)
    {
        this.hiveMetastoreServiceClientPrincipal = Optional.ofNullable(hiveMetastoreServiceClientPrincipal);
        return this;
    }

    public Optional<String> getHiveMetastoreServicePrincipal()
    {
        return hiveMetastoreServicePrincipal;
    }

    @Config("hive.metastore.service.principal")
    @ConfigDescription("hive metastore service principal ")
    public AlibabaDlfMetaStoreConfig setHiveMetastoreServicePrincipal(String hiveMetastoreServicePrincipal)
    {
        this.hiveMetastoreServicePrincipal = Optional.ofNullable(hiveMetastoreServicePrincipal);
        return this;
    }

    public Optional<String> getHiveMetastoreClientKeytab()
    {
        return hiveMetastoreClientKeytab;
    }

    @Config("hive.metastore.client.keytab")
    @ConfigDescription("hive metastore client keytab ")
    public AlibabaDlfMetaStoreConfig setHiveMetastoreClientKeytab(String hiveMetastoreClientKeytab)
    {
        this.hiveMetastoreClientKeytab = Optional.ofNullable(hiveMetastoreClientKeytab);
        return this;
    }

    public ProxyMode getProxyMode()
    {
        return this.proxyMode;
    }

    @Config("alibaba-dlf.catalog.proxyMode")
    @ConfigDescription("The mode of proxy settings.")
    public AlibabaDlfMetaStoreConfig setProxyMode(String proxyMode)
    {
        this.proxyMode = ProxyMode.valueOf(proxyMode);
        return this;
    }

    public Boolean getEnableBitVector()
    {
        return enableBitVector;
    }

    @Config("alibaba-dlf.catalog.enable.bit.vector")
    @ConfigDescription("Whether to enable bit vector in statistics.")
    public AlibabaDlfMetaStoreConfig setEnableBitVector(Boolean enableBitVector)
    {
        this.enableBitVector = enableBitVector;
        return this;
    }

    public Integer getBatchSizeForGetPartititon()
    {
        return batchSizeForGetPartititon;
    }

    @Config("alibaba-dlf.catalog.client.col.stats.pageSize")
    @ConfigDescription("The page size for getting parttition statistics.")
    public AlibabaDlfMetaStoreConfig setBatchSizeForGetPartititon(Integer batchSizeForGetPartititon)
    {
        this.batchSizeForGetPartititon = batchSizeForGetPartititon;
        return this;
    }

    public Boolean getEnableFileOperation()
    {
        return enableFileOperation;
    }

    @Config("alibaba-dlf.catalog.enable.file.operation")
    @ConfigDescription("enable file operation in double write mode")
    public AlibabaDlfMetaStoreConfig setEnableFileOperation(Boolean enableFileOperation)
    {
        this.enableFileOperation = enableFileOperation;
        return this;
    }

    public Double getEnableFileOperationGrayRate()
    {
        return enableFileOperationGrayRate;
    }

    @Config("alibaba-dlf.catalog.enable.file.operation.gray.rate")
    @ConfigDescription("enable dlf write operation rate in double write mode")
    public AlibabaDlfMetaStoreConfig setEnableFileOperationGrayRate(Double enableFileOperationGrayRate)
    {
        this.enableFileOperationGrayRate = enableFileOperationGrayRate;
        return this;
    }

    public Optional<Boolean> getAssumeCanonicalPartitionKeys()
    {
        return assumeCanonicalPartitionKeys;
    }

    @Config("alibaba-dlf.catalog.assume-canonical-partition-keys")
    public AlibabaDlfMetaStoreConfig setAssumeCanonicalPartitionKeys(Boolean assumeCanonicalPartitionKeys)
    {
        this.assumeCanonicalPartitionKeys = Optional.ofNullable(assumeCanonicalPartitionKeys);
        return this;
    }

    public Integer getTableColStatsBatchSize()
    {
        return tableColStatsBatchSize;
    }

    @Config("alibaba-dlf.catalog.client.table.col.stats.pageSize")
    @ConfigDescription("the batch size to get table column statics")
    public AlibabaDlfMetaStoreConfig setTableColStatsBatchSize(Integer tableColStatsBatchSize)
    {
        this.tableColStatsBatchSize = tableColStatsBatchSize;
        return this;
    }
}
