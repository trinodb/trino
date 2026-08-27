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
package io.trino.plugin.paimon;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.apache.paimon.options.Options;

import java.util.HashMap;
import java.util.Map;

import static io.trino.plugin.paimon.catalog.PaimonCatalog.DEFAULT_SESSION_CATALOG_CACHE_MAXIMUM_SIZE;

/**
 * Configuration class for Paimon connector to declare configuration properties
 * so that Airlift Bootstrap framework knows they are being consumed.
 */
public class PaimonConfig
{
    private String warehouse;
    private String metastore;
    private String uri;
    private String jdbcUser;
    private String jdbcPassword;
    private String tableType;
    private Boolean lockEnabled;
    private String lockType;
    private String lockCheckMaxSleep;
    private String lockAcquireTimeout;
    private Integer clientPoolSize;
    private Boolean cacheEnabled;
    private String cacheExpireAfterAccess;
    private String cacheExpirationInterval;
    private String cacheExpireAfterWrite;
    private Long cachePartitionMaxNum;
    private String cacheManifestSmallFileMemory;
    private String cacheManifestSmallFileThreshold;
    private String cacheManifestMaxMemory;
    private Boolean cacheManifestSoftValues;
    private Integer cacheSnapshotMaxNumPerTable;
    private Integer cacheDeletionVectorsMaxNum;
    private Boolean caseSensitive;
    private Boolean allowUpperCase;
    private Boolean syncAllProperties;
    private Boolean formatTableEnabled;
    private Boolean resolvingFileIoEnabled;
    private Boolean fileIoAllowCache;
    private Boolean localCacheEnabled;
    private String localCacheDir;
    private String localCacheMaxSize;
    private String localCacheBlockSize;
    private String localCacheWhitelist;
    private String catalogKey;
    private Integer lockKeyMaxLength;
    private String hiveConfDir;
    private String hadoopConfDir;
    private String metastoreClientClass;
    private Boolean locationInProperties;
    private Long clientPoolCacheEvictionIntervalMs;
    private Boolean hiveSkipUpdateStats;
    private String clientPoolCacheKeys;
    private Boolean alterTableCascade;
    private String s3Endpoint;
    private String s3AccessKey;
    private String s3AccessKeyFallback;
    private String s3AwsAccessKey;
    private String s3SecretKey;
    private String s3SecretKeyFallback;
    private String s3AwsSecretKey;
    private Boolean s3PathStyleAccess;
    private Boolean s3PathStyleAccessFallback;
    private String s3Region;
    private String s3EndpointRegion;
    private String s3SignerType;
    private String s3SigningAlgorithm;
    private String s3AEndpoint;
    private String s3AAccessKey;
    private String s3AAccessKeyFallback;
    private String s3ASecretKey;
    private String s3ASecretKeyFallback;
    private Boolean s3APathStyleAccess;
    private Boolean s3APathStyleAccessFallback;
    private String s3ARegion;
    private String s3AEndpointRegion;
    private String s3ASignerType;
    private String s3ASigningAlgorithm;
    private String fsS3AEndpoint;
    private String fsS3AAccessKey;
    private String fsS3AAccessKeyFallback;
    private String fsS3ASecretKey;
    private String fsS3ASecretKeyFallback;
    private Boolean fsS3APathStyleAccess;
    private Boolean fsS3APathStyleAccessFallback;
    private String fsS3ARegion;
    private String fsS3AEndpointRegion;
    private String fsS3ASignerType;
    private String fsS3ASigningAlgorithm;
    private Boolean fsNativeS3Enabled;
    private Boolean fsHadoopEnabled;
    private int catalogSessionCacheMaximumSize = DEFAULT_SESSION_CATALOG_CACHE_MAXIMUM_SIZE;
    private String writeSpillPath = System.getProperty("java.io.tmpdir");

    @NotNull
    public String getWarehouse()
    {
        return warehouse;
    }

    @Config("warehouse")
    public PaimonConfig setWarehouse(String warehouse)
    {
        this.warehouse = warehouse;
        return this;
    }

    public String getMetastore()
    {
        return metastore;
    }

    @Config("metastore")
    public PaimonConfig setMetastore(String metastore)
    {
        this.metastore = metastore;
        return this;
    }

    public String getUri()
    {
        return uri;
    }

    @Config("uri")
    public PaimonConfig setUri(String uri)
    {
        this.uri = uri;
        return this;
    }

    public String getJdbcUser()
    {
        return jdbcUser;
    }

    @Config("jdbc.user")
    public PaimonConfig setJdbcUser(String jdbcUser)
    {
        this.jdbcUser = jdbcUser;
        return this;
    }

    public String getJdbcPassword()
    {
        return jdbcPassword;
    }

    @Config("jdbc.password")
    @ConfigSecuritySensitive
    public PaimonConfig setJdbcPassword(String jdbcPassword)
    {
        this.jdbcPassword = jdbcPassword;
        return this;
    }

    public String getTableType()
    {
        return tableType;
    }

    @Config("table.type")
    public PaimonConfig setTableType(String tableType)
    {
        this.tableType = tableType;
        return this;
    }

    public Boolean getLockEnabled()
    {
        return lockEnabled;
    }

    @Config("lock.enabled")
    public PaimonConfig setLockEnabled(Boolean lockEnabled)
    {
        this.lockEnabled = lockEnabled;
        return this;
    }

    public String getLockType()
    {
        return lockType;
    }

    @Config("lock.type")
    public PaimonConfig setLockType(String lockType)
    {
        this.lockType = lockType;
        return this;
    }

    public String getLockCheckMaxSleep()
    {
        return lockCheckMaxSleep;
    }

    @Config("lock-check-max-sleep")
    public PaimonConfig setLockCheckMaxSleep(String lockCheckMaxSleep)
    {
        this.lockCheckMaxSleep = lockCheckMaxSleep;
        return this;
    }

    public String getLockAcquireTimeout()
    {
        return lockAcquireTimeout;
    }

    @Config("lock-acquire-timeout")
    public PaimonConfig setLockAcquireTimeout(String lockAcquireTimeout)
    {
        this.lockAcquireTimeout = lockAcquireTimeout;
        return this;
    }

    public Integer getClientPoolSize()
    {
        return clientPoolSize;
    }

    @Config("client-pool-size")
    public PaimonConfig setClientPoolSize(Integer clientPoolSize)
    {
        this.clientPoolSize = clientPoolSize;
        return this;
    }

    public Boolean getCacheEnabled()
    {
        return cacheEnabled;
    }

    @Config("cache-enabled")
    public PaimonConfig setCacheEnabled(Boolean cacheEnabled)
    {
        this.cacheEnabled = cacheEnabled;
        return this;
    }

    public String getCacheExpireAfterAccess()
    {
        return cacheExpireAfterAccess;
    }

    @Config("cache.expire-after-access")
    public PaimonConfig setCacheExpireAfterAccess(String cacheExpireAfterAccess)
    {
        this.cacheExpireAfterAccess = cacheExpireAfterAccess;
        return this;
    }

    public String getCacheExpirationInterval()
    {
        return cacheExpirationInterval;
    }

    @Config("cache.expiration-interval")
    public PaimonConfig setCacheExpirationInterval(String cacheExpirationInterval)
    {
        this.cacheExpirationInterval = cacheExpirationInterval;
        return this;
    }

    public String getCacheExpireAfterWrite()
    {
        return cacheExpireAfterWrite;
    }

    @Config("cache.expire-after-write")
    public PaimonConfig setCacheExpireAfterWrite(String cacheExpireAfterWrite)
    {
        this.cacheExpireAfterWrite = cacheExpireAfterWrite;
        return this;
    }

    public Long getCachePartitionMaxNum()
    {
        return cachePartitionMaxNum;
    }

    @Config("cache.partition.max-num")
    public PaimonConfig setCachePartitionMaxNum(Long cachePartitionMaxNum)
    {
        this.cachePartitionMaxNum = cachePartitionMaxNum;
        return this;
    }

    public String getCacheManifestSmallFileMemory()
    {
        return cacheManifestSmallFileMemory;
    }

    @Config("cache.manifest.small-file-memory")
    public PaimonConfig setCacheManifestSmallFileMemory(String cacheManifestSmallFileMemory)
    {
        this.cacheManifestSmallFileMemory = cacheManifestSmallFileMemory;
        return this;
    }

    public String getCacheManifestSmallFileThreshold()
    {
        return cacheManifestSmallFileThreshold;
    }

    @Config("cache.manifest.small-file-threshold")
    public PaimonConfig setCacheManifestSmallFileThreshold(String cacheManifestSmallFileThreshold)
    {
        this.cacheManifestSmallFileThreshold = cacheManifestSmallFileThreshold;
        return this;
    }

    public String getCacheManifestMaxMemory()
    {
        return cacheManifestMaxMemory;
    }

    @Config("cache.manifest.max-memory")
    public PaimonConfig setCacheManifestMaxMemory(String cacheManifestMaxMemory)
    {
        this.cacheManifestMaxMemory = cacheManifestMaxMemory;
        return this;
    }

    public Boolean getCacheManifestSoftValues()
    {
        return cacheManifestSoftValues;
    }

    @Config("cache.manifest.soft-values")
    public PaimonConfig setCacheManifestSoftValues(Boolean cacheManifestSoftValues)
    {
        this.cacheManifestSoftValues = cacheManifestSoftValues;
        return this;
    }

    public Integer getCacheSnapshotMaxNumPerTable()
    {
        return cacheSnapshotMaxNumPerTable;
    }

    @Config("cache.snapshot.max-num-per-table")
    public PaimonConfig setCacheSnapshotMaxNumPerTable(Integer cacheSnapshotMaxNumPerTable)
    {
        this.cacheSnapshotMaxNumPerTable = cacheSnapshotMaxNumPerTable;
        return this;
    }

    public Integer getCacheDeletionVectorsMaxNum()
    {
        return cacheDeletionVectorsMaxNum;
    }

    @Config("cache.deletion-vectors.max-num")
    public PaimonConfig setCacheDeletionVectorsMaxNum(Integer cacheDeletionVectorsMaxNum)
    {
        this.cacheDeletionVectorsMaxNum = cacheDeletionVectorsMaxNum;
        return this;
    }

    public Boolean getCaseSensitive()
    {
        return caseSensitive;
    }

    @Config("case-sensitive")
    public PaimonConfig setCaseSensitive(Boolean caseSensitive)
    {
        this.caseSensitive = caseSensitive;
        return this;
    }

    public Boolean getAllowUpperCase()
    {
        return allowUpperCase;
    }

    @Config("allow-upper-case")
    public PaimonConfig setAllowUpperCase(Boolean allowUpperCase)
    {
        this.allowUpperCase = allowUpperCase;
        return this;
    }

    public Boolean getSyncAllProperties()
    {
        return syncAllProperties;
    }

    @Config("sync-all-properties")
    public PaimonConfig setSyncAllProperties(Boolean syncAllProperties)
    {
        this.syncAllProperties = syncAllProperties;
        return this;
    }

    public Boolean getFormatTableEnabled()
    {
        return formatTableEnabled;
    }

    @Config("format-table.enabled")
    public PaimonConfig setFormatTableEnabled(Boolean formatTableEnabled)
    {
        this.formatTableEnabled = formatTableEnabled;
        return this;
    }

    public Boolean getResolvingFileIoEnabled()
    {
        return resolvingFileIoEnabled;
    }

    @Config("resolving-file-io.enabled")
    public PaimonConfig setResolvingFileIoEnabled(Boolean resolvingFileIoEnabled)
    {
        this.resolvingFileIoEnabled = resolvingFileIoEnabled;
        return this;
    }

    public Boolean getFileIoAllowCache()
    {
        return fileIoAllowCache;
    }

    @Config("file-io.allow-cache")
    public PaimonConfig setFileIoAllowCache(Boolean fileIoAllowCache)
    {
        this.fileIoAllowCache = fileIoAllowCache;
        return this;
    }

    public Boolean getLocalCacheEnabled()
    {
        return localCacheEnabled;
    }

    @Config("local-cache.enabled")
    public PaimonConfig setLocalCacheEnabled(Boolean localCacheEnabled)
    {
        this.localCacheEnabled = localCacheEnabled;
        return this;
    }

    public String getLocalCacheDir()
    {
        return localCacheDir;
    }

    @Config("local-cache.dir")
    public PaimonConfig setLocalCacheDir(String localCacheDir)
    {
        this.localCacheDir = localCacheDir;
        return this;
    }

    public String getLocalCacheMaxSize()
    {
        return localCacheMaxSize;
    }

    @Config("local-cache.max-size")
    public PaimonConfig setLocalCacheMaxSize(String localCacheMaxSize)
    {
        this.localCacheMaxSize = localCacheMaxSize;
        return this;
    }

    public String getLocalCacheBlockSize()
    {
        return localCacheBlockSize;
    }

    @Config("local-cache.block-size")
    public PaimonConfig setLocalCacheBlockSize(String localCacheBlockSize)
    {
        this.localCacheBlockSize = localCacheBlockSize;
        return this;
    }

    public String getLocalCacheWhitelist()
    {
        return localCacheWhitelist;
    }

    @Config("local-cache.whitelist")
    public PaimonConfig setLocalCacheWhitelist(String localCacheWhitelist)
    {
        this.localCacheWhitelist = localCacheWhitelist;
        return this;
    }

    public String getCatalogKey()
    {
        return catalogKey;
    }

    @Config("catalog-key")
    public PaimonConfig setCatalogKey(String catalogKey)
    {
        this.catalogKey = catalogKey;
        return this;
    }

    public Integer getLockKeyMaxLength()
    {
        return lockKeyMaxLength;
    }

    @Config("lock-key-max-length")
    public PaimonConfig setLockKeyMaxLength(Integer lockKeyMaxLength)
    {
        this.lockKeyMaxLength = lockKeyMaxLength;
        return this;
    }

    public String getHiveConfDir()
    {
        return hiveConfDir;
    }

    @Config("hive-conf-dir")
    public PaimonConfig setHiveConfDir(String hiveConfDir)
    {
        this.hiveConfDir = hiveConfDir;
        return this;
    }

    public String getHadoopConfDir()
    {
        return hadoopConfDir;
    }

    @Config("hadoop-conf-dir")
    public PaimonConfig setHadoopConfDir(String hadoopConfDir)
    {
        this.hadoopConfDir = hadoopConfDir;
        return this;
    }

    public String getMetastoreClientClass()
    {
        return metastoreClientClass;
    }

    @Config("metastore.client.class")
    public PaimonConfig setMetastoreClientClass(String metastoreClientClass)
    {
        this.metastoreClientClass = metastoreClientClass;
        return this;
    }

    public Boolean getLocationInProperties()
    {
        return locationInProperties;
    }

    @Config("location-in-properties")
    public PaimonConfig setLocationInProperties(Boolean locationInProperties)
    {
        this.locationInProperties = locationInProperties;
        return this;
    }

    public Long getClientPoolCacheEvictionIntervalMs()
    {
        return clientPoolCacheEvictionIntervalMs;
    }

    @Config("client-pool-cache.eviction-interval-ms")
    public PaimonConfig setClientPoolCacheEvictionIntervalMs(Long clientPoolCacheEvictionIntervalMs)
    {
        this.clientPoolCacheEvictionIntervalMs = clientPoolCacheEvictionIntervalMs;
        return this;
    }

    public Boolean getHiveSkipUpdateStats()
    {
        return hiveSkipUpdateStats;
    }

    @Config("hive.skip-update-stats")
    public PaimonConfig setHiveSkipUpdateStats(Boolean hiveSkipUpdateStats)
    {
        this.hiveSkipUpdateStats = hiveSkipUpdateStats;
        return this;
    }

    public String getClientPoolCacheKeys()
    {
        return clientPoolCacheKeys;
    }

    @Config("client-pool-cache.keys")
    public PaimonConfig setClientPoolCacheKeys(String clientPoolCacheKeys)
    {
        this.clientPoolCacheKeys = clientPoolCacheKeys;
        return this;
    }

    public Boolean getAlterTableCascade()
    {
        return alterTableCascade;
    }

    @Config("alter-table-cascade")
    public PaimonConfig setAlterTableCascade(Boolean alterTableCascade)
    {
        this.alterTableCascade = alterTableCascade;
        return this;
    }

    public String getS3Endpoint()
    {
        return s3Endpoint;
    }

    @Config("s3.endpoint")
    public PaimonConfig setS3Endpoint(String s3Endpoint)
    {
        this.s3Endpoint = s3Endpoint;
        return this;
    }

    public String getS3AccessKey()
    {
        return s3AccessKey;
    }

    @Config("s3.access-key")
    public PaimonConfig setS3AccessKey(String s3AccessKey)
    {
        this.s3AccessKey = s3AccessKey;
        return this;
    }

    public String getS3AccessKeyFallback()
    {
        return s3AccessKeyFallback;
    }

    @Config("s3.access.key")
    public PaimonConfig setS3AccessKeyFallback(String s3AccessKey)
    {
        this.s3AccessKeyFallback = s3AccessKey;
        return this;
    }

    public String getS3AwsAccessKey()
    {
        return s3AwsAccessKey;
    }

    @Config("s3.aws-access-key")
    public PaimonConfig setS3AwsAccessKey(String s3AwsAccessKey)
    {
        this.s3AwsAccessKey = s3AwsAccessKey;
        return this;
    }

    public String getS3SecretKey()
    {
        return s3SecretKey;
    }

    @Config("s3.secret-key")
    @ConfigSecuritySensitive
    public PaimonConfig setS3SecretKey(String s3SecretKey)
    {
        this.s3SecretKey = s3SecretKey;
        return this;
    }

    public String getS3SecretKeyFallback()
    {
        return s3SecretKeyFallback;
    }

    @Config("s3.secret.key")
    @ConfigSecuritySensitive
    public PaimonConfig setS3SecretKeyFallback(String s3SecretKey)
    {
        this.s3SecretKeyFallback = s3SecretKey;
        return this;
    }

    public String getS3AwsSecretKey()
    {
        return s3AwsSecretKey;
    }

    @Config("s3.aws-secret-key")
    @ConfigSecuritySensitive
    public PaimonConfig setS3AwsSecretKey(String s3AwsSecretKey)
    {
        this.s3AwsSecretKey = s3AwsSecretKey;
        return this;
    }

    public Boolean getS3PathStyleAccess()
    {
        return s3PathStyleAccess;
    }

    @Config("s3.path-style-access")
    public PaimonConfig setS3PathStyleAccess(Boolean s3PathStyleAccess)
    {
        this.s3PathStyleAccess = s3PathStyleAccess;
        return this;
    }

    public Boolean getS3PathStyleAccessFallback()
    {
        return s3PathStyleAccessFallback;
    }

    @Config("s3.path.style.access")
    public PaimonConfig setS3PathStyleAccessFallback(Boolean s3PathStyleAccess)
    {
        this.s3PathStyleAccessFallback = s3PathStyleAccess;
        return this;
    }

    public String getS3Region()
    {
        return s3Region;
    }

    @Config("s3.region")
    public PaimonConfig setS3Region(String s3Region)
    {
        this.s3Region = s3Region;
        return this;
    }

    public String getS3EndpointRegion()
    {
        return s3EndpointRegion;
    }

    @Config("s3.endpoint.region")
    public PaimonConfig setS3EndpointRegion(String s3EndpointRegion)
    {
        this.s3EndpointRegion = s3EndpointRegion;
        return this;
    }

    public String getS3SignerType()
    {
        return s3SignerType;
    }

    @Config("s3.signer-type")
    public PaimonConfig setS3SignerType(String s3SignerType)
    {
        this.s3SignerType = s3SignerType;
        return this;
    }

    public String getS3SigningAlgorithm()
    {
        return s3SigningAlgorithm;
    }

    @Config("s3.signing-algorithm")
    public PaimonConfig setS3SigningAlgorithm(String s3SigningAlgorithm)
    {
        this.s3SigningAlgorithm = s3SigningAlgorithm;
        return this;
    }

    public String getS3AEndpoint()
    {
        return s3AEndpoint;
    }

    @Config("s3a.endpoint")
    public PaimonConfig setS3AEndpoint(String s3Endpoint)
    {
        this.s3AEndpoint = s3Endpoint;
        return this;
    }

    public String getS3AAccessKey()
    {
        return s3AAccessKey;
    }

    @Config("s3a.access-key")
    public PaimonConfig setS3AAccessKey(String s3AccessKey)
    {
        this.s3AAccessKey = s3AccessKey;
        return this;
    }

    public String getS3AAccessKeyFallback()
    {
        return s3AAccessKeyFallback;
    }

    @Config("s3a.access.key")
    public PaimonConfig setS3AAccessKeyFallback(String s3AccessKey)
    {
        this.s3AAccessKeyFallback = s3AccessKey;
        return this;
    }

    public String getS3ASecretKey()
    {
        return s3ASecretKey;
    }

    @Config("s3a.secret-key")
    @ConfigSecuritySensitive
    public PaimonConfig setS3ASecretKey(String s3SecretKey)
    {
        this.s3ASecretKey = s3SecretKey;
        return this;
    }

    public String getS3ASecretKeyFallback()
    {
        return s3ASecretKeyFallback;
    }

    @Config("s3a.secret.key")
    @ConfigSecuritySensitive
    public PaimonConfig setS3ASecretKeyFallback(String s3SecretKey)
    {
        this.s3ASecretKeyFallback = s3SecretKey;
        return this;
    }

    public Boolean getS3APathStyleAccess()
    {
        return s3APathStyleAccess;
    }

    @Config("s3a.path-style-access")
    public PaimonConfig setS3APathStyleAccess(Boolean s3PathStyleAccess)
    {
        this.s3APathStyleAccess = s3PathStyleAccess;
        return this;
    }

    public Boolean getS3APathStyleAccessFallback()
    {
        return s3APathStyleAccessFallback;
    }

    @Config("s3a.path.style.access")
    public PaimonConfig setS3APathStyleAccessFallback(Boolean s3PathStyleAccess)
    {
        this.s3APathStyleAccessFallback = s3PathStyleAccess;
        return this;
    }

    public String getS3ARegion()
    {
        return s3ARegion;
    }

    @Config("s3a.region")
    public PaimonConfig setS3ARegion(String s3Region)
    {
        this.s3ARegion = s3Region;
        return this;
    }

    public String getS3AEndpointRegion()
    {
        return s3AEndpointRegion;
    }

    @Config("s3a.endpoint.region")
    public PaimonConfig setS3AEndpointRegion(String s3EndpointRegion)
    {
        this.s3AEndpointRegion = s3EndpointRegion;
        return this;
    }

    public String getS3ASignerType()
    {
        return s3ASignerType;
    }

    @Config("s3a.signer-type")
    public PaimonConfig setS3ASignerType(String s3SignerType)
    {
        this.s3ASignerType = s3SignerType;
        return this;
    }

    public String getS3ASigningAlgorithm()
    {
        return s3ASigningAlgorithm;
    }

    @Config("s3a.signing-algorithm")
    public PaimonConfig setS3ASigningAlgorithm(String s3SigningAlgorithm)
    {
        this.s3ASigningAlgorithm = s3SigningAlgorithm;
        return this;
    }

    public String getFsS3AEndpoint()
    {
        return fsS3AEndpoint;
    }

    @Config("fs.s3a.endpoint")
    public PaimonConfig setFsS3AEndpoint(String s3Endpoint)
    {
        this.fsS3AEndpoint = s3Endpoint;
        return this;
    }

    public String getFsS3AAccessKey()
    {
        return fsS3AAccessKey;
    }

    @Config("fs.s3a.access-key")
    public PaimonConfig setFsS3AAccessKey(String s3AccessKey)
    {
        this.fsS3AAccessKey = s3AccessKey;
        return this;
    }

    public String getFsS3AAccessKeyFallback()
    {
        return fsS3AAccessKeyFallback;
    }

    @Config("fs.s3a.access.key")
    public PaimonConfig setFsS3AAccessKeyFallback(String s3AccessKey)
    {
        this.fsS3AAccessKeyFallback = s3AccessKey;
        return this;
    }

    public String getFsS3ASecretKey()
    {
        return fsS3ASecretKey;
    }

    @Config("fs.s3a.secret-key")
    @ConfigSecuritySensitive
    public PaimonConfig setFsS3ASecretKey(String s3SecretKey)
    {
        this.fsS3ASecretKey = s3SecretKey;
        return this;
    }

    public String getFsS3ASecretKeyFallback()
    {
        return fsS3ASecretKeyFallback;
    }

    @Config("fs.s3a.secret.key")
    @ConfigSecuritySensitive
    public PaimonConfig setFsS3ASecretKeyFallback(String s3SecretKey)
    {
        this.fsS3ASecretKeyFallback = s3SecretKey;
        return this;
    }

    public Boolean getFsS3APathStyleAccess()
    {
        return fsS3APathStyleAccess;
    }

    @Config("fs.s3a.path-style-access")
    public PaimonConfig setFsS3APathStyleAccess(Boolean s3PathStyleAccess)
    {
        this.fsS3APathStyleAccess = s3PathStyleAccess;
        return this;
    }

    public Boolean getFsS3APathStyleAccessFallback()
    {
        return fsS3APathStyleAccessFallback;
    }

    @Config("fs.s3a.path.style.access")
    public PaimonConfig setFsS3APathStyleAccessFallback(Boolean s3PathStyleAccess)
    {
        this.fsS3APathStyleAccessFallback = s3PathStyleAccess;
        return this;
    }

    public String getFsS3ARegion()
    {
        return fsS3ARegion;
    }

    @Config("fs.s3a.region")
    public PaimonConfig setFsS3ARegion(String s3Region)
    {
        this.fsS3ARegion = s3Region;
        return this;
    }

    public String getFsS3AEndpointRegion()
    {
        return fsS3AEndpointRegion;
    }

    @Config("fs.s3a.endpoint.region")
    public PaimonConfig setFsS3AEndpointRegion(String s3EndpointRegion)
    {
        this.fsS3AEndpointRegion = s3EndpointRegion;
        return this;
    }

    public String getFsS3ASignerType()
    {
        return fsS3ASignerType;
    }

    @Config("fs.s3a.signer-type")
    public PaimonConfig setFsS3ASignerType(String s3SignerType)
    {
        this.fsS3ASignerType = s3SignerType;
        return this;
    }

    public String getFsS3ASigningAlgorithm()
    {
        return fsS3ASigningAlgorithm;
    }

    @Config("fs.s3a.signing-algorithm")
    public PaimonConfig setFsS3ASigningAlgorithm(String s3SigningAlgorithm)
    {
        this.fsS3ASigningAlgorithm = s3SigningAlgorithm;
        return this;
    }

    public Boolean getFsNativeS3Enabled()
    {
        return fsNativeS3Enabled;
    }

    @Config("fs.native-s3.enabled")
    public PaimonConfig setFsNativeS3Enabled(Boolean fsNativeS3Enabled)
    {
        this.fsNativeS3Enabled = fsNativeS3Enabled;
        return this;
    }

    public Boolean getFsHadoopEnabled()
    {
        return fsHadoopEnabled;
    }

    @Config("fs.hadoop.enabled")
    public PaimonConfig setFsHadoopEnabled(Boolean fsHadoopEnabled)
    {
        this.fsHadoopEnabled = fsHadoopEnabled;
        return this;
    }

    public int getCatalogSessionCacheMaximumSize()
    {
        return catalogSessionCacheMaximumSize;
    }

    @Config("catalog.session-cache.maximum-size")
    public PaimonConfig setCatalogSessionCacheMaximumSize(int catalogSessionCacheMaximumSize)
    {
        this.catalogSessionCacheMaximumSize = catalogSessionCacheMaximumSize;
        return this;
    }

    @NotBlank
    public String getWriteSpillPath()
    {
        return writeSpillPath;
    }

    @Config("write.spill-path")
    public PaimonConfig setWriteSpillPath(String writeSpillPath)
    {
        this.writeSpillPath = writeSpillPath;
        return this;
    }

    @AssertTrue(message = "must not contain empty path entries")
    public boolean isWriteSpillPathEntriesValid()
    {
        return PaimonWriteSpillPaths.hasValidEntries(writeSpillPath);
    }

    /**
     * Convert this configuration to Paimon Options. This method creates a Map of
     * all non-null configuration properties and returns it as a Paimon Options
     * object.
     */
    public Options toOptions()
    {
        Map<String, String> options = new HashMap<>();

        if (warehouse != null) {
            options.put("warehouse", warehouse);
        }
        putIfPresentTrimmed(options, "metastore", metastore);
        putIfPresent(options, "uri", uri);
        putIfPresent(options, "jdbc.user", jdbcUser);
        putIfPresent(options, "jdbc.password", jdbcPassword);
        putIfPresentTrimmed(options, "table.type", tableType);
        putIfPresent(options, "lock.enabled", lockEnabled);
        putIfPresentTrimmed(options, "lock.type", lockType);
        putIfPresent(options, "lock-check-max-sleep", lockCheckMaxSleep);
        putIfPresent(options, "lock-acquire-timeout", lockAcquireTimeout);
        putIfPresent(options, "client-pool-size", clientPoolSize);
        putIfPresent(options, "cache-enabled", cacheEnabled);
        putIfPresent(options, "cache.expire-after-access", cacheExpireAfterAccess);
        putIfPresent(options, "cache.expiration-interval", cacheExpirationInterval);
        putIfPresent(options, "cache.expire-after-write", cacheExpireAfterWrite);
        putIfPresent(options, "cache.partition.max-num", cachePartitionMaxNum);
        putIfPresent(options, "cache.manifest.small-file-memory", cacheManifestSmallFileMemory);
        putIfPresent(options, "cache.manifest.small-file-threshold", cacheManifestSmallFileThreshold);
        putIfPresent(options, "cache.manifest.max-memory", cacheManifestMaxMemory);
        putIfPresent(options, "cache.manifest.soft-values", cacheManifestSoftValues);
        putIfPresent(options, "cache.snapshot.max-num-per-table", cacheSnapshotMaxNumPerTable);
        putIfPresent(options, "cache.deletion-vectors.max-num", cacheDeletionVectorsMaxNum);
        putIfPresent(options, "case-sensitive", caseSensitive);
        putIfPresent(options, "allow-upper-case", allowUpperCase);
        putIfPresent(options, "sync-all-properties", syncAllProperties);
        putIfPresent(options, "format-table.enabled", formatTableEnabled);
        putIfPresent(options, "resolving-file-io.enabled", resolvingFileIoEnabled);
        putIfPresent(options, "file-io.allow-cache", fileIoAllowCache);
        putIfPresent(options, "local-cache.enabled", localCacheEnabled);
        putIfPresent(options, "local-cache.dir", localCacheDir);
        putIfPresent(options, "local-cache.max-size", localCacheMaxSize);
        putIfPresent(options, "local-cache.block-size", localCacheBlockSize);
        putIfPresent(options, "local-cache.whitelist", localCacheWhitelist);
        putIfPresent(options, "catalog-key", catalogKey);
        putIfPresent(options, "lock-key-max-length", lockKeyMaxLength);
        putIfPresent(options, "hive-conf-dir", hiveConfDir);
        putIfPresent(options, "hadoop-conf-dir", hadoopConfDir);
        putIfPresentTrimmed(options, "metastore.client.class", metastoreClientClass);
        putIfPresent(options, "location-in-properties", locationInProperties);
        putIfPresent(options, "client-pool-cache.eviction-interval-ms", clientPoolCacheEvictionIntervalMs);
        putIfPresent(options, "hive.skip-update-stats", hiveSkipUpdateStats);
        putIfPresent(options, "client-pool-cache.keys", clientPoolCacheKeys);
        putIfPresent(options, "alter-table-cascade", alterTableCascade);
        putIfPresent(options, "s3.endpoint", firstNonNull(s3Endpoint, s3AEndpoint, fsS3AEndpoint));
        putIfPresent(options, "s3.access-key", firstNonNull(
                s3AccessKey,
                s3AccessKeyFallback,
                s3AwsAccessKey,
                s3AAccessKey,
                s3AAccessKeyFallback,
                fsS3AAccessKey,
                fsS3AAccessKeyFallback));
        putIfPresent(options, "s3.secret-key", firstNonNull(
                s3SecretKey,
                s3SecretKeyFallback,
                s3AwsSecretKey,
                s3ASecretKey,
                s3ASecretKeyFallback,
                fsS3ASecretKey,
                fsS3ASecretKeyFallback));
        putIfPresent(options, "s3.path-style-access", firstNonNull(
                s3PathStyleAccess,
                s3PathStyleAccessFallback,
                s3APathStyleAccess,
                s3APathStyleAccessFallback,
                fsS3APathStyleAccess,
                fsS3APathStyleAccessFallback));
        putIfPresent(options, "s3.region", firstNonNull(
                s3Region,
                s3EndpointRegion,
                s3ARegion,
                s3AEndpointRegion,
                fsS3ARegion,
                fsS3AEndpointRegion));
        putIfPresent(options, "s3.signer-type", firstNonNull(
                s3SignerType,
                s3SigningAlgorithm,
                s3ASignerType,
                s3ASigningAlgorithm,
                fsS3ASignerType,
                fsS3ASigningAlgorithm));
        if (fsNativeS3Enabled != null) {
            options.put("fs.native-s3.enabled", fsNativeS3Enabled.toString());
        }
        if (fsHadoopEnabled != null) {
            options.put("fs.hadoop.enabled", fsHadoopEnabled.toString());
        }

        return new Options(options);
    }

    private static void putIfPresent(Map<String, String> options, String key, Object value)
    {
        if (value != null) {
            options.put(key, value.toString());
        }
    }

    private static void putIfPresentTrimmed(Map<String, String> options, String key, String value)
    {
        if (value != null) {
            options.put(key, value.strip());
        }
    }

    @SafeVarargs
    private static <T> T firstNonNull(T... values)
    {
        for (T value : values) {
            if (value != null) {
                return value;
            }
        }
        return null;
    }
}
