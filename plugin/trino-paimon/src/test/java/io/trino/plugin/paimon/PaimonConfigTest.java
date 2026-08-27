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

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import org.apache.paimon.options.Options;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.trino.plugin.paimon.catalog.PaimonCatalog.DEFAULT_SESSION_CATALOG_CACHE_MAXIMUM_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PaimonConfigTest
{
    @Test
    public void testDefaultConfigToOptionsIsEmpty()
    {
        PaimonConfig config = new PaimonConfig();

        Options options = config.toOptions();

        assertThat(options.toMap()).isEmpty();
    }

    @Test
    public void testWarehouseIsMapped()
    {
        PaimonConfig config = new PaimonConfig().setWarehouse("/tmp/warehouse");

        Options options = config.toOptions();

        assertThat(options.toMap()).containsEntry("warehouse", "/tmp/warehouse");
    }

    @Test
    public void testCatalogOptionsAreMapped()
    {
        PaimonConfig config = new PaimonConfig()
                .setWarehouse("/tmp/warehouse")
                .setMetastore(" jdbc ")
                .setUri("jdbc:postgresql://localhost:5432/paimon")
                .setJdbcUser("paimon")
                .setJdbcPassword("secret")
                .setTableType(" EXTERNAL ")
                .setLockEnabled(true)
                .setLockType(" jdbc ")
                .setLockCheckMaxSleep("9 s")
                .setLockAcquireTimeout("10 min")
                .setClientPoolSize(8)
                .setCaseSensitive(true)
                .setSyncAllProperties(false)
                .setFormatTableEnabled(false)
                .setResolvingFileIoEnabled(true)
                .setFileIoAllowCache(false)
                .setCatalogKey("prod")
                .setLockKeyMaxLength(128);

        Options options = config.toOptions();

        assertThat(options.toMap())
                .containsEntry("warehouse", "/tmp/warehouse")
                .containsEntry("metastore", "jdbc")
                .containsEntry("uri", "jdbc:postgresql://localhost:5432/paimon")
                .containsEntry("jdbc.user", "paimon")
                .containsEntry("jdbc.password", "secret")
                .containsEntry("table.type", "EXTERNAL")
                .containsEntry("lock.enabled", "true")
                .containsEntry("lock.type", "jdbc")
                .containsEntry("lock-check-max-sleep", "9 s")
                .containsEntry("lock-acquire-timeout", "10 min")
                .containsEntry("client-pool-size", "8")
                .containsEntry("case-sensitive", "true")
                .containsEntry("sync-all-properties", "false")
                .containsEntry("format-table.enabled", "false")
                .containsEntry("resolving-file-io.enabled", "true")
                .containsEntry("file-io.allow-cache", "false")
                .containsEntry("catalog-key", "prod")
                .containsEntry("lock-key-max-length", "128");
    }

    @Test
    public void testCatalogCacheOptionsAreMapped()
    {
        PaimonConfig config = new PaimonConfig()
                .setCacheEnabled(false)
                .setCacheExpireAfterAccess("11 min")
                .setCacheExpirationInterval("10 min")
                .setCacheExpireAfterWrite("12 min")
                .setCachePartitionMaxNum(13L)
                .setCacheManifestSmallFileMemory("14 MB")
                .setCacheManifestSmallFileThreshold("15 MB")
                .setCacheManifestMaxMemory("16 MB")
                .setCacheManifestSoftValues(false)
                .setCacheSnapshotMaxNumPerTable(17)
                .setCacheDeletionVectorsMaxNum(18)
                .setLocalCacheEnabled(true)
                .setLocalCacheDir("/tmp/paimon-cache")
                .setLocalCacheMaxSize("19 GB")
                .setLocalCacheBlockSize("20 MB")
                .setLocalCacheWhitelist("meta,data");

        Options options = config.toOptions();

        assertThat(options.toMap())
                .containsEntry("cache-enabled", "false")
                .containsEntry("cache.expire-after-access", "11 min")
                .containsEntry("cache.expiration-interval", "10 min")
                .containsEntry("cache.expire-after-write", "12 min")
                .containsEntry("cache.partition.max-num", "13")
                .containsEntry("cache.manifest.small-file-memory", "14 MB")
                .containsEntry("cache.manifest.small-file-threshold", "15 MB")
                .containsEntry("cache.manifest.max-memory", "16 MB")
                .containsEntry("cache.manifest.soft-values", "false")
                .containsEntry("cache.snapshot.max-num-per-table", "17")
                .containsEntry("cache.deletion-vectors.max-num", "18")
                .containsEntry("local-cache.enabled", "true")
                .containsEntry("local-cache.dir", "/tmp/paimon-cache")
                .containsEntry("local-cache.max-size", "19 GB")
                .containsEntry("local-cache.block-size", "20 MB")
                .containsEntry("local-cache.whitelist", "meta,data");
    }

    @Test
    public void testCatalogFallbackOptionsAreMapped()
    {
        PaimonConfig config = new PaimonConfig()
                .setCacheExpirationInterval("10 min")
                .setAllowUpperCase(true);

        Options options = config.toOptions();

        assertThat(options.toMap())
                .containsEntry("cache.expiration-interval", "10 min")
                .containsEntry("allow-upper-case", "true");
    }

    @Test
    public void testHiveCatalogOptionsAreMapped()
    {
        PaimonConfig config = new PaimonConfig()
                .setHiveConfDir("/etc/hive/conf")
                .setHadoopConfDir("/etc/hadoop/conf")
                .setMetastoreClientClass(" com.example.CustomHiveMetaStoreClient ")
                .setLocationInProperties(true)
                .setClientPoolCacheEvictionIntervalMs(21L)
                .setHiveSkipUpdateStats(true)
                .setClientPoolCacheKeys("user_name,conf:hive.metastore.uris")
                .setAlterTableCascade(false);

        Options options = config.toOptions();

        assertThat(options.toMap())
                .containsEntry("hive-conf-dir", "/etc/hive/conf")
                .containsEntry("hadoop-conf-dir", "/etc/hadoop/conf")
                .containsEntry("metastore.client.class", "com.example.CustomHiveMetaStoreClient")
                .containsEntry("location-in-properties", "true")
                .containsEntry("client-pool-cache.eviction-interval-ms", "21")
                .containsEntry("hive.skip-update-stats", "true")
                .containsEntry("client-pool-cache.keys", "user_name,conf:hive.metastore.uris")
                .containsEntry("alter-table-cascade", "false");
    }

    @Test
    public void testS3CredentialsAreMapped()
    {
        PaimonConfig config = new PaimonConfig()
                .setS3AccessKey("access")
                .setS3SecretKey("secret");

        Options options = config.toOptions();

        assertThat(options.toMap())
                .containsEntry("s3.access-key", "access")
                .containsEntry("s3.secret-key", "secret");
    }

    @Test
    public void testS3EndpointAndRegionAreMapped()
    {
        PaimonConfig config = new PaimonConfig()
                .setS3Endpoint("http://localhost:9000")
                .setS3Region("us-east-1");

        Options options = config.toOptions();

        assertThat(options.toMap())
                .containsEntry("s3.endpoint", "http://localhost:9000")
                .containsEntry("s3.region", "us-east-1");
    }

    @Test
    public void testS3PathStyleAccessIsMapped()
    {
        PaimonConfig config = new PaimonConfig().setS3PathStyleAccess(true);

        Options options = config.toOptions();

        assertThat(options.toMap()).containsEntry("s3.path-style-access", "true");
    }

    @Test
    public void testS3FallbackAliasesAreMappedToCanonicalOptions()
    {
        PaimonConfig config = new PaimonConfig()
                .setS3AccessKeyFallback("access")
                .setS3SecretKeyFallback("secret")
                .setS3PathStyleAccessFallback(true);

        Options options = config.toOptions();

        assertThat(options.toMap())
                .containsEntry("s3.access-key", "access")
                .containsEntry("s3.secret-key", "secret")
                .containsEntry("s3.path-style-access", "true")
                .doesNotContainKeys("s3.access.key", "s3.secret.key", "s3.path.style.access");
    }

    @Test
    public void testS3APrefixAliasesAreMappedToCanonicalOptions()
    {
        PaimonConfig config = new PaimonConfig()
                .setS3AEndpoint("http://localhost:9000")
                .setS3AAccessKeyFallback("access")
                .setS3ASecretKeyFallback("secret")
                .setS3APathStyleAccessFallback(true)
                .setS3AEndpointRegion("us-east-1")
                .setS3ASigningAlgorithm("custom-signer");

        Options options = config.toOptions();

        assertThat(options.toMap())
                .containsEntry("s3.endpoint", "http://localhost:9000")
                .containsEntry("s3.access-key", "access")
                .containsEntry("s3.secret-key", "secret")
                .containsEntry("s3.path-style-access", "true")
                .containsEntry("s3.region", "us-east-1")
                .containsEntry("s3.signer-type", "custom-signer")
                .doesNotContainKeys(
                        "s3a.endpoint",
                        "s3a.access.key",
                        "s3a.secret.key",
                        "s3a.path.style.access",
                        "s3a.endpoint.region",
                        "s3a.signing-algorithm");
    }

    @Test
    public void testFsS3APrefixAliasesAreMappedToCanonicalOptions()
    {
        PaimonConfig config = new PaimonConfig()
                .setFsS3AEndpoint("http://localhost:9000")
                .setFsS3AAccessKeyFallback("access")
                .setFsS3ASecretKeyFallback("secret")
                .setFsS3APathStyleAccessFallback(true)
                .setFsS3AEndpointRegion("us-east-1")
                .setFsS3ASigningAlgorithm("custom-signer");

        Options options = config.toOptions();

        assertThat(options.toMap())
                .containsEntry("s3.endpoint", "http://localhost:9000")
                .containsEntry("s3.access-key", "access")
                .containsEntry("s3.secret-key", "secret")
                .containsEntry("s3.path-style-access", "true")
                .containsEntry("s3.region", "us-east-1")
                .containsEntry("s3.signer-type", "custom-signer")
                .doesNotContainKeys(
                        "fs.s3a.endpoint",
                        "fs.s3a.access.key",
                        "fs.s3a.secret.key",
                        "fs.s3a.path.style.access",
                        "fs.s3a.endpoint.region",
                        "fs.s3a.signing-algorithm");
    }

    @Test
    public void testTrinoS3CredentialAliasesAreMappedToCanonicalOptions()
    {
        PaimonConfig config = new PaimonConfig()
                .setS3AwsAccessKey("access")
                .setS3AwsSecretKey("secret");

        Options options = config.toOptions();

        assertThat(options.toMap())
                .containsEntry("s3.access-key", "access")
                .containsEntry("s3.secret-key", "secret")
                .doesNotContainKeys("s3.aws-access-key", "s3.aws-secret-key");
    }

    @Test
    public void testS3CanonicalOptionsTakePrecedenceOverAliases()
    {
        PaimonConfig config = new PaimonConfig()
                .setS3Endpoint("http://canonical")
                .setS3AEndpoint("http://s3a")
                .setFsS3AEndpoint("http://fs-s3a")
                .setS3AccessKey("canonical-access")
                .setS3AccessKeyFallback("fallback-access")
                .setS3AwsAccessKey("trino-access")
                .setS3AAccessKey("s3a-access")
                .setFsS3AAccessKey("fs-s3a-access")
                .setS3SecretKey("canonical-secret")
                .setS3SecretKeyFallback("fallback-secret")
                .setS3AwsSecretKey("trino-secret")
                .setS3ASecretKey("s3a-secret")
                .setFsS3ASecretKey("fs-s3a-secret")
                .setS3PathStyleAccess(false)
                .setS3PathStyleAccessFallback(true)
                .setS3APathStyleAccess(true)
                .setFsS3APathStyleAccess(true)
                .setS3Region("canonical-region")
                .setS3ARegion("s3a-region")
                .setFsS3ARegion("fs-s3a-region")
                .setS3SignerType("canonical-signer")
                .setS3ASignerType("s3a-signer")
                .setFsS3ASignerType("fs-s3a-signer");

        Options options = config.toOptions();

        assertThat(options.toMap())
                .containsEntry("s3.endpoint", "http://canonical")
                .containsEntry("s3.access-key", "canonical-access")
                .containsEntry("s3.secret-key", "canonical-secret")
                .containsEntry("s3.path-style-access", "false")
                .containsEntry("s3.region", "canonical-region")
                .containsEntry("s3.signer-type", "canonical-signer");
    }

    @Test
    public void testFileSystemFlagsAreMapped()
    {
        PaimonConfig config = new PaimonConfig()
                .setFsNativeS3Enabled(true)
                .setFsHadoopEnabled(false);

        Options options = config.toOptions();

        assertThat(options.toMap())
                .containsEntry("fs.native-s3.enabled", "true")
                .containsEntry("fs.hadoop.enabled", "false");
    }

    @Test
    public void testUnsetPropertiesAreNotIncluded()
    {
        PaimonConfig config = new PaimonConfig().setWarehouse("/tmp/warehouse");

        Options options = config.toOptions();

        assertThat(options.toMap())
                .containsEntry("warehouse", "/tmp/warehouse")
                .doesNotContainKeys("metastore", "uri", "s3.access-key", "s3.secret-key", "fs.native-s3.enabled");
    }

    @Test
    public void testGettersReturnSetValues()
    {
        PaimonConfig config = new PaimonConfig()
                .setWarehouse("/tmp/warehouse")
                .setS3AccessKey("access")
                .setS3SecretKey("secret")
                .setS3Endpoint("http://localhost:9000")
                .setS3Region("us-east-1")
                .setS3PathStyleAccess(true)
                .setFsNativeS3Enabled(true)
                .setFsHadoopEnabled(false);

        assertThat(config.getWarehouse()).isEqualTo("/tmp/warehouse");
        assertThat(config.getS3AccessKey()).isEqualTo("access");
        assertThat(config.getS3SecretKey()).isEqualTo("secret");
        assertThat(config.getS3Endpoint()).isEqualTo("http://localhost:9000");
        assertThat(config.getS3Region()).isEqualTo("us-east-1");
        assertThat(config.getS3PathStyleAccess()).isTrue();
        assertThat(config.getFsNativeS3Enabled()).isTrue();
        assertThat(config.getFsHadoopEnabled()).isFalse();
        assertThat(config.getCatalogSessionCacheMaximumSize()).isEqualTo(DEFAULT_SESSION_CATALOG_CACHE_MAXIMUM_SIZE);
        assertThat(config.getWriteSpillPath()).isEqualTo(System.getProperty("java.io.tmpdir"));
    }

    @Test
    public void testCatalogSessionCacheMaximumSizeIsConnectorOnly()
    {
        PaimonConfig config = new PaimonConfig()
                .setWarehouse("/tmp/warehouse")
                .setCatalogSessionCacheMaximumSize(10);

        Options options = config.toOptions();

        assertThat(config.getCatalogSessionCacheMaximumSize()).isEqualTo(10);
        assertThat(options.toMap())
                .containsEntry("warehouse", "/tmp/warehouse")
                .doesNotContainKey("catalog.session-cache.maximum-size");
    }

    @Test
    public void testWriteSpillPathIsConnectorOnly()
    {
        PaimonConfig config = new PaimonConfig()
                .setWarehouse("/tmp/warehouse")
                .setWriteSpillPath("/tmp/paimon-spill");

        Options options = config.toOptions();

        assertThat(config.getWriteSpillPath()).isEqualTo("/tmp/paimon-spill");
        assertThat(options.toMap())
                .containsEntry("warehouse", "/tmp/warehouse")
                .doesNotContainKey("write.spill-path");
    }

    @Test
    public void testWriteSpillPathMustNotBeEmpty()
    {
        assertFailsValidation(
                new PaimonConfig().setWriteSpillPath(""),
                "writeSpillPath",
                "must not be blank",
                NotBlank.class);
        assertFailsValidation(
                new PaimonConfig().setWriteSpillPath("   "),
                "writeSpillPath",
                "must not be blank",
                NotBlank.class);
        assertFailsValidation(
                new PaimonConfig().setWriteSpillPath("/tmp,,/var/tmp"),
                "writeSpillPathEntriesValid",
                "must not contain empty path entries",
                AssertTrue.class);
        assertFailsValidation(
                new PaimonConfig().setWriteSpillPath("/tmp,"),
                "writeSpillPathEntriesValid",
                "must not contain empty path entries",
                AssertTrue.class);
    }

    @Test
    public void testWriteSpillPathsAreTrimmedDeduplicatedAndCreated(@TempDir Path tempDir)
    {
        Path firstPath = tempDir.resolve("first");
        Path secondPath = tempDir.resolve("second");

        assertThat(PaimonWriteSpillPaths.split(" " + firstPath + ", " + secondPath + File.pathSeparator + firstPath + " "))
                .containsExactly(firstPath.toString(), secondPath.toString());
        assertThat(firstPath).isDirectory();
        assertThat(secondPath).isDirectory();
    }

    @Test
    public void testWriteSpillPathRejectsFile(@TempDir Path tempDir)
            throws IOException
    {
        Path file = Files.createFile(tempDir.resolve("spill-file"));

        assertThatThrownBy(() -> PaimonWriteSpillPaths.split(file.toString()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Failed to prepare Paimon write spill path");
    }

    @Test
    public void testAllPropertiesTogether()
    {
        PaimonConfig config = new PaimonConfig()
                .setWarehouse("/tmp/warehouse")
                .setS3AccessKey("access")
                .setS3SecretKey("secret")
                .setS3Endpoint("http://localhost:9000")
                .setS3Region("us-east-1")
                .setS3PathStyleAccess(true)
                .setFsNativeS3Enabled(true)
                .setFsHadoopEnabled(false);

        Options options = config.toOptions();

        assertThat(options.toMap()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "warehouse", "/tmp/warehouse",
                "s3.access-key", "access",
                "s3.secret-key", "secret",
                "s3.endpoint", "http://localhost:9000",
                "s3.region", "us-east-1",
                "s3.path-style-access", "true",
                "fs.native-s3.enabled", "true",
                "fs.hadoop.enabled", "false"));
    }
}
