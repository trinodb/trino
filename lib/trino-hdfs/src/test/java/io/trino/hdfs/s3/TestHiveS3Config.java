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
package io.trino.hdfs.s3;

import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestHiveS3Config
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HiveS3Config.class)
                .setS3AwsAccessKey(null)
                .setS3AwsSecretKey(null)
                .setS3Endpoint(null)
                .setS3Region(null)
                .setS3SignerType(null)
                .setS3SignerClass(null)
                .setS3PathStyleAccess(false)
                .setS3IamRole(null)
                .setS3ExternalId(null)
                .setS3StorageClass(TrinoS3StorageClass.STANDARD)
                .setS3SslEnabled(true)
                .setS3SseEnabled(false)
                .setS3SseType(TrinoS3SseType.S3)
                .setS3SseKmsKeyId(null)
                .setS3KmsKeyId(null)
                .setS3EncryptionMaterialsProvider(null)
                .setS3MaxClientRetries(5)
                .setS3MaxErrorRetries(10)
                .setS3MaxBackoffTime(new Duration(10, TimeUnit.MINUTES))
                .setS3MaxRetryTime(new Duration(10, TimeUnit.MINUTES))
                .setS3ConnectTimeout(new Duration(5, TimeUnit.SECONDS))
                .setS3ConnectTtl(null)
                .setS3SocketTimeout(new Duration(5, TimeUnit.SECONDS))
                .setS3MultipartMinFileSize(DataSize.of(16, Unit.MEGABYTE))
                .setS3MultipartMinPartSize(DataSize.of(5, Unit.MEGABYTE))
                .setS3MaxConnections(500)
                .setS3StagingDirectory(new File(StandardSystemProperty.JAVA_IO_TMPDIR.value()))
                .setPinS3ClientToCurrentRegion(false)
                .setS3UserAgentPrefix("")
                .setS3AclType(TrinoS3AclType.PRIVATE)
                .setSkipGlacierObjects(false)
                .setRequesterPaysEnabled(false)
                .setS3StreamingUploadEnabled(true)
                .setS3StreamingPartSize(DataSize.of(32, Unit.MEGABYTE))
                .setS3ProxyHost(null)
                .setS3ProxyPort(-1)
                .setS3ProxyProtocol("HTTPS")
                .setS3NonProxyHosts(ImmutableList.of())
                .setS3ProxyUsername(null)
                .setS3ProxyPassword(null)
                .setS3PreemptiveBasicProxyAuth(false)
                .setS3StsEndpoint(null)
                .setS3StsRegion(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path stagingDirectory = Files.createTempDirectory(null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.s3.aws-access-key", "abc123")
                .put("hive.s3.aws-secret-key", "secret")
                .put("hive.s3.endpoint", "endpoint.example.com")
                .put("hive.s3.region", "eu-central-1")
                .put("hive.s3.signer-type", "S3SignerType")
                .put("hive.s3.signer-class", "com.amazonaws.services.s3.internal.AWSS3V4Signer")
                .put("hive.s3.path-style-access", "true")
                .put("hive.s3.iam-role", "roleArn")
                .put("hive.s3.external-id", "externalId")
                .put("hive.s3.storage-class", "INTELLIGENT_TIERING")
                .put("hive.s3.ssl.enabled", "false")
                .put("hive.s3.sse.enabled", "true")
                .put("hive.s3.sse.type", "KMS")
                .put("hive.s3.sse.kms-key-id", "KMS_KEY_ID")
                .put("hive.s3.encryption-materials-provider", "EMP_CLASS")
                .put("hive.s3.kms-key-id", "KEY_ID")
                .put("hive.s3.max-client-retries", "9")
                .put("hive.s3.max-error-retries", "8")
                .put("hive.s3.max-backoff-time", "4m")
                .put("hive.s3.max-retry-time", "20m")
                .put("hive.s3.connect-timeout", "8s")
                .put("hive.s3.connect-ttl", "30m")
                .put("hive.s3.socket-timeout", "4m")
                .put("hive.s3.multipart.min-file-size", "32MB")
                .put("hive.s3.multipart.min-part-size", "15MB")
                .put("hive.s3.max-connections", "77")
                .put("hive.s3.staging-directory", stagingDirectory.toString())
                .put("hive.s3.pin-client-to-current-region", "true")
                .put("hive.s3.user-agent-prefix", "user-agent-prefix")
                .put("hive.s3.upload-acl-type", "PUBLIC_READ")
                .put("hive.s3.skip-glacier-objects", "true")
                .put("hive.s3.requester-pays.enabled", "true")
                .put("hive.s3.streaming.enabled", "false")
                .put("hive.s3.streaming.part-size", "15MB")
                .put("hive.s3.proxy.host", "localhost")
                .put("hive.s3.proxy.port", "14000")
                .put("hive.s3.proxy.protocol", "HTTP")
                .put("hive.s3.proxy.non-proxy-hosts", "test,test2,test3")
                .put("hive.s3.proxy.username", "test")
                .put("hive.s3.proxy.password", "test")
                .put("hive.s3.proxy.preemptive-basic-auth", "true")
                .put("hive.s3.sts.endpoint", "http://minio:9000")
                .put("hive.s3.sts.region", "eu-central-1")
                .buildOrThrow();

        HiveS3Config expected = new HiveS3Config()
                .setS3AwsAccessKey("abc123")
                .setS3AwsSecretKey("secret")
                .setS3Endpoint("endpoint.example.com")
                .setS3Region("eu-central-1")
                .setS3SignerType(TrinoS3SignerType.S3SignerType)
                .setS3SignerClass("com.amazonaws.services.s3.internal.AWSS3V4Signer")
                .setS3PathStyleAccess(true)
                .setS3IamRole("roleArn")
                .setS3ExternalId("externalId")
                .setS3StorageClass(TrinoS3StorageClass.INTELLIGENT_TIERING)
                .setS3SslEnabled(false)
                .setS3SseEnabled(true)
                .setS3SseType(TrinoS3SseType.KMS)
                .setS3SseKmsKeyId("KMS_KEY_ID")
                .setS3EncryptionMaterialsProvider("EMP_CLASS")
                .setS3KmsKeyId("KEY_ID")
                .setS3MaxClientRetries(9)
                .setS3MaxErrorRetries(8)
                .setS3MaxBackoffTime(new Duration(4, TimeUnit.MINUTES))
                .setS3MaxRetryTime(new Duration(20, TimeUnit.MINUTES))
                .setS3ConnectTimeout(new Duration(8, TimeUnit.SECONDS))
                .setS3ConnectTtl(new Duration(30, TimeUnit.MINUTES))
                .setS3SocketTimeout(new Duration(4, TimeUnit.MINUTES))
                .setS3MultipartMinFileSize(DataSize.of(32, Unit.MEGABYTE))
                .setS3MultipartMinPartSize(DataSize.of(15, Unit.MEGABYTE))
                .setS3MaxConnections(77)
                .setS3StagingDirectory(stagingDirectory.toFile())
                .setPinS3ClientToCurrentRegion(true)
                .setS3UserAgentPrefix("user-agent-prefix")
                .setS3AclType(TrinoS3AclType.PUBLIC_READ)
                .setSkipGlacierObjects(true)
                .setRequesterPaysEnabled(true)
                .setS3StreamingUploadEnabled(false)
                .setS3StreamingPartSize(DataSize.of(15, Unit.MEGABYTE))
                .setS3ProxyHost("localhost")
                .setS3ProxyPort(14000)
                .setS3ProxyProtocol("HTTP")
                .setS3NonProxyHosts(ImmutableList.of("test", "test2", "test3"))
                .setS3ProxyUsername("test")
                .setS3ProxyPassword("test")
                .setS3PreemptiveBasicProxyAuth(true)
                .setS3StsEndpoint("http://minio:9000")
                .setS3StsRegion("eu-central-1");

        assertFullMapping(properties, expected);
    }
}
