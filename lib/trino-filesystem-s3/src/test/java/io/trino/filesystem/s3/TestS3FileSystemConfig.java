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
package io.trino.filesystem.s3;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.filesystem.s3.S3FileSystemConfig.ObjectCannedAcl;
import io.trino.filesystem.s3.S3FileSystemConfig.S3SseType;
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.filesystem.s3.S3FileSystemConfig.RetryMode.LEGACY;
import static io.trino.filesystem.s3.S3FileSystemConfig.RetryMode.STANDARD;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestS3FileSystemConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(S3FileSystemConfig.class)
                .setAwsAccessKey(null)
                .setAwsSecretKey(null)
                .setEndpoint(null)
                .setRegion(null)
                .setPathStyleAccess(false)
                .setIamRole(null)
                .setRoleSessionName("trino-filesystem")
                .setExternalId(null)
                .setStsEndpoint(null)
                .setStsRegion(null)
                .setCannedAcl(ObjectCannedAcl.NONE)
                .setSseType(S3SseType.NONE)
                .setRetryMode(LEGACY)
                .setMaxErrorRetries(10)
                .setSseKmsKeyId(null)
                .setUseWebIdentityTokenCredentialsProvider(false)
                .setSseCustomerKey(null)
                .setStreamingPartSize(DataSize.of(16, MEGABYTE))
                .setRequesterPays(false)
                .setMaxConnections(500)
                .setConnectionTtl(null)
                .setConnectionMaxIdleTime(null)
                .setSocketConnectTimeout(null)
                .setSocketReadTimeout(null)
                .setTcpKeepAlive(false)
                .setHttpProxy(null)
                .setHttpProxySecure(false)
                .setNonProxyHosts(null)
                .setHttpProxyUsername(null)
                .setHttpProxyPassword(null)
                .setHttpProxyPreemptiveBasicProxyAuth(false)
                .setSupportsExclusiveCreate(true)
                .setApplicationId("Trino"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("s3.aws-access-key", "abc123")
                .put("s3.aws-secret-key", "secret")
                .put("s3.endpoint", "endpoint.example.com")
                .put("s3.region", "eu-central-1")
                .put("s3.path-style-access", "true")
                .put("s3.iam-role", "myrole")
                .put("s3.role-session-name", "mysession")
                .put("s3.external-id", "myid")
                .put("s3.sts.endpoint", "sts.example.com")
                .put("s3.sts.region", "us-west-2")
                .put("s3.canned-acl", "BUCKET_OWNER_FULL_CONTROL")
                .put("s3.retry-mode", "STANDARD")
                .put("s3.max-error-retries", "12")
                .put("s3.sse.type", "KMS")
                .put("s3.sse.kms-key-id", "mykey")
                .put("s3.sse.customer-key", "customerKey")
                .put("s3.use-web-identity-token-credentials-provider", "true")
                .put("s3.streaming.part-size", "42MB")
                .put("s3.requester-pays", "true")
                .put("s3.max-connections", "42")
                .put("s3.connection-ttl", "1m")
                .put("s3.connection-max-idle-time", "2m")
                .put("s3.socket-connect-timeout", "3m")
                .put("s3.socket-read-timeout", "4m")
                .put("s3.tcp-keep-alive", "true")
                .put("s3.http-proxy", "localhost:8888")
                .put("s3.http-proxy.secure", "true")
                .put("s3.http-proxy.non-proxy-hosts", "test1,test2,test3")
                .put("s3.http-proxy.username", "test")
                .put("s3.http-proxy.password", "test")
                .put("s3.http-proxy.preemptive-basic-auth", "true")
                .put("s3.exclusive-create", "false")
                .put("s3.application-id", "application id")
                .buildOrThrow();

        S3FileSystemConfig expected = new S3FileSystemConfig()
                .setAwsAccessKey("abc123")
                .setAwsSecretKey("secret")
                .setEndpoint("endpoint.example.com")
                .setRegion("eu-central-1")
                .setPathStyleAccess(true)
                .setIamRole("myrole")
                .setRoleSessionName("mysession")
                .setExternalId("myid")
                .setStsEndpoint("sts.example.com")
                .setStsRegion("us-west-2")
                .setCannedAcl(ObjectCannedAcl.BUCKET_OWNER_FULL_CONTROL)
                .setStreamingPartSize(DataSize.of(42, MEGABYTE))
                .setRetryMode(STANDARD)
                .setMaxErrorRetries(12)
                .setSseType(S3SseType.KMS)
                .setSseKmsKeyId("mykey")
                .setUseWebIdentityTokenCredentialsProvider(true)
                .setSseCustomerKey("customerKey")
                .setRequesterPays(true)
                .setMaxConnections(42)
                .setConnectionTtl(new Duration(1, MINUTES))
                .setConnectionMaxIdleTime(new Duration(2, MINUTES))
                .setSocketConnectTimeout(new Duration(3, MINUTES))
                .setSocketReadTimeout(new Duration(4, MINUTES))
                .setTcpKeepAlive(true)
                .setHttpProxy(HostAndPort.fromParts("localhost", 8888))
                .setHttpProxySecure(true)
                .setNonProxyHosts("test1, test2, test3")
                .setHttpProxyUsername("test")
                .setHttpProxyPassword("test")
                .setHttpProxyPreemptiveBasicProxyAuth(true)
                .setSupportsExclusiveCreate(false)
                .setApplicationId("application id");

        assertFullMapping(properties, expected);
    }

    @Test
    public void testSSEWithCustomerKeyValidation()
    {
        assertFailsValidation(new S3FileSystemConfig()
                        .setSseType(S3SseType.CUSTOMER),
                "sseWithCustomerKeyConfigValid",
                "s3.sse.customer-key has to be set for server-side encryption with customer-provided key",
                AssertTrue.class);
    }
}
