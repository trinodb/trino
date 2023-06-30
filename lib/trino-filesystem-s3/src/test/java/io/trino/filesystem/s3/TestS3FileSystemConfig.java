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
import io.trino.filesystem.s3.S3FileSystemConfig.S3SseType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

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
                .setSseType(S3SseType.NONE)
                .setSseKmsKeyId(null)
                .setStreamingPartSize(DataSize.of(16, MEGABYTE))
                .setRequesterPays(false)
                .setMaxConnections(null)
                .setHttpProxy(null)
                .setHttpProxySecure(false));
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
                .put("s3.sse.type", "KMS")
                .put("s3.sse.kms-key-id", "mykey")
                .put("s3.streaming.part-size", "42MB")
                .put("s3.requester-pays", "true")
                .put("s3.max-connections", "42")
                .put("s3.http-proxy", "localhost:8888")
                .put("s3.http-proxy.secure", "true")
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
                .setStreamingPartSize(DataSize.of(42, MEGABYTE))
                .setSseType(S3SseType.KMS)
                .setSseKmsKeyId("mykey")
                .setRequesterPays(true)
                .setMaxConnections(42)
                .setHttpProxy(HostAndPort.fromParts("localhost", 8888))
                .setHttpProxySecure(true);

        assertFullMapping(properties, expected);
    }
}
