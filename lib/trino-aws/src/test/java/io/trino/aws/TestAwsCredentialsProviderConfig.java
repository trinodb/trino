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
package io.trino.aws;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestAwsCredentialsProviderConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AwsCredentialsProviderConfig.class)
                .setServiceUri(null)
                .setCustomCredentialsProvider(null)
                .setRegion(null)
                .setStsEndpoint(null)
                .setStsRegion(null)
                .setProxyApiId(null)
                .setIamRole(null)
                .setIamRoleSessionName("trino-session")
                .setExternalId(null)
                .setAccessKey(null)
                .setSecretKey(null)
                .setSessionToken(null));
    }

    @Test
    public void testExplicitPropertyMapping()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("service-uri", "proto://service")
                .put("credentials-provider", "custom")
                .put("region", "us-east-1")
                .put("sts-endpoint", "STS override")
                .put("sts-region", "us-east-2")
                .put("proxy-api-id", "abc123")
                .put("iam-role", "role")
                .put("iam-session-name", "custom-session")
                .put("external-id", "external-id")
                .put("access-key", "ABC")
                .put("secret-key", "DEF")
                .put("session-token", "GHI")
                .buildOrThrow();

        AwsCredentialsProviderConfig expected = new AwsCredentialsProviderConfig()
                .setServiceUri(URI.create("proto://service"))
                .setCustomCredentialsProvider("custom")
                .setRegion("us-east-1")
                .setStsEndpoint("STS override")
                .setStsRegion("us-east-2")
                .setProxyApiId("abc123")
                .setIamRole("role")
                .setIamRoleSessionName("custom-session")
                .setExternalId("external-id")
                .setAccessKey("ABC")
                .setSecretKey("DEF")
                .setSessionToken("GHI");

        assertFullMapping(properties, expected);
    }
}
