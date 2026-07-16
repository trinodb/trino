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
package io.trino.plugin.base.encryption;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

final class TestAwsKmsConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AwsKmsConfig.class)
                .setRegion(null)
                .setEndpoint(null)
                .setStsRegion(null)
                .setStsEndpoint(null)
                .setIamRole(null)
                .setExternalId(null)
                .setAccessKey(null)
                .setSecretKey(null));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("aws.kms.region", "us-east-1")
                .put("aws.kms.endpoint", "http://foo.bar")
                .put("aws.kms.sts.region", "us-west-2")
                .put("aws.kms.sts.endpoint", "http://sts.foo.bar")
                .put("aws.kms.iam-role", "arn:aws:iam::123456789012:role/MyRole")
                .put("aws.kms.external-id", "my-external-id")
                .put("aws.kms.access-key", "ABC")
                .put("aws.kms.secret-key", "DEF")
                .buildOrThrow();

        AwsKmsConfig expected = new AwsKmsConfig()
                .setRegion("us-east-1")
                .setEndpoint(URI.create("http://foo.bar"))
                .setStsRegion("us-west-2")
                .setStsEndpoint(URI.create("http://sts.foo.bar"))
                .setIamRole("arn:aws:iam::123456789012:role/MyRole")
                .setExternalId("my-external-id")
                .setAccessKey("ABC")
                .setSecretKey("DEF");

        assertFullMapping(properties, expected);
    }
}
