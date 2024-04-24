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
package io.trino.plugin.opensearch;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.trino.plugin.opensearch.AwsSecurityConfig.DeploymentType.SERVERLESS;

public class TestAwsSecurityConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AwsSecurityConfig.class)
                .setAccessKey(null)
                .setSecretKey(null)
                .setRegion(null)
                .setIamRole(null)
                .setExternalId(null)
                .setDeploymentType(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("opensearch.aws.access-key", "access")
                .put("opensearch.aws.secret-key", "secret")
                .put("opensearch.aws.region", "region")
                .put("opensearch.aws.iam-role", "iamRole")
                .put("opensearch.aws.external-id", "externalId")
                .put("opensearch.aws.deployment-type", "SERVERLESS")
                .buildOrThrow();

        AwsSecurityConfig expected = new AwsSecurityConfig()
                .setAccessKey("access")
                .setSecretKey("secret")
                .setRegion("region")
                .setIamRole("iamRole")
                .setExternalId("externalId")
                .setDeploymentType(SERVERLESS);

        assertFullMapping(properties, expected);
    }
}
