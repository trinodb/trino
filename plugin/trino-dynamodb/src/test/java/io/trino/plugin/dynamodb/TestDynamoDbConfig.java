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
package io.trino.plugin.dynamodb;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestDynamoDbConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DynamoDbConfig.class)
                .setAwsAccessKey(null)
                .setAwsSecretKey(null)
                .setAwsRegion(null)
                .setScanSegments(1));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("dynamodb.aws-access-key", "AKIAIOSFODNN7EXAMPLE")
                .put("dynamodb.aws-secret-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
                .put("dynamodb.aws-region", "us-west-2")
                .put("dynamodb.scan-segments", "4")
                .buildOrThrow();

        DynamoDbConfig expected = new DynamoDbConfig()
                .setAwsAccessKey("AKIAIOSFODNN7EXAMPLE")
                .setAwsSecretKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
                .setAwsRegion("us-west-2")
                .setScanSegments(4);

        assertFullMapping(properties, expected);
    }
}
