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
package io.trino.plugin.hive.s3;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestS3SecurityMappingConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(S3SecurityMappingConfig.class)
                .setJsonPointer("")
                .setConfigFilePath(null)
                .setRoleCredentialName(null)
                .setKmsKeyIdCredentialName(null)
                .setRefreshPeriod(null)
                .setColonReplacement(null));
    }

    @Test
    public void testExplicitPropertyMappingsWithFile()
            throws IOException
    {
        Path securityMappingConfigFile = Files.createTempFile(null, null);

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.s3.security-mapping.config-file", securityMappingConfigFile.toString())
                .put("hive.s3.security-mapping.json-pointer", "/data")
                .put("hive.s3.security-mapping.iam-role-credential-name", "iam-role-credential-name")
                .put("hive.s3.security-mapping.kms-key-id-credential-name", "kms-key-id-credential-name")
                .put("hive.s3.security-mapping.refresh-period", "1s")
                .put("hive.s3.security-mapping.colon-replacement", "#")
                .build();

        S3SecurityMappingConfig expected = new S3SecurityMappingConfig()
                .setConfigFilePath(securityMappingConfigFile.toString())
                .setJsonPointer("/data")
                .setRoleCredentialName("iam-role-credential-name")
                .setKmsKeyIdCredentialName("kms-key-id-credential-name")
                .setRefreshPeriod(new Duration(1, SECONDS))
                .setColonReplacement("#");

        assertFullMapping(properties, expected);
    }

    @Test
    public void testExplicitPropertyMappingsWithUrl()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.s3.security-mapping.config-file", "http://test:1234/example")
                .put("hive.s3.security-mapping.json-pointer", "/data")
                .put("hive.s3.security-mapping.iam-role-credential-name", "iam-role-credential-name")
                .put("hive.s3.security-mapping.kms-key-id-credential-name", "kms-key-id-credential-name")
                .put("hive.s3.security-mapping.refresh-period", "1s")
                .put("hive.s3.security-mapping.colon-replacement", "#")
                .build();

        S3SecurityMappingConfig expected = new S3SecurityMappingConfig()
                .setConfigFilePath("http://test:1234/example")
                .setJsonPointer("/data")
                .setRoleCredentialName("iam-role-credential-name")
                .setKmsKeyIdCredentialName("kms-key-id-credential-name")
                .setRefreshPeriod(new Duration(1, SECONDS))
                .setColonReplacement("#");

        assertFullMapping(properties, expected);
    }
}
