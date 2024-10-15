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
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;

import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestS3SecurityMappingConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(S3SecurityMappingConfig.class)
                .setJsonPointer("")
                .setConfigFile(null)
                .setConfigUri(null)
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

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("s3.security-mapping.config-file", securityMappingConfigFile.toString())
                .put("s3.security-mapping.json-pointer", "/data")
                .put("s3.security-mapping.iam-role-credential-name", "iam-role-credential-name")
                .put("s3.security-mapping.kms-key-id-credential-name", "kms-key-id-credential-name")
                .put("s3.security-mapping.refresh-period", "13s")
                .put("s3.security-mapping.colon-replacement", "#")
                .buildOrThrow();

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        S3SecurityMappingConfig config = configurationFactory.build(S3SecurityMappingConfig.class);

        assertThat(config.getConfigFile()).contains(securityMappingConfigFile.toFile());
        assertThat(config.getConfigUri()).isEmpty();
        assertThat(config.getJsonPointer()).isEqualTo("/data");
        assertThat(config.getRoleCredentialName()).contains("iam-role-credential-name");
        assertThat(config.getKmsKeyIdCredentialName()).contains("kms-key-id-credential-name");
        assertThat(config.getRefreshPeriod()).contains(new Duration(13, SECONDS));
        assertThat(config.getColonReplacement()).contains("#");
    }

    @Test
    public void testExplicitPropertyMappingsWithUrl()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("s3.security-mapping.config-uri", "http://test:1234/example")
                .put("s3.security-mapping.json-pointer", "/data")
                .put("s3.security-mapping.iam-role-credential-name", "iam-role-credential-name")
                .put("s3.security-mapping.kms-key-id-credential-name", "kms-key-id-credential-name")
                .put("s3.security-mapping.refresh-period", "13s")
                .put("s3.security-mapping.colon-replacement", "#")
                .buildOrThrow();

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        S3SecurityMappingConfig config = configurationFactory.build(S3SecurityMappingConfig.class);

        assertThat(config.getConfigFile()).isEmpty();
        assertThat(config.getConfigUri()).contains(URI.create("http://test:1234/example"));
        assertThat(config.getJsonPointer()).isEqualTo("/data");
        assertThat(config.getRoleCredentialName()).contains("iam-role-credential-name");
        assertThat(config.getKmsKeyIdCredentialName()).contains("kms-key-id-credential-name");
        assertThat(config.getRefreshPeriod()).contains(new Duration(13, SECONDS));
        assertThat(config.getColonReplacement()).contains("#");
    }

    @Test
    public void testConfigFileDoesNotExist()
    {
        File file = new File("/doesNotExist-" + UUID.randomUUID());
        assertFailsValidation(
                new S3SecurityMappingConfig()
                        .setConfigFile(file),
                "configFile",
                "file does not exist: " + file,
                FileExists.class);
    }
}
