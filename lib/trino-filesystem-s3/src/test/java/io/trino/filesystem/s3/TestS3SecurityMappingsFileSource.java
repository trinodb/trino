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
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.secrets.SecretsResolver;
import io.trino.filesystem.Location;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class TestS3SecurityMappingsFileSource
{
    @Test
    public void testSecretsResolution(@TempDir Path tempDir)
            throws IOException
    {
        String jsonWithSecrets = "{\"mappings\": [{\"iamRole\":\"${TESTING:my-role}\",\"prefix\":\"s3://my-bucket/\",\"user\":\"test\"}]}";
        Path configFile = tempDir.resolve("mappings.json");
        Files.writeString(configFile, jsonWithSecrets);

        S3SecurityMappingConfig config = new S3SecurityMappingConfig().setConfigFile(configFile.toFile());
        SecretsResolver secretsResolver = new SecretsResolver(ImmutableMap.of("testing", key -> "arn:aws:iam::resolved-" + key));
        var provider = new S3SecurityMappingsFileSource(config, secretsResolver);

        S3SecurityMappings mappings = provider.get();
        ConnectorIdentity identity = ConnectorIdentity.forUser("test").withGroups(ImmutableSet.of()).build();
        S3SecurityMapping mapping = mappings.getMapping(identity, new S3Location(Location.of("s3://my-bucket/test"))).orElseThrow();
        assertThat(mapping.iamRole()).hasValue("arn:aws:iam::resolved-my-role");
    }
}
