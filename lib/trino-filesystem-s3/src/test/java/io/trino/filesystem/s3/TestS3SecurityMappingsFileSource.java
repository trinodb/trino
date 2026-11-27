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
import com.google.common.primitives.Bytes;
import io.airlift.configuration.secrets.SecretsResolver;
import io.trino.filesystem.Location;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_16;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestS3SecurityMappingsFileSource
{
    private static final String JSON_WITH_SECRETS = "{\"mappings\": [{\"iamRole\":\"${TESTING:my-role}\",\"prefix\":\"s3://my-bucket/\",\"user\":\"test\"}]}";

    @Test
    public void testSecretsResolution(@TempDir Path tempDir)
            throws IOException
    {
        Path configFile = tempDir.resolve("mappings.json");
        Files.writeString(configFile, JSON_WITH_SECRETS);

        assertSecretsResolved(configFile);
    }

    @Test
    public void testUtf8WithByteOrderMark(@TempDir Path tempDir)
            throws IOException
    {
        Path configFile = tempDir.resolve("mappings.json");
        Files.write(configFile, Bytes.concat(new byte[] {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF}, JSON_WITH_SECRETS.getBytes(UTF_8)));

        assertSecretsResolved(configFile);
    }

    @Test
    public void testUtf16(@TempDir Path tempDir)
            throws IOException
    {
        Path configFile = tempDir.resolve("mappings.json");
        Files.writeString(configFile, JSON_WITH_SECRETS, UTF_16);

        assertSecretsResolved(configFile);
    }

    @Test
    public void testInvalidJson(@TempDir Path tempDir)
            throws IOException
    {
        Path configFile = tempDir.resolve("mappings.json");
        Files.writeString(configFile, "{\"mappings\": [");

        assertThatThrownBy(() -> createFileSource(configFile).get())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid security mapping file: " + configFile)
                .cause().hasMessage("Could not parse JSON");
    }

    @Test
    public void testUnresolvableSecret(@TempDir Path tempDir)
            throws IOException
    {
        Path configFile = tempDir.resolve("mappings.json");
        Files.writeString(configFile, "{\"mappings\": [{\"iamRole\":\"${UNKNOWN:my-role}\",\"user\":\"test\"}]}");

        assertThatThrownBy(() -> createFileSource(configFile).get())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid security mapping file: " + configFile)
                .cause().hasMessage("Failed to resolve secret in security mapping at '/mappings/0/iamRole': No secret provider for key 'unknown'");
    }

    private static void assertSecretsResolved(Path configFile)
    {
        S3SecurityMappings mappings = createFileSource(configFile).get();
        ConnectorIdentity identity = ConnectorIdentity.forUser("test").withGroups(ImmutableSet.of()).build();
        S3SecurityMapping mapping = mappings.getMapping(identity, new S3Location(Location.of("s3://my-bucket/test"))).orElseThrow();
        assertThat(mapping.iamRole()).hasValue("arn:aws:iam::resolved-my-role");
    }

    private static S3SecurityMappingsFileSource createFileSource(Path configFile)
    {
        S3SecurityMappingConfig config = new S3SecurityMappingConfig().setConfigFile(configFile.toFile());
        SecretsResolver secretsResolver = new SecretsResolver(ImmutableMap.of("testing", key -> "arn:aws:iam::resolved-" + key));
        return new S3SecurityMappingsFileSource(config, secretsResolver);
    }
}
