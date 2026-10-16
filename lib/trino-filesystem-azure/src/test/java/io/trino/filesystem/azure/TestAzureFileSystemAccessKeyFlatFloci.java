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
package io.trino.filesystem.azure;

import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static io.trino.filesystem.azure.AbstractTestAzureFileSystem.AccountKind.FLAT;
import static io.trino.testing.containers.FlociAzure.FLOCI_AZURE_ACCOUNT_KEY;
import static io.trino.testing.containers.FlociAzure.FLOCI_AZURE_FLAT_ACCOUNT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(Lifecycle.PER_CLASS)
public class TestAzureFileSystemAccessKeyFlatFloci
        extends AbstractTestAzureFileSystemFloci
{
    @BeforeAll
    void setup()
            throws IOException
    {
        initializeWithAccessKeyAndMultipartWrites(
                FLOCI_AZURE_FLAT_ACCOUNT,
                FLOCI_AZURE_ACCOUNT_KEY,
                FLAT);
    }

    @Test
    void testServiceEndpointConfiguration()
            throws IOException, URISyntaxException
    {
        URI endpoint = flociAzure().getEndpoint();
        URI httpEndpoint = new URI("http", null, endpoint.getHost(), endpoint.getPort(), "/", null, null);
        AzureFileSystemFactory factory = new AzureFileSystemFactory(
                OpenTelemetry.noop(),
                new AzureAuthAccessKey(FLOCI_AZURE_ACCOUNT_KEY),
                new AzureFileSystemConfig().setServiceEndpoint(httpEndpoint));
        try {
            TrinoFileSystem fileSystem = factory.create(ConnectorIdentity.ofUser("test"));
            Location location = getRootLocation().appendPath("service-endpoint");
            byte[] data = "service endpoint".getBytes(UTF_8);
            fileSystem.newOutputFile(location).createOrOverwrite(data);
            try (TrinoInput input = fileSystem.newInputFile(location).newInput()) {
                assertThat(input.readFully(0, data.length).getBytes()).isEqualTo(data);
            }
            fileSystem.deleteFile(location);
        }
        finally {
            factory.destroy();
        }
    }

    @Test
    void testServiceEndpointOverrideStillValidatesLocationEndpoint()
    {
        Location location = Location.of("abfs://test@%s.dfs.core.chinacloudapi.cn/file".formatted(FLOCI_AZURE_FLAT_ACCOUNT));

        assertThatThrownBy(() -> getFileSystem().newInputFile(location).exists())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Location does not match configured Azure endpoint");
    }
}
