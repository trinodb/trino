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

import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.Location;
import io.trino.filesystem.azure.AbstractTestAzureFileSystemFloci.FlociAzureResource;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;
import java.util.Random;

import static io.trino.testing.containers.FlociAzure.FLOCI_AZURE_ACCOUNT_KEY;
import static io.trino.testing.containers.FlociAzure.FLOCI_AZURE_HIERARCHICAL_ACCOUNT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Runs the file system against an Azure emulator with a single HTTP request permit. A permit that is
 * leaked, or an event loop that blocks waiting for one, stalls the next request and fails the
 * test by timeout.
 */
@TestInstance(PER_CLASS)
@Timeout(60)
final class TestAzureFileSystemConcurrencyLimit
{
    private static final int SINGLE_UPLOAD_SIZE = 64 * 1024;
    private static final int BLOCK_SIZE = 256 * 1024;

    @AutoClose
    private final FlociAzureResource resource = new FlociAzureResource();

    private AzureFileSystemFactory fileSystemFactory;
    private AzureFileSystem fileSystem;

    @BeforeAll
    void setUpFileSystem()
    {
        fileSystemFactory = new AzureFileSystemFactory(
                OpenTelemetry.noop(),
                new AzureAuthAccessKey(FLOCI_AZURE_ACCOUNT_KEY),
                new AzureFileSystemConfig()
                        .setServiceEndpoint(resource.flociAzure().getEndpoint())
                        .setMaxHttpRequests(1)
                        .setMaxSingleUploadSize(DataSize.ofBytes(SINGLE_UPLOAD_SIZE))
                        .setWriteBlockSize(DataSize.ofBytes(BLOCK_SIZE)),
                Optional.of(resource.httpClient()));
        fileSystem = (AzureFileSystem) fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
        fileSystem.createBlobContainerClient(new AzureLocation(location("")), Optional.empty()).create();
    }

    @AfterAll
    void tearDownFileSystem()
    {
        if (fileSystemFactory != null) {
            fileSystemFactory.destroy();
        }
    }

    @Test
    void testSingleBlockUpload()
            throws IOException
    {
        // Above the single upload size and below the block size, so the upload sends one Put Block, then
        // Put Block List from the event loop that completes the Put Block while it still holds the only permit
        writeAndVerify(location("single-block"), data(SINGLE_UPLOAD_SIZE * 2, 1));
    }

    @Test
    void testMultiBlockUpload()
            throws IOException
    {
        // The upload subscribes up to the write concurrency of Put Block requests at once, so all but one wait
        // for the permit, and the event loop completing each Put Block subscribes the next one
        writeAndVerify(location("multi-block"), data(BLOCK_SIZE * 16 + 1, 2));
    }

    private void writeAndVerify(Location location, byte[] data)
            throws IOException
    {
        try (OutputStream outputStream = fileSystem.newOutputFile(location).create()) {
            outputStream.write(data);
        }
        assertThat(fileSystem.newInputFile(location).exists()).isTrue();
        try (InputStream inputStream = fileSystem.newInputFile(location).newStream()) {
            assertThat(inputStream.readAllBytes()).isEqualTo(data);
        }
    }

    private static Location location(String name)
    {
        return Location.of("abfs://test@%s.dfs.core.windows.net/%s".formatted(FLOCI_AZURE_HIERARCHICAL_ACCOUNT, name));
    }

    private static byte[] data(int size, int seed)
    {
        // random contents detect blocks committed in the wrong order
        byte[] data = new byte[size];
        new Random(seed).nextBytes(data);
        return data;
    }
}
