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
package io.trino.plugin.iceberg.fileio;

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.local.LocalFileSystemFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestForwardingFileIo
{
    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(FileIO.class, ForwardingFileIo.class);
        assertAllMethodsOverridden(SupportsBulkOperations.class, ForwardingFileIo.class);
    }

    @Test
    public void testUseFileSizeFromMetadata()
            throws Exception
    {
        Path tempDir = Files.createTempDirectory("test_forwarding_fileio");
        Path filePath = tempDir.resolve("data.txt");
        Files.writeString(filePath, "test-data");

        LocalFileSystemFactory factory = new LocalFileSystemFactory(tempDir);
        TrinoFileSystem fileSystem = factory.create(SESSION);

        long actualLength = Files.size(filePath);

        try (ForwardingFileIo ignoringFileIo = new ForwardingFileIo(fileSystem, false)) {
            assertThat(ignoringFileIo.newInputFile("file:///data.txt", 1).getLength())
                    .isEqualTo(actualLength);
        }

        try (ForwardingFileIo usingFileIo = new ForwardingFileIo(fileSystem, true)) {
            assertThat(usingFileIo.newInputFile("file:///data.txt", 1).getLength())
                    .isEqualTo(1);
        }
        deleteRecursively(tempDir, ALLOW_INSECURE);
    }
}
