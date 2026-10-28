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
package io.trino.plugin.exchange.hdfs;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.exchange.filesystem.CommitManifest;
import io.trino.plugin.exchange.filesystem.FileStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
import static org.assertj.core.api.Assertions.assertThat;

class TestHadoopCommitManifest
{
    @Test
    void testManifestRoundTripAndCleanup(@TempDir Path temporaryDirectory)
            throws IOException
    {
        try (var storage = createStorage(temporaryDirectory)) {
            Path partitionDirectory = temporaryDirectory.resolve("exchange/partition");
            URI directory = Files.createDirectories(partitionDirectory.resolve("0")).toUri();
            URI file = directory.resolve("0_0.data");
            var writer = storage.createExchangeStorageWriter(file);
            getFutureValue(writer.write(utf8Slice("first")));
            getFutureValue(writer.write(utf8Slice("second")));
            getFutureValue(writer.finish());
            assertThat(writer.getFileStatus()).isEqualTo(new FileStatus(file.toString(), 11));

            var manifest = new CommitManifest(ImmutableList.of(writer.getFileStatus()));
            getFutureValue(storage.writeMarkerFile(directory, manifest));
            assertThat(getFutureValue(storage.readMarkerFile(directory)).files()).containsExactlyElementsOf(manifest.files());
            URI marker = directory.resolve(CommitManifest.FILE_NAME);
            assertThat(getFutureValue(storage.readMarkerFile(marker)).files()).containsExactlyElementsOf(manifest.files());

            getFutureValue(storage.deleteFiles(ImmutableList.of(file, marker)));
            assertThat(Path.of(file)).doesNotExist();
            assertThat(Path.of(marker)).doesNotExist();
            assertThat(Path.of(directory)).doesNotExist();
            assertThat(partitionDirectory).doesNotExist();
            assertThat(temporaryDirectory.resolve("exchange")).isDirectory();
            assertThat(temporaryDirectory.resolve("core-site.xml")).exists();
        }
    }

    @Test
    void testCleanupPreservesUnlistedFiles(@TempDir Path temporaryDirectory)
            throws IOException
    {
        try (var storage = createStorage(temporaryDirectory)) {
            for (String unlistedPath : ImmutableList.of("0/unlisted.data", "1/unlisted.data")) {
                Path partitionDirectory = Files.createTempDirectory(temporaryDirectory, "partition");
                Path attemptDirectory = Files.createDirectory(partitionDirectory.resolve("0"));
                Path unlistedFile = partitionDirectory.resolve(unlistedPath);
                Files.createDirectories(unlistedFile.getParent());
                Files.writeString(unlistedFile, "keep");
                Path dataFile = Files.createFile(attemptDirectory.resolve("0_0.data"));
                Path markerFile = Files.createFile(attemptDirectory.resolve(CommitManifest.FILE_NAME));

                getFutureValue(storage.deleteFiles(ImmutableList.of(dataFile.toUri(), markerFile.toUri())));
                assertThat(dataFile).doesNotExist();
                assertThat(markerFile).doesNotExist();
                assertThat(unlistedFile).hasContent("keep");
                assertThat(partitionDirectory).isDirectory();
                if (unlistedFile.getParent().equals(attemptDirectory)) {
                    assertThat(attemptDirectory).isDirectory();
                }
                else {
                    assertThat(attemptDirectory).doesNotExist();
                }
                getFutureValue(storage.deleteFiles(ImmutableList.of(dataFile.toUri(), markerFile.toUri())));
                assertThat(unlistedFile).hasContent("keep");
            }
        }
    }

    private static HadoopFileSystemExchangeStorage createStorage(Path temporaryDirectory)
            throws IOException
    {
        Path configFile = temporaryDirectory.resolve("core-site.xml");
        Files.writeString(configFile,
                """
                <configuration>
                    <property>
                        <name>fs.defaultFS</name>
                        <value>file:///</value>
                    </property>
                    <property>
                        <name>fs.file.impl</name>
                        <value>org.apache.hadoop.fs.RawLocalFileSystem</value>
                    </property>
                    <property>
                        <name>fs.file.impl.disable.cache</name>
                        <value>true</value>
                    </property>
                </configuration>
                """);
        var config = new ExchangeHdfsConfig().setResourceConfigFiles(ImmutableList.of(configFile.toString()));
        return new HadoopFileSystemExchangeStorage(config);
    }
}
