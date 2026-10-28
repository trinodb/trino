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
        try (var storage = new HadoopFileSystemExchangeStorage(config)) {
            URI directory = Files.createDirectory(temporaryDirectory.resolve("exchange")).toUri();
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
            assertThat(configFile).exists();
        }
    }
}
