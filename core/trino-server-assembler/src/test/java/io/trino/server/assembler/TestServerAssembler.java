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
package io.trino.server.assembler;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

final class TestServerAssembler
{
    /**
     * A plugin archive named {@code trino-<name>.zip} installs as {@code plugin/<name>},
     * so the pom's dependency list is the only place plugins are enumerated. An overlay
     * is layered into the plugins it names instead of installing as one of its own.
     */
    @Test
    void testPluginsArePlacedByNameAndOverlaysAreLayeredIn(@TempDir Path directory)
            throws IOException
    {
        Path stage = Files.createDirectories(directory.resolve("stage"));
        Files.writeString(stage.resolve("NOTICE"), "notice");

        Path plugins = Files.createDirectories(directory.resolve("plugins"));
        zip(plugins.resolve("trino-hive.zip"), "trino-hive-1/", ImmutableMap.of("hive.jar", "a"));
        zip(plugins.resolve("trino-iceberg.zip"), "trino-iceberg-1/", ImmutableMap.of("iceberg.jar", "b"));
        zip(plugins.resolve("trino-hdfs.zip"), "hdfs/", ImmutableMap.of("hadoop.jar", "c"));

        Path archive = directory.resolve("out.tar.gz");
        ServerAssembler.main(new String[] {
                "--stage", stage.toString(),
                "--plugins", plugins.toString(),
                "--output", archive.toString(),
                "--root-directory", "root",
                "--timestamp", "2026-07-18T00:54:20Z",
                "--overlay", "trino-hdfs=hive,iceberg",
        });

        List<String> names = names(archive);
        assertThat(names).contains(
                "root/NOTICE",
                "root/plugin/hive/hive.jar",
                "root/plugin/iceberg/iceberg.jar",
                "root/plugin/hive/hdfs/hadoop.jar",
                "root/plugin/iceberg/hdfs/hadoop.jar");

        // The overlay is not a plugin of its own.
        assertThat(names).noneMatch(name -> name.startsWith("root/plugin/hdfs/"));
    }

    private static void zip(Path archive, String rootDirectory, Map<String, String> entries)
            throws IOException
    {
        try (ZipArchiveOutputStream out = new ZipArchiveOutputStream(Files.newOutputStream(archive))) {
            for (Map.Entry<String, String> entry : entries.entrySet()) {
                ZipArchiveEntry zipEntry = new ZipArchiveEntry(rootDirectory + entry.getKey());
                zipEntry.setUnixMode(0644);
                out.putArchiveEntry(zipEntry);
                out.write(entry.getValue().getBytes(StandardCharsets.UTF_8));
                out.closeArchiveEntry();
            }
        }
    }

    private static List<String> names(Path archive)
            throws IOException
    {
        List<String> names = new ArrayList<>();
        try (TarArchiveInputStream input = new TarArchiveInputStream(GzipCompressorInputStream.builder()
                .setInputStream(Files.newInputStream(archive))
                .setDecompressConcatenated(true)
                .get())) {
            TarArchiveEntry entry;
            while ((entry = input.getNextEntry()) != null) {
                names.add(entry.getName());
            }
        }
        return names;
    }
}
