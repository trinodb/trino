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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

final class TestDistributionWriter
{
    private static final Instant TIMESTAMP = Instant.parse("2026-07-18T00:54:20Z");

    @Test
    void testDuplicateJarsAcrossArchivesBecomeHardLinks(@TempDir Path directory)
            throws IOException
    {
        Path first = zip(directory.resolve("first.zip"), "first-1/", ImmutableMap.of("guava.jar", "shared", "only-first.jar", "one"));
        Path second = zip(directory.resolve("second.zip"), "second-1/", ImmutableMap.of("guava.jar", "shared", "only-second.jar", "two"));

        Path archive = directory.resolve("out.tar.gz");
        try (DistributionWriter writer = writer(archive, Optional.empty())) {
            writer.addArchive("plugin/first", first, true);
            writer.addArchive("plugin/second", second, true);
        }

        Map<String, TarArchiveEntry> entries = read(archive);
        assertThat(entries.get("root/plugin/first/guava.jar").isLink()).isFalse();

        TarArchiveEntry duplicate = entries.get("root/plugin/second/guava.jar");
        assertThat(duplicate.isLink()).isTrue();
        assertThat(duplicate.getLinkName()).isEqualTo("root/plugin/first/guava.jar");

        assertThat(entries.get("root/plugin/second/only-second.jar").isLink()).isFalse();
    }

    @Test
    void testOverlayIsLayeredIntoEachNamedPlugin(@TempDir Path directory)
            throws IOException
    {
        Path plugin = zip(directory.resolve("plugin.zip"), "trino-hive-1/", ImmutableMap.of("hive.jar", "a"));
        Path hdfs = zip(directory.resolve("hdfs.zip"), "hdfs/", ImmutableMap.of("hadoop.jar", "b"));

        Path archive = directory.resolve("out.tar.gz");
        try (DistributionWriter writer = writer(archive, Optional.empty())) {
            writer.addArchive("plugin/hive", plugin, true);
            writer.addArchive("plugin/hive", hdfs, false);
        }

        assertThat(read(archive).keySet())
                .contains("root/plugin/hive/hive.jar", "root/plugin/hive/hdfs/hadoop.jar");
    }

    @Test
    void testHardLinksInsideSourceTarAreResolved(@TempDir Path directory)
            throws IOException
    {
        // Reproduces trino-server consuming the trino-server-core archive, which
        // itself contains hard link entries written by this class.
        Path source = directory.resolve("core.tar.gz");
        try (TarArchiveOutputStream out = new TarArchiveOutputStream(new GzipCompressorOutputStream(Files.newOutputStream(source)))) {
            TarArchiveEntry original = new TarArchiveEntry("core-1/lib/guava.jar");
            byte[] contents = "shared".getBytes(StandardCharsets.UTF_8);
            original.setSize(contents.length);
            out.putArchiveEntry(original);
            out.write(contents);
            out.closeArchiveEntry();

            TarArchiveEntry link = new TarArchiveEntry("core-1/plugin/jmx/guava.jar", TarArchiveEntry.LF_LINK);
            link.setLinkName("core-1/lib/guava.jar");
            out.putArchiveEntry(link);
            out.closeArchiveEntry();
        }

        Path archive = directory.resolve("out.tar.gz");
        try (DistributionWriter writer = writer(archive, Optional.empty())) {
            writer.addArchive("", source, true);
        }

        Map<String, TarArchiveEntry> entries = read(archive);
        assertThat(entries.get("root/lib/guava.jar").isLink()).isFalse();
        assertThat(entries.get("root/plugin/jmx/guava.jar").isLink()).isTrue();
        assertThat(entries.get("root/plugin/jmx/guava.jar").getLinkName()).isEqualTo("root/lib/guava.jar");
    }

    @Test
    void testExplodedTreeMatchesArchiveAndSharesStorage(@TempDir Path directory)
            throws IOException
    {
        Path first = zip(directory.resolve("first.zip"), "first-1/", ImmutableMap.of("guava.jar", "shared"));
        Path second = zip(directory.resolve("second.zip"), "second-1/", ImmutableMap.of("guava.jar", "shared"));

        Path exploded = directory.resolve("exploded");
        Path archive = directory.resolve("out.tar.gz");
        try (DistributionWriter writer = writer(archive, Optional.of(exploded))) {
            writer.addArchive("plugin/first", first, true);
            writer.addArchive("plugin/second", second, true);
        }

        Path original = exploded.resolve("plugin/first/guava.jar");
        Path duplicate = exploded.resolve("plugin/second/guava.jar");
        assertThat(Files.readString(original)).isEqualTo("shared");
        assertThat(Files.readString(duplicate)).isEqualTo("shared");

        // The duplicate is hard linked, so the tree does not pay for it twice.
        assertThat(Files.readAttributes(duplicate, "unix:ino").get("ino"))
                .isEqualTo(Files.readAttributes(original, "unix:ino").get("ino"));
    }

    @Test
    void testFilteredFileIsSubstitutedAndNotDeduplicated(@TempDir Path directory)
            throws IOException
    {
        Path source = zip(directory.resolve("launcher.zip"), "launcher-1/", ImmutableMap.of(
                "launcher.properties", "main-class=${main-class}\nprocess-name=${process-name}\n",
                "launcher", "exec \"${DIR}/launcher\"\n"));

        Path archive = directory.resolve("out.tar.gz");
        try (DistributionWriter writer = new DistributionWriter(
                archive,
                "root",
                TIMESTAMP,
                DistributionWriter.DEFAULT_COMPRESSION_LEVEL,
                ImmutableList.of(FileSystems.getDefault().getPathMatcher("glob:*.jar")),
                Optional.empty(),
                ImmutableList.of("bin/launcher.properties"),
                ImmutableMap.of("main-class", "io.trino.server.TrinoServer", "process-name", "trino-server"))) {
            writer.addArchive("bin", source, true);
        }

        assertThat(contents(archive, "root/bin/launcher.properties"))
                .isEqualTo("main-class=io.trino.server.TrinoServer\nprocess-name=trino-server\n");

        // Shell expansions of the same shape must survive untouched.
        assertThat(contents(archive, "root/bin/launcher")).isEqualTo("exec \"${DIR}/launcher\"\n");
    }

    @Test
    void testExecutableBitSurvivesFromZip(@TempDir Path directory)
            throws IOException
    {
        Path source = directory.resolve("bin.zip");
        try (ZipArchiveOutputStream out = new ZipArchiveOutputStream(Files.newOutputStream(source))) {
            write(out, "bin-1/launcher", "#!/bin/sh\n", 0755);
            write(out, "bin-1/launcher.properties", "key=value\n", 0644);
        }

        Path archive = directory.resolve("out.tar.gz");
        try (DistributionWriter writer = writer(archive, Optional.empty())) {
            writer.addArchive("bin", source, true);
        }

        Map<String, TarArchiveEntry> entries = read(archive);
        assertThat(entries.get("root/bin/launcher").getMode() & 0777).isEqualTo(0755);
        assertThat(entries.get("root/bin/launcher.properties").getMode() & 0777).isEqualTo(0644);
    }

    @Test
    void testArchiveIsReproducible(@TempDir Path directory)
            throws IOException
    {
        Path source = zip(directory.resolve("plugin.zip"), "plugin-1/", ImmutableMap.of("one.jar", "one", "two.jar", "two"));

        Path first = directory.resolve("first.tar.gz");
        Path second = directory.resolve("second.tar.gz");
        for (Path archive : ImmutableList.of(first, second)) {
            try (DistributionWriter writer = writer(archive, Optional.empty())) {
                writer.addArchive("plugin/one", source, true);
            }
        }

        assertThat(Files.readAllBytes(first)).isEqualTo(Files.readAllBytes(second));
    }

    @Test
    void testOwnershipAndTimestampAreFixed(@TempDir Path directory)
            throws IOException
    {
        Path file = directory.resolve("one.jar");
        Files.writeString(file, "one");

        Path archive = directory.resolve("out.tar.gz");
        try (DistributionWriter writer = writer(archive, Optional.empty())) {
            writer.addFile("lib/one.jar", file);
        }

        TarArchiveEntry entry = read(archive).get("root/lib/one.jar");
        assertThat(entry.getLongUserId()).isEqualTo(0);
        assertThat(entry.getLongGroupId()).isEqualTo(0);
        assertThat(entry.getUserName()).isEmpty();
        assertThat(entry.getLastModifiedTime().toInstant()).isEqualTo(TIMESTAMP);
    }

    @Test
    void testParentDirectoryEntriesArePresentAndPrecedeTheirContents(@TempDir Path directory)
            throws IOException
    {
        Path source = zip(directory.resolve("plugin.zip"), "plugin-1/", ImmutableMap.of("one.jar", "one"));

        Path archive = directory.resolve("out.tar.gz");
        try (DistributionWriter writer = writer(archive, Optional.empty())) {
            writer.addArchive("plugin/hive", source, true);
        }

        List<String> names = ImmutableList.copyOf(read(archive).keySet());
        assertThat(names).contains("root/plugin/", "root/plugin/hive/");
        assertThat(names.indexOf("root/plugin/")).isLessThan(names.indexOf("root/plugin/hive/"));
        assertThat(names.indexOf("root/plugin/hive/")).isLessThan(names.indexOf("root/plugin/hive/one.jar"));
    }

    private static DistributionWriter writer(Path archive, Optional<Path> explodeDirectory)
            throws IOException
    {
        return new DistributionWriter(
                archive,
                "root",
                TIMESTAMP,
                DistributionWriter.DEFAULT_COMPRESSION_LEVEL,
                ImmutableList.of(FileSystems.getDefault().getPathMatcher("glob:*.jar")),
                explodeDirectory,
                ImmutableList.of(),
                ImmutableMap.of());
    }

    private static Path zip(Path archive, String rootDirectory, Map<String, String> entries)
            throws IOException
    {
        try (ZipArchiveOutputStream out = new ZipArchiveOutputStream(Files.newOutputStream(archive))) {
            for (Map.Entry<String, String> entry : entries.entrySet()) {
                write(out, rootDirectory + entry.getKey(), entry.getValue(), 0644);
            }
        }
        return archive;
    }

    private static void write(ZipArchiveOutputStream out, String name, String contents, int mode)
            throws IOException
    {
        ZipArchiveEntry entry = new ZipArchiveEntry(name);
        entry.setUnixMode(mode);
        out.putArchiveEntry(entry);
        out.write(contents.getBytes(StandardCharsets.UTF_8));
        out.closeArchiveEntry();
    }

    private static Map<String, TarArchiveEntry> read(Path archive)
            throws IOException
    {
        Map<String, TarArchiveEntry> entries = new LinkedHashMap<>();
        try (TarArchiveInputStream input = open(archive)) {
            TarArchiveEntry entry;
            while ((entry = input.getNextEntry()) != null) {
                entries.put(entry.getName(), entry);
            }
        }
        return entries;
    }

    private static String contents(Path archive, String name)
            throws IOException
    {
        try (TarArchiveInputStream input = open(archive)) {
            TarArchiveEntry entry;
            while ((entry = input.getNextEntry()) != null) {
                if (entry.getName().equals(name)) {
                    return new String(input.readAllBytes(), StandardCharsets.UTF_8);
                }
            }
        }
        throw new AssertionError("Entry not found: " + name);
    }

    private static TarArchiveInputStream open(Path archive)
            throws IOException
    {
        return new TarArchiveInputStream(GzipCompressorInputStream.builder()
                .setInputStream(Files.newInputStream(archive))
                .setDecompressConcatenated(true)
                .get());
    }
}
