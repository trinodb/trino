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
package io.trino.server;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

import static io.trino.server.ServerArchive.extractPlugins;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.newOutputStream;
import static java.nio.file.Files.readString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestServerArchive
{
    @Test
    void testExtractPlugins(@TempDir Path tempDir)
            throws IOException
    {
        Path archive = tempDir.resolve("trino-server-1.tar.gz");
        try (TarArchiveOutputStream archiveOutput = newArchive(archive)) {
            writeFile(archiveOutput, "trino-server-1/lib/shared.jar", "shared");
            writeHardLink(archiveOutput, "trino-server-1/plugin/example/shared.jar", "trino-server-1/lib/shared.jar");
            // a link is allowed to precede the entry holding its payload
            writeHardLink(archiveOutput, "trino-server-1/plugin/example/deferred.jar", "trino-server-1/lib/deferred.jar");
            writeFile(archiveOutput, "trino-server-1/lib/deferred.jar", "deferred");
            writeFile(archiveOutput, "trino-server-1/plugin/example/own.jar", "own");
            writeFile(archiveOutput, "trino-server-1/README.txt", "ignored");
        }

        Path plugins = extractPlugins(archive, tempDir.resolve("extracted"));

        assertThat(readString(plugins.resolve("example/own.jar"))).isEqualTo("own");
        assertThat(readString(plugins.resolve("example/shared.jar"))).isEqualTo("shared");
        assertThat(readString(plugins.resolve("example/deferred.jar"))).isEqualTo("deferred");
        assertThat(fileKey(plugins.resolve("example/shared.jar")))
                .as("linked plugin JAR shares its payload instead of copying it")
                .isEqualTo(fileKey(plugins.resolve("../lib/shared.jar")));
    }

    @Test
    void testExtractPluginsFromConcatenatedGzipMembers(@TempDir Path tempDir)
            throws IOException
    {
        ByteArrayOutputStream tar = new ByteArrayOutputStream();
        try (TarArchiveOutputStream archiveOutput = new TarArchiveOutputStream(tar)) {
            writeFile(archiveOutput, "trino-server-1/lib/shared.jar", "shared");
            writeHardLink(archiveOutput, "trino-server-1/plugin/example/shared.jar", "trino-server-1/lib/shared.jar");
            writeFile(archiveOutput, "trino-server-1/plugin/example/own.jar", "own");
        }

        // a streaming assembly writes fixed size gzip members, so the archive is a concatenation of them
        byte[] bytes = tar.toByteArray();
        int split = bytes.length / 2;
        Path archive = tempDir.resolve("trino-server-1.tar.gz");
        try (OutputStream output = newOutputStream(archive)) {
            writeGzipMember(output, bytes, 0, split);
            writeGzipMember(output, bytes, split, bytes.length - split);
        }

        Path plugins = extractPlugins(archive, tempDir.resolve("extracted"));

        assertThat(readString(plugins.resolve("example/own.jar"))).isEqualTo("own");
        assertThat(readString(plugins.resolve("example/shared.jar"))).isEqualTo("shared");
    }

    @Test
    void testExtractPluginsRejectsUnresolvedHardLink(@TempDir Path tempDir)
            throws IOException
    {
        Path archive = tempDir.resolve("trino-server-1.tar.gz");
        try (TarArchiveOutputStream archiveOutput = newArchive(archive)) {
            writeHardLink(archiveOutput, "trino-server-1/plugin/example/missing.jar", "trino-server-1/lib/missing.jar");
        }

        assertThatThrownBy(() -> extractPlugins(archive, tempDir.resolve("extracted")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("has no payload");
    }

    @Test
    void testExtractPluginsRejectsEscapingEntry(@TempDir Path tempDir)
            throws IOException
    {
        Path archive = tempDir.resolve("trino-server-1.tar.gz");
        try (TarArchiveOutputStream archiveOutput = newArchive(archive)) {
            writeFile(archiveOutput, "trino-server-1/plugin/../../escaped.jar", "escaped");
        }

        assertThatThrownBy(() -> extractPlugins(archive, tempDir.resolve("extracted")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("escapes the extraction directory");
    }

    private static TarArchiveOutputStream newArchive(Path archive)
            throws IOException
    {
        return new TarArchiveOutputStream(new GzipCompressorOutputStream(newOutputStream(archive)));
    }

    private static void writeGzipMember(OutputStream output, byte[] bytes, int offset, int length)
            throws IOException
    {
        // the member has to be finished without closing the file it is appended to
        GzipCompressorOutputStream member = new GzipCompressorOutputStream(new FilterOutputStream(output)
        {
            @Override
            public void close() {}
        });
        member.write(bytes, offset, length);
        member.close();
    }

    private static void writeFile(TarArchiveOutputStream archiveOutput, String name, String content)
            throws IOException
    {
        byte[] bytes = content.getBytes(UTF_8);
        TarArchiveEntry entry = new TarArchiveEntry(name);
        entry.setSize(bytes.length);
        archiveOutput.putArchiveEntry(entry);
        archiveOutput.write(bytes);
        archiveOutput.closeArchiveEntry();
    }

    private static void writeHardLink(TarArchiveOutputStream archiveOutput, String name, String linkName)
            throws IOException
    {
        TarArchiveEntry entry = new TarArchiveEntry(name, TarConstants.LF_LINK);
        entry.setLinkName(linkName);
        archiveOutput.putArchiveEntry(entry);
        archiveOutput.closeArchiveEntry();
    }

    private static Object fileKey(Path file)
            throws IOException
    {
        return Files.readAttributes(file, BasicFileAttributes.class).fileKey();
    }
}
