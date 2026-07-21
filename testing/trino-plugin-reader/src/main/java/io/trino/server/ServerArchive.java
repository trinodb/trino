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
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Reads the plugin tree out of a Trino server tarball.
 * <p>
 * Plugins are loaded through a {@link java.net.URLClassLoader}, which needs seekable JAR
 * files, so the entries have to be materialized on disk before they can be read. A server
 * tarball stores every duplicated JAR as a hard link to a single payload under {@code lib},
 * and those links are recreated here rather than copied, which keeps the extracted tree at
 * the size of its unique content.
 */
final class ServerArchive
{
    private static final String PLUGIN_PREFIX = "plugin/";
    private static final String JAR_SUFFIX = ".jar";

    private ServerArchive() {}

    /**
     * Extracts the plugin tree of {@code archive} into {@code targetDirectory} and returns
     * the directory holding the individual plugins.
     */
    static Path extractPlugins(Path archive, Path targetDirectory)
            throws IOException
    {
        Map<Path, Path> links = new LinkedHashMap<>();
        try (InputStream input = Files.newInputStream(archive);
                TarArchiveInputStream archiveInput = new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(input)))) {
            TarArchiveEntry entry;
            while ((entry = archiveInput.getNextEntry()) != null) {
                Optional<String> name = archiveRelativePath(entry.getName());
                if (name.isEmpty() || !isRequired(name.get())) {
                    continue;
                }
                Path target = resolveSafely(targetDirectory, name.get());
                if (entry.isDirectory()) {
                    Files.createDirectories(target);
                    continue;
                }
                Files.createDirectories(target.getParent());
                if (entry.isLink()) {
                    // the payload an entry links to can appear anywhere in the archive, so links are created once it is known to be extracted
                    Optional<String> linkName = archiveRelativePath(entry.getLinkName());
                    if (linkName.isEmpty()) {
                        throw new IOException("Unexpected hard link target in %s: %s".formatted(archive, entry.getLinkName()));
                    }
                    links.put(target, resolveSafely(targetDirectory, linkName.get()));
                    continue;
                }
                if (entry.isSymbolicLink()) {
                    // the launcher ships symbolic links, none of which are needed to load plugins
                    continue;
                }
                Files.copy(archiveInput, target);
            }
        }

        for (Map.Entry<Path, Path> link : links.entrySet()) {
            if (!Files.isRegularFile(link.getValue())) {
                throw new IOException("Hard link %s in %s has no payload at %s".formatted(link.getKey(), archive, link.getValue()));
            }
            Files.createLink(link.getKey(), link.getValue());
        }
        return targetDirectory.resolve("plugin");
    }

    /**
     * Strips the single root directory that a server tarball wraps its content in, so that
     * entries can be addressed by their position within the distribution.
     */
    private static Optional<String> archiveRelativePath(String name)
    {
        int separator = name.indexOf('/');
        if (separator < 0) {
            return Optional.empty();
        }
        return Optional.of(name.substring(separator + 1));
    }

    /**
     * Selects the plugin tree, plus every JAR outside of it, because a plugin JAR can be
     * stored as a hard link to a payload living under {@code lib}.
     */
    private static boolean isRequired(String name)
    {
        return name.startsWith(PLUGIN_PREFIX) || name.endsWith(JAR_SUFFIX);
    }

    private static Path resolveSafely(Path targetDirectory, String name)
            throws IOException
    {
        Path resolved = targetDirectory.resolve(name).normalize();
        if (!resolved.startsWith(targetDirectory)) {
            throw new IOException("Entry escapes the extraction directory: " + name);
        }
        return resolved;
    }
}
