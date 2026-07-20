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
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.output.TeeOutputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.zip.CRC32;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Objects.requireNonNull;

/**
 * Streams a Trino distribution archive directly from its source artifacts.
 * <p>
 * Nothing is expanded to disk on the way. Plugin archives are read entry by entry
 * and written straight into the output, which matters because the distribution
 * contains roughly 6600 jars of which only about 900 are distinct: the redundant
 * copies become hard link entries and are never decompressed at all.
 * <p>
 * Duplicates are recognised by uncompressed size together with CRC32. A zip's
 * central directory carries both, so a duplicate is identified without reading any
 * of its content. Entries arriving from a tar have to be buffered to compute the
 * checksum, since the tar format does not record one.
 * <p>
 * Entry order follows the order in which sources are added, and within a source its
 * own entry order, so the output is deterministic. Hard link targets always precede
 * the entries pointing at them.
 */
final class DistributionWriter
        implements Closeable
{
    public static final int DEFAULT_COMPRESSION_LEVEL = 9;

    private static final int CHUNK_SIZE = 8 * 1024 * 1024;
    private static final int MAXIMUM_COMPRESSION_THREADS = 8;

    private final TarArchiveOutputStream output;
    private final String rootDirectory;
    private final Instant timestamp;
    private final List<PathMatcher> hardLinkIncludes;
    private final Optional<Path> explodeDirectory;
    private final Map<String, String> filters;
    private final List<String> filteredPaths;

    private final Map<Identity, String> linkTargets = new HashMap<>();
    private final Set<String> directories = new HashSet<>();
    private long entryCount;

    public DistributionWriter(
            Path outputFile,
            String rootDirectory,
            Instant timestamp,
            int compressionLevel,
            List<PathMatcher> hardLinkIncludes,
            Optional<Path> explodeDirectory,
            List<String> filteredPaths,
            Map<String, String> filters)
            throws IOException
    {
        this.rootDirectory = requireNonNull(rootDirectory, "rootDirectory is null");
        this.timestamp = requireNonNull(timestamp, "timestamp is null");
        this.hardLinkIncludes = ImmutableList.copyOf(requireNonNull(hardLinkIncludes, "hardLinkIncludes is null"));
        this.explodeDirectory = requireNonNull(explodeDirectory, "explodeDirectory is null");
        this.filteredPaths = ImmutableList.copyOf(requireNonNull(filteredPaths, "filteredPaths is null"));
        this.filters = Map.copyOf(requireNonNull(filters, "filters is null"));

        this.output = new TarArchiveOutputStream(new ParallelGzipOutputStream(
                new BufferedOutputStream(Files.newOutputStream(outputFile)),
                compressionLevel,
                CHUNK_SIZE,
                compressionThreads()));
        this.output.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
        this.output.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);

        if (explodeDirectory.isPresent()) {
            Files.createDirectories(explodeDirectory.get());
        }

        TarArchiveEntry root = new TarArchiveEntry(rootDirectory + "/");
        prepare(root, FileModes.DEFAULT_DIRECTORY_MODE);
        output.putArchiveEntry(root);
        output.closeArchiveEntry();
        entryCount++;
    }

    public long entryCount()
    {
        return entryCount;
    }

    /**
     * Adds a file that is already on disk, such as a jar staged into {@code lib} or
     * a loose file like {@code NOTICE}.
     */
    public void addFile(String name, Path file)
            throws IOException
    {
        add(name, FileModes.modeOf(file, false), Content.of(file));
    }

    /**
     * Adds every entry of an archive under the given destination prefix.
     *
     * @param stripRootDirectory drop the archive's single root directory, so its
     *         contents land directly under the prefix
     */
    public void addArchive(String prefix, Path archive, boolean stripRootDirectory)
            throws IOException
    {
        if (archive.getFileName().toString().endsWith(".zip")) {
            addZip(prefix, archive, stripRootDirectory);
            return;
        }
        addTarGz(prefix, archive, stripRootDirectory);
    }

    private void addZip(String prefix, Path archive, boolean stripRootDirectory)
            throws IOException
    {
        try (ZipFile zip = ZipFile.builder().setPath(archive).get()) {
            List<ZipArchiveEntry> entries = new ArrayList<>();
            zip.getEntries().asIterator().forEachRemaining(entries::add);
            entries.sort(Comparator.comparing(ZipArchiveEntry::getName));

            Optional<String> root = Optional.empty();
            if (stripRootDirectory) {
                root = singleRootDirectory(entries.stream().map(ZipArchiveEntry::getName).toList());
                checkArgument(root.isPresent(), "Archive has no single root directory to strip: %s", archive);
            }

            for (ZipArchiveEntry entry : entries) {
                if (entry.isDirectory()) {
                    continue;
                }
                Optional<String> name = destination(prefix, entry.getName(), root);
                if (name.isEmpty()) {
                    continue;
                }
                // The central directory already told us the size and checksum, so a
                // duplicate is resolved without touching the compressed content.
                add(name.get(), FileModes.normalize(entry.getUnixMode(), false), Content.of(zip, entry));
            }
        }
    }

    private void addTarGz(String prefix, Path archive, boolean stripRootDirectory)
            throws IOException
    {
        // A tar cannot be read out of order and carries no content checksum, so each
        // entry is buffered to compute one.
        Map<String, String> writtenNames = new HashMap<>();
        GzipCompressorInputStream gzip = GzipCompressorInputStream.builder()
                .setInputStream(new BufferedInputStream(Files.newInputStream(archive)))
                .setDecompressConcatenated(true)
                .get();

        Optional<String> root = Optional.empty();
        if (stripRootDirectory) {
            root = singleRootDirectory(tarEntryNames(archive));
            checkArgument(root.isPresent(), "Archive has no single root directory to strip: %s", archive);
        }

        try (TarArchiveInputStream input = new TarArchiveInputStream(gzip)) {
            ArchiveEntry archiveEntry;
            while ((archiveEntry = input.getNextEntry()) != null) {
                TarArchiveEntry entry = (TarArchiveEntry) archiveEntry;
                if (entry.isDirectory()) {
                    continue;
                }
                Optional<String> name = destination(prefix, entry.getName(), root);
                if (name.isEmpty()) {
                    continue;
                }

                // A hard link inside the source archive points at an entry already
                // written; reuse whatever destination that entry received.
                if (entry.isLink()) {
                    String target = writtenNames.get(entry.getLinkName());
                    checkArgument(target != null, "Hard link target not found in archive: %s", entry.getLinkName());
                    writeLink(name.get(), FileModes.normalize(entry.getMode(), false), target);
                    continue;
                }

                add(name.get(), FileModes.normalize(entry.getMode(), false), Content.buffer(input, entry.getSize()));
                // Link targets are recorded fully qualified, matching what writeLink expects.
                writtenNames.put(entry.getName(), qualified(name.get()));
            }
        }
    }

    private List<String> tarEntryNames(Path archive)
            throws IOException
    {
        ImmutableList.Builder<String> names = ImmutableList.builder();
        GzipCompressorInputStream gzip = GzipCompressorInputStream.builder()
                .setInputStream(new BufferedInputStream(Files.newInputStream(archive)))
                .setDecompressConcatenated(true)
                .get();
        try (TarArchiveInputStream input = new TarArchiveInputStream(gzip)) {
            ArchiveEntry entry;
            while ((entry = input.getNextEntry()) != null) {
                names.add(entry.getName());
            }
        }
        return names.build();
    }

    private void add(String name, int mode, Content content)
            throws IOException
    {
        createDirectories(name);

        if (filteredPaths.contains(name)) {
            // Filtering rewrites the content, so its checksum no longer identifies the
            // source artifact and it must not take part in deduplication.
            byte[] filtered = filter(content.readAll());
            writeContent(name, mode, filtered.length, new ByteArrayInputStream(filtered));
            return;
        }

        if (deduplicated(name)) {
            Identity identity = new Identity(content.size(), content.crc());
            String existing = linkTargets.get(identity);
            if (existing != null) {
                writeLink(name, mode, existing);
                return;
            }
            linkTargets.put(identity, qualified(name));
        }

        try (InputStream input = content.open()) {
            writeContent(name, mode, content.size(), input);
        }
    }

    private void writeContent(String name, int mode, long size, InputStream input)
            throws IOException
    {
        TarArchiveEntry entry = new TarArchiveEntry(qualified(name));
        entry.setSize(size);
        prepare(entry, mode);
        output.putArchiveEntry(entry);

        if (explodeDirectory.isEmpty()) {
            input.transferTo(output);
            output.closeArchiveEntry();
            entryCount++;
            return;
        }

        // With an exploded tree requested, feed the archive and the file from the same
        // pass. Materialising first and reading the file back would double the I/O.
        Path file = explodeDirectory.get().resolve(name);
        Files.createDirectories(file.getParent());
        try (OutputStream fileOutput = new BufferedOutputStream(Files.newOutputStream(file))) {
            input.transferTo(new TeeOutputStream(output, fileOutput));
        }
        FileModes.apply(file, mode);
        output.closeArchiveEntry();
        entryCount++;
    }

    private void writeLink(String name, int mode, String target)
            throws IOException
    {
        createDirectories(name);

        TarArchiveEntry entry = new TarArchiveEntry(qualified(name), TarArchiveEntry.LF_LINK);
        entry.setLinkName(target);
        prepare(entry, mode);
        output.putArchiveEntry(entry);
        output.closeArchiveEntry();
        entryCount++;

        if (explodeDirectory.isPresent()) {
            Path file = explodeDirectory.get().resolve(name);
            Path existing = explodeDirectory.get().resolve(target.substring(rootDirectory.length() + 1));
            Files.createDirectories(file.getParent());
            Files.deleteIfExists(file);
            try {
                // Hard linking keeps the exploded tree near the size of the archive
                // instead of several gigabytes.
                Files.createLink(file, existing);
            }
            catch (IOException | UnsupportedOperationException e) {
                Files.copy(existing, file, REPLACE_EXISTING);
            }
        }
    }

    private void createDirectories(String name)
            throws IOException
    {
        int separator = name.lastIndexOf('/');
        if (separator < 0) {
            return;
        }
        String parent = name.substring(0, separator);

        List<String> missing = new ArrayList<>();
        for (String current = parent; !current.isEmpty(); ) {
            if (!directories.add(current)) {
                break;
            }
            missing.add(current);
            int index = current.lastIndexOf('/');
            if (index < 0) {
                break;
            }
            current = current.substring(0, index);
        }

        for (int i = missing.size() - 1; i >= 0; i--) {
            TarArchiveEntry entry = new TarArchiveEntry(qualified(missing.get(i)) + "/");
            prepare(entry, FileModes.DEFAULT_DIRECTORY_MODE);
            output.putArchiveEntry(entry);
            output.closeArchiveEntry();
            entryCount++;
        }
    }

    private byte[] filter(byte[] contents)
    {
        String text = new String(contents, StandardCharsets.UTF_8);
        for (Map.Entry<String, String> property : filters.entrySet()) {
            text = text.replace("${" + property.getKey() + "}", property.getValue());
        }
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private String qualified(String name)
    {
        return rootDirectory + "/" + name;
    }

    private boolean deduplicated(String name)
    {
        Path path = Path.of(name).getFileName();
        return hardLinkIncludes.stream().anyMatch(matcher -> matcher.matches(path));
    }

    private void prepare(TarArchiveEntry entry, int mode)
    {
        entry.setModTime(timestamp.toEpochMilli());
        entry.setMode(entry.getMode() & ~0777 | mode);
        entry.setUserId(0);
        entry.setGroupId(0);
        entry.setUserName("");
        entry.setGroupName("");
    }

    private static Optional<String> destination(String prefix, String entryName, Optional<String> rootDirectory)
    {
        String name = entryName;
        if (name.endsWith("/")) {
            name = name.substring(0, name.length() - 1);
        }
        if (rootDirectory.isPresent()) {
            String root = rootDirectory.get() + "/";
            if (!name.startsWith(root)) {
                return Optional.empty();
            }
            name = name.substring(root.length());
        }
        if (name.isEmpty()) {
            return Optional.empty();
        }
        if (prefix.isEmpty()) {
            return Optional.of(name);
        }
        return Optional.of(prefix + "/" + name);
    }

    /**
     * Returns the directory every entry lives under, or empty if they do not share one.
     */
    private static Optional<String> singleRootDirectory(List<String> names)
    {
        Optional<String> root = Optional.empty();
        for (String entry : names) {
            String name = entry;
            if (name.endsWith("/")) {
                name = name.substring(0, name.length() - 1);
            }
            if (name.isEmpty()) {
                continue;
            }
            int separator = name.indexOf('/');
            if (separator < 0) {
                // Something sits at the archive root next to the candidate directory.
                if (root.isPresent() && !root.get().equals(name)) {
                    return Optional.empty();
                }
                root = Optional.of(name);
                continue;
            }
            String candidate = name.substring(0, separator);
            if (root.isPresent() && !root.get().equals(candidate)) {
                return Optional.empty();
            }
            root = Optional.of(candidate);
        }
        return root;
    }

    private static int compressionThreads()
    {
        return Math.max(1, Math.min(MAXIMUM_COMPRESSION_THREADS, Runtime.getRuntime().availableProcessors() - 1));
    }

    @Override
    public void close()
            throws IOException
    {
        output.close();
    }

    private record Identity(long size, long crc) {}

    /**
     * Content whose size and checksum are known before the first byte is read, so a
     * duplicate can be recognised without opening it.
     */
    private record Content(long size, long crc, InputStreamSupplier opener)
    {
        public interface InputStreamSupplier
        {
            InputStream open()
                    throws IOException;
        }

        /**
         * A zip records both values in its central directory.
         */
        public static Content of(ZipFile zip, ZipArchiveEntry entry)
        {
            return new Content(entry.getSize(), entry.getCrc(), () -> zip.getInputStream(entry));
        }

        public static Content of(Path file)
                throws IOException
        {
            CRC32 crc = new CRC32();
            byte[] buffer = new byte[65536];
            try (InputStream input = Files.newInputStream(file)) {
                int read;
                while ((read = input.read(buffer)) >= 0) {
                    crc.update(buffer, 0, read);
                }
            }
            return new Content(Files.size(file), crc.getValue(), () -> Files.newInputStream(file));
        }

        /**
         * Consumes an entry from a stream that cannot be re-read. A tar records no
         * checksum, so the content is held in memory while one is computed. Entries are
         * single jars, the largest of which is a few tens of megabytes.
         */
        public static Content buffer(InputStream input, long size)
                throws IOException
        {
            byte[] contents = input.readNBytes(toIntExact(size));
            CRC32 crc = new CRC32();
            crc.update(contents, 0, contents.length);
            return new Content(contents.length, crc.getValue(), () -> new ByteArrayInputStream(contents));
        }

        public InputStream open()
                throws IOException
        {
            return opener.open();
        }

        public byte[] readAll()
                throws IOException
        {
            try (InputStream input = open()) {
                return input.readAllBytes();
            }
        }
    }
}
