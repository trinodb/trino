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
package io.trino.plugin.hive.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.spi.TrinoException;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Multimaps.asMap;
import static io.trino.hive.thrift.metastore.hive_metastoreConstants.TABLE_IS_TRANSACTIONAL;
import static io.trino.hive.thrift.metastore.hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_BUCKET_FILES;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.comparing;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

// based on org.apache.hadoop.hive.ql.io.AcidUtils
public final class AcidTables
{
    private AcidTables() {}

    public static boolean isInsertOnlyTable(Map<String, String> parameters)
    {
        return "insert_only".equalsIgnoreCase(parameters.get(TABLE_TRANSACTIONAL_PROPERTIES));
    }

    public static boolean isTransactionalTable(Map<String, String> parameters)
    {
        return "true".equalsIgnoreCase(parameters.get(TABLE_IS_TRANSACTIONAL)) ||
                "true".equalsIgnoreCase(parameters.get(TABLE_IS_TRANSACTIONAL.toUpperCase(ENGLISH)));
    }

    public static boolean isFullAcidTable(Map<String, String> parameters)
    {
        return isTransactionalTable(parameters) && !isInsertOnlyTable(parameters);
    }

    public static Path bucketFileName(Path subdir, int bucket)
    {
        return new Path(subdir, "bucket_%05d".formatted(bucket));
    }

    public static String deltaSubdir(long writeId, int statementId)
    {
        return "delta_%07d_%07d_%04d".formatted(writeId, writeId, statementId);
    }

    public static String deleteDeltaSubdir(long writeId, int statementId)
    {
        return "delete_" + deltaSubdir(writeId, statementId);
    }

    public static void writeAcidVersionFile(TrinoFileSystem fileSystem, String deltaOrBaseDir)
            throws IOException
    {
        TrinoOutputFile file = fileSystem.newOutputFile(versionFilePath(deltaOrBaseDir));
        try (var out = file.createOrOverwrite()) {
            out.write('2');
        }
    }

    public static int readAcidVersionFile(TrinoFileSystem fileSystem, String deltaOrBaseDir)
            throws IOException
    {
        TrinoInputFile file = fileSystem.newInputFile(versionFilePath(deltaOrBaseDir));
        if (!file.exists()) {
            return 0;
        }
        // Hive only reads one byte from the file
        try (var in = file.newStream()) {
            byte[] bytes = in.readNBytes(1);
            if (bytes.length == 1) {
                return parseInt(new String(bytes, UTF_8));
            }
            return 0;
        }
    }

    private static String versionFilePath(String deltaOrBaseDir)
    {
        return deltaOrBaseDir + "/_orc_acid_version";
    }

    public static AcidState getAcidState(TrinoFileSystem fileSystem, String directory, ValidWriteIdList writeIdList)
            throws IOException
    {
        // directory = /hive/data/abc
        // path = /hive/data/abc/base_00001/file.orc
        // suffix = base_00001/file.orc
        // name = base_00001
        // baseDir = /hive/data/abc/base_00001

        ListMultimap<String, FileEntry> groupedFiles = ArrayListMultimap.create();
        List<FileEntry> originalFiles = new ArrayList<>();

        for (FileEntry file : listFiles(fileSystem, directory)) {
            String suffix = listingSuffix(directory, file.path());

            int slash = suffix.indexOf('/');
            String name = (slash == -1) ? "" : suffix.substring(0, slash);

            if (name.startsWith("base_") || name.startsWith("delta_") || name.startsWith("delete_delta_")) {
                if (suffix.indexOf('/', slash + 1) != -1) {
                    throw new TrinoException(HIVE_INVALID_BUCKET_FILES, "Found file in sub-directory of ACID directory: " + file.path());
                }
                groupedFiles.put(name, file);
            }
            else if (file.length() > 0) {
                originalFiles.add(file);
            }
        }

        List<ParsedDelta> workingDeltas = new ArrayList<>();
        String oldestBase = null;
        long oldestBaseWriteId = Long.MAX_VALUE;
        String bestBasePath = null;
        long bestBaseWriteId = 0;
        List<FileEntry> bestBaseFiles = ImmutableList.of();

        for (var entry : asMap(groupedFiles).entrySet()) {
            String name = entry.getKey();
            String baseDir = directory + "/" + name;
            List<FileEntry> files = entry.getValue();

            if (name.startsWith("base_")) {
                ParsedBase base = parseBase(name);
                long writeId = base.writeId();
                if (oldestBaseWriteId > writeId) {
                    oldestBase = baseDir;
                    oldestBaseWriteId = writeId;
                }
                if (((bestBasePath == null) || (bestBaseWriteId < writeId)) &&
                        isValidBase(base, writeIdList, fileSystem, baseDir)) {
                    bestBasePath = baseDir;
                    bestBaseWriteId = writeId;
                    bestBaseFiles = files;
                }
            }
            else {
                String deltaPrefix = name.startsWith("delta_") ? "delta_" : "delete_delta_";
                ParsedDelta delta = parseDelta(baseDir, deltaPrefix, files);
                if (writeIdList.isWriteIdRangeValid(delta.min(), delta.max())) {
                    workingDeltas.add(delta);
                }
            }
        }

        if ((oldestBase != null) && (bestBasePath == null)) {
            long[] exceptions = writeIdList.getInvalidWriteIds();
            String minOpenWriteId = ((exceptions != null) && (exceptions.length > 0)) ? String.valueOf(exceptions[0]) : "x";
            throw new IOException("Not enough history available for (%s,%s). Oldest available base: %s"
                    .formatted(writeIdList.getHighWatermark(), minOpenWriteId, oldestBase));
        }

        if (bestBasePath != null) {
            originalFiles.clear();
        }

        originalFiles.sort(comparing(FileEntry::path));
        workingDeltas.sort(null);

        List<ParsedDelta> deltas = new ArrayList<>();
        long current = bestBaseWriteId;
        int lastStatementId = -1;
        ParsedDelta prev = null;
        for (ParsedDelta next : workingDeltas) {
            if (next.max() > current) {
                if (writeIdList.isWriteIdRangeValid(current + 1, next.max())) {
                    deltas.add(next);
                    current = next.max();
                    lastStatementId = next.statementId();
                    prev = next;
                }
            }
            else if ((next.max() == current) && (lastStatementId >= 0)) {
                deltas.add(next);
                prev = next;
            }
            else if ((prev != null) &&
                    (next.max() == prev.max()) &&
                    (next.min() == prev.min()) &&
                    (next.statementId() == prev.statementId())) {
                deltas.add(next);
                prev = next;
            }
        }

        return new AcidState(Optional.ofNullable(bestBasePath), bestBaseFiles, deltas, originalFiles);
    }

    private static boolean isValidBase(ParsedBase base, ValidWriteIdList writeIdList, TrinoFileSystem fileSystem, String baseDir)
            throws IOException
    {
        if (base.writeId() == Long.MIN_VALUE) {
            return true;
        }

        if ((base.visibilityId() > 0) || isCompacted(fileSystem, baseDir)) {
            return writeIdList.isValidBase(base.writeId());
        }

        return writeIdList.isWriteIdValid(base.writeId());
    }

    private static boolean isCompacted(TrinoFileSystem fileSystem, String baseDir)
            throws IOException
    {
        TrinoInputFile file = fileSystem.newInputFile(baseDir + "/_metadata_acid");
        if (!file.exists()) {
            return false;
        }

        Map<String, String> metadata;
        try (var in = file.newStream()) {
            metadata = new ObjectMapper().readValue(in, new TypeReference<>() {});
        }
        catch (IOException e) {
            throw new IOException("Failed to read %s: %s".formatted(file.location(), e.getMessage()), e);
        }

        String version = metadata.get("thisFileVersion");
        if (!"0".equals(version)) {
            throw new IOException("Unexpected ACID metadata version: " + version);
        }

        String format = metadata.get("dataFormat");
        if (!"compacted".equals(format)) {
            throw new IOException("Unexpected value for ACID dataFormat: " + format);
        }

        return true;
    }

    @VisibleForTesting
    static ParsedDelta parseDelta(String path, String deltaPrefix, List<FileEntry> files)
    {
        String fileName = path.substring(path.lastIndexOf('/') + 1);
        checkArgument(fileName.startsWith(deltaPrefix), "File does not start with '%s': %s", deltaPrefix, path);

        int visibility = fileName.indexOf("_v");
        if (visibility != -1) {
            fileName = fileName.substring(0, visibility);
        }

        boolean deleteDelta = deltaPrefix.equals("delete_delta_");

        String rest = fileName.substring(deltaPrefix.length());
        int split = rest.indexOf('_');
        int split2 = rest.indexOf('_', split + 1);
        long min = parseLong(rest.substring(0, split));

        if (split2 == -1) {
            long max = parseLong(rest.substring(split + 1));
            return new ParsedDelta(min, max, path, -1, deleteDelta, files);
        }

        long max = parseLong(rest.substring(split + 1, split2));
        int statementId = parseInt(rest.substring(split2 + 1));
        return new ParsedDelta(min, max, path, statementId, deleteDelta, files);
    }

    @VisibleForTesting
    static ParsedBase parseBase(String name)
    {
        checkArgument(name.startsWith("base_"), "File does not start with 'base_': %s", name);
        name = name.substring("base_".length());
        int index = name.indexOf("_v");
        if (index == -1) {
            return new ParsedBase(parseLong(name), 0);
        }
        return new ParsedBase(
                parseLong(name.substring(0, index)),
                parseLong(name.substring(index + 2)));
    }

    private static List<FileEntry> listFiles(TrinoFileSystem fileSystem, String directory)
            throws IOException
    {
        List<FileEntry> files = new ArrayList<>();
        FileIterator iterator = fileSystem.listFiles(directory);
        while (iterator.hasNext()) {
            FileEntry file = iterator.next();
            String name = new Path(file.path()).getName();
            if (!name.startsWith("_") && !name.startsWith(".")) {
                files.add(file);
            }
        }
        return files;
    }

    private static String listingSuffix(String directory, String file)
    {
        checkArgument(file.startsWith(directory), "file '%s' does not start with directory '%s'", file, directory);
        checkArgument((file.length() - directory.length()) >= 2, "file name is too short");
        checkArgument(file.charAt(directory.length()) == '/', "no slash after directory prefix");
        checkArgument(file.charAt(directory.length() + 1) != '/', "extra slash after directory prefix");
        return file.substring(directory.length() + 1);
    }

    public record AcidState(
            Optional<String> baseDirectory,
            List<FileEntry> baseFiles,
            List<ParsedDelta> deltas,
            List<FileEntry> originalFiles)
    {
        public AcidState
        {
            requireNonNull(baseDirectory, "baseDirectory is null");
            baseFiles = ImmutableList.copyOf(requireNonNull(baseFiles, "baseFiles is null"));
            deltas = ImmutableList.copyOf(requireNonNull(deltas, "deltas is null"));
            originalFiles = ImmutableList.copyOf(requireNonNull(originalFiles, "originalFiles is null"));
        }
    }

    public record ParsedBase(long writeId, long visibilityId) {}

    public record ParsedDelta(long min, long max, String path, int statementId, boolean deleteDelta, List<FileEntry> files)
            implements Comparable<ParsedDelta>
    {
        public ParsedDelta
        {
            requireNonNull(path, "path is null");
            files = ImmutableList.copyOf(requireNonNull(files, "files is null"));
        }

        @Override
        public int compareTo(ParsedDelta other)
        {
            return ComparisonChain.start()
                    .compare(min, other.min)
                    .compare(other.max, max)
                    .compare(statementId, other.statementId)
                    .compare(path, other.path)
                    .result();
        }
    }
}
