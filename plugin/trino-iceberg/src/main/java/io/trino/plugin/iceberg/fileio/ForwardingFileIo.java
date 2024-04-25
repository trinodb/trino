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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class ForwardingFileIo
        implements SupportsBulkOperations
{
    private static final int DELETE_BATCH_SIZE = 1000;
    private static final int BATCH_DELETE_PATHS_MESSAGE_LIMIT = 5;

    private final TrinoFileSystem fileSystem;
    private final Map<String, String> properties;

    public ForwardingFileIo(TrinoFileSystem fileSystem)
    {
        this(fileSystem, ImmutableMap.of());
    }

    public ForwardingFileIo(TrinoFileSystem fileSystem, Map<String, String> properties)
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
    }

    @Override
    public InputFile newInputFile(String path)
    {
        return new ForwardingInputFile(fileSystem.newInputFile(Location.of(path)));
    }

    @Override
    public InputFile newInputFile(String path, long length)
    {
        return new ForwardingInputFile(fileSystem.newInputFile(Location.of(path), length));
    }

    @Override
    public OutputFile newOutputFile(String path)
    {
        return new ForwardingOutputFile(fileSystem, Location.of(path));
    }

    @Override
    public void deleteFile(String path)
    {
        try {
            fileSystem.deleteFile(Location.of(path));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to delete file: " + path, e);
        }
    }

    @Override
    public void deleteFile(InputFile file)
    {
        SupportsBulkOperations.super.deleteFile(file);
    }

    @Override
    public void deleteFile(OutputFile file)
    {
        SupportsBulkOperations.super.deleteFile(file);
    }

    @Override
    public void deleteFiles(Iterable<String> pathsToDelete)
            throws BulkDeletionFailureException
    {
        Iterable<List<String>> partitions = Iterables.partition(pathsToDelete, DELETE_BATCH_SIZE);
        partitions.forEach(this::deleteBatch);
    }

    @Override
    public InputFile newInputFile(ManifestFile manifest)
    {
        return SupportsBulkOperations.super.newInputFile(manifest);
    }

    @Override
    public InputFile newInputFile(DataFile file)
    {
        return SupportsBulkOperations.super.newInputFile(file);
    }

    @Override
    public InputFile newInputFile(DeleteFile file)
    {
        return SupportsBulkOperations.super.newInputFile(file);
    }

    private void deleteBatch(List<String> filesToDelete)
    {
        try {
            fileSystem.deleteFiles(filesToDelete.stream().map(Location::of).toList());
        }
        catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to delete some or all of files: " +
                            Stream.concat(
                                            filesToDelete.stream()
                                                    .limit(BATCH_DELETE_PATHS_MESSAGE_LIMIT),
                                            filesToDelete.size() > BATCH_DELETE_PATHS_MESSAGE_LIMIT ? Stream.of("...") : Stream.of())
                                    .collect(joining(", ", "[", "]")),
                    e);
        }
    }

    @Override
    public Map<String, String> properties()
    {
        return properties;
    }

    @Override
    public void initialize(Map<String, String> properties)
    {
        throw new UnsupportedOperationException("ForwardingFileIO does not support initialization by properties");
    }

    @Override
    public void close() {}
}
