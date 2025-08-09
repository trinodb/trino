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
package io.trino.plugin.hive.metastore.polaris;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Forwarding FileIO implementation that delegates to Trino's TrinoFileSystem
 * to avoid Hadoop dependencies in the Polaris metastore integration.
 */
public class ForwardingFileIo
        implements org.apache.iceberg.io.FileIO
{
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
