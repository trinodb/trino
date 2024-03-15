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
package io.trino.plugin.deltalake.kernel.engine;

import io.delta.kernel.engine.FileReadRequest;
import io.delta.kernel.engine.FileSystemClient;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static io.delta.kernel.internal.util.Utils.toCloseableIterator;
import static java.util.Objects.requireNonNull;

public class TrinoFileSystemClient
        implements FileSystemClient
{
    private final TrinoFileSystem fileSystem;

    public TrinoFileSystemClient(TrinoFileSystem fileSystem)
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
    }

    @Override
    public CloseableIterator<FileStatus> listFrom(String s)
            throws IOException
    {
        Location location = Location.of(s);

        String fileNamePrefix = location.fileName();
        Location parent = location.parentDirectory();

        FileIterator fileIterator = fileSystem.listFiles(parent);
        List<FileEntry> entries = new ArrayList<>();
        while (fileIterator.hasNext()) {
            FileEntry fileEntry = fileIterator.next();
            if (fileEntry.location().fileName().compareTo(fileNamePrefix) >= 0) {
                entries.add(fileEntry);
            }
        }

        entries.sort(Comparator.comparing(f -> f.location().fileName()));

        return toCloseableIterator(entries.iterator())
                .map(entry -> {
                    return FileStatus.of(
                            entry.location().toString(),
                            entry.length(),
                            entry.lastModified().toEpochMilli());
                });
    }

    @Override
    public String resolvePath(String s)
            throws IOException
    {
        return s; // the path is already resolved
    }

    @Override
    public CloseableIterator<ByteArrayInputStream> readFiles(CloseableIterator<FileReadRequest> closeableIterator)
            throws IOException
    {
        return closeableIterator.map(request -> {
            byte[] buff = new byte[request.getReadLength()];
            try (TrinoInput trinoInput = fileSystem.newInputFile(Location.of(request.getPath())).newInput()) {
                trinoInput.readFully(request.getStartOffset(), buff, 0, request.getReadLength());
                return new ByteArrayInputStream(buff);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public boolean mkdirs(String s)
            throws IOException
    {
        fileSystem.createDirectory(Location.of(s));
        return true;
    }
}
