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
package io.trino.filesystem.memory;

import io.airlift.slice.Slice;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.memory.MemoryOutputFile.OutputBlob;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Iterator;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A blob file system for testing.
 */
public class MemoryFileSystem
        implements TrinoFileSystem
{
    private final ConcurrentMap<String, MemoryBlob> blobs = new ConcurrentHashMap<>();

    boolean isEmpty()
    {
        return blobs.isEmpty();
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        String key = toBlobKey(location);
        return new MemoryInputFile(location, () -> blobs.get(key), OptionalLong.empty());
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        String key = toBlobKey(location);
        return new MemoryInputFile(location, () -> blobs.get(key), OptionalLong.of(length));
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        String key = toBlobKey(location);
        OutputBlob outputBlob = new OutputBlob()
        {
            @Override
            public boolean exists()
            {
                return blobs.containsKey(key);
            }

            @Override
            public void createBlob(Slice data)
                    throws FileAlreadyExistsException
            {
                if (blobs.putIfAbsent(key, new MemoryBlob(data)) != null) {
                    throw new FileAlreadyExistsException(location.toString());
                }
            }

            @Override
            public void overwriteBlob(Slice data)
            {
                blobs.put(key, new MemoryBlob(data));
            }
        };
        return new MemoryOutputFile(location, outputBlob);
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        if (blobs.remove(toBlobKey(location)) == null) {
            throw new FileNotFoundException(location.toString());
        }
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        String prefix = toBlobPrefix(location);
        blobs.keySet().removeIf(path -> path.startsWith(prefix));
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        String sourceKey = toBlobKey(source);
        String targetKey = toBlobKey(target);

        // File rename is not atomic and that is ok
        MemoryBlob sourceData = blobs.get(sourceKey);
        if (sourceData == null) {
            throw new IOException("File rename from %s to %s failed: Source does not exist".formatted(source, target));
        }
        if (blobs.putIfAbsent(targetKey, sourceData) != null) {
            throw new IOException("File rename from %s to %s failed: Target already exists".formatted(source, target));
        }
        blobs.remove(sourceKey, sourceData);
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        String prefix = toBlobPrefix(location);
        Iterator<FileEntry> iterator = blobs.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .map(entry -> new FileEntry(
                        Location.of("memory:///" + entry.getKey()),
                        entry.getValue().data().length(),
                        entry.getValue().lastModified(),
                        Optional.empty()))
                .iterator();
        return new FileIterator()
        {
            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public FileEntry next()
            {
                return iterator.next();
            }
        };
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        validateMemoryLocation(location);
        if (location.path().isEmpty() || listFiles(location).hasNext()) {
            return Optional.of(true);
        }
        return Optional.empty();
    }

    @Override
    public void createDirectory(Location location)
            throws IOException
    {
        validateMemoryLocation(location);
        // memory file system does not have directories
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        throw new IOException("Memory file system does not support directory renames");
    }

    private static String toBlobKey(Location location)
    {
        validateMemoryLocation(location);
        location.verifyValidFileLocation();
        return location.path();
    }

    private static String toBlobPrefix(Location location)
    {
        validateMemoryLocation(location);
        String directoryPath = location.path();
        if (!directoryPath.isEmpty() && !directoryPath.endsWith("/")) {
            directoryPath += "/";
        }
        return directoryPath;
    }

    private static void validateMemoryLocation(Location location)
    {
        checkArgument(location.scheme().equals(Optional.of("memory")), "Only 'memory' scheme is supported: %s", location);
        checkArgument(location.userInfo().isEmpty(), "Memory location cannot contain user info: %s", location);
        checkArgument(location.host().isEmpty(), "Memory location cannot contain a host: %s", location);
    }
}
