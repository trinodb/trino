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

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
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
    public TrinoInputFile newInputFile(String location)
    {
        String key = toBlobKey(location);
        return new MemoryInputFile(location, () -> blobs.get(key), OptionalLong.empty());
    }

    @Override
    public TrinoInputFile newInputFile(String location, long length)
    {
        String key = toBlobKey(location);
        return new MemoryInputFile(location, () -> blobs.get(key), OptionalLong.of(length));
    }

    @Override
    public TrinoOutputFile newOutputFile(String location)
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
                    throw new FileAlreadyExistsException(location);
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
    public void deleteFile(String location)
            throws IOException
    {
        if (blobs.remove(toBlobKey(location)) == null) {
            throw new NoSuchFileException(location);
        }
    }

    @Override
    public void deleteDirectory(String location)
            throws IOException
    {
        String prefix = toBlobPrefix(location);
        blobs.keySet().removeIf(path -> path.startsWith(prefix));
    }

    @Override
    public void renameFile(String source, String target)
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
    public FileIterator listFiles(String location)
            throws IOException
    {
        String prefix = toBlobPrefix(location);
        Iterator<FileEntry> iterator = blobs.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .map(entry -> new FileEntry(
                        "memory:///" + entry.getKey(),
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

    private static String toBlobKey(String locationString)
    {
        Location location = parseMemoryLocation(locationString);
        location.verifyValidFileLocation();
        return location.path();
    }

    private static String toBlobPrefix(String location)
    {
        String directoryPath = parseMemoryLocation(location).path();
        if (!directoryPath.isEmpty() && !directoryPath.endsWith("/")) {
            directoryPath += "/";
        }
        return directoryPath;
    }

    private static Location parseMemoryLocation(String locationString)
    {
        Location location = Location.of(locationString);
        checkArgument(location.scheme().equals(Optional.of("memory")), "Only 'memory' scheme is supported: %s", locationString);
        checkArgument(location.userInfo().isEmpty(), "Memory location cannot contain user info: %s", locationString);
        checkArgument(location.host().isEmpty(), "Memory location cannot contain a host: %s", locationString);
        return location;
    }
}
