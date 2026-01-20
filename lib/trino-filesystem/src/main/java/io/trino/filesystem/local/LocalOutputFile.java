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
package io.trino.filesystem.local;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.memory.context.AggregatedMemoryContext;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.trino.filesystem.local.LocalUtils.handleException;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class LocalOutputFile
        implements TrinoOutputFile
{
    private final Location location;
    private final Path path;

    public LocalOutputFile(Location location, Path path)
    {
        this.location = requireNonNull(location, "location is null");
        this.path = requireNonNull(path, "path is null");
    }

    public LocalOutputFile(File file)
    {
        this(Location.of(file.toURI().toString()), file.toPath());
    }

    @Override
    public OutputStream create(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        try {
            Files.createDirectories(path.getParent());
            OutputStream stream = Files.newOutputStream(path, CREATE_NEW, WRITE);
            return new LocalOutputStream(location, stream);
        }
        catch (IOException e) {
            throw handleException(location, e);
        }
    }

    @Override
    public void createExclusive(byte[] data)
            throws IOException
    {
        Files.createDirectories(path.getParent());

        // see if we can stop early without acquire locking
        if (Files.exists(path)) {
            throw new FileAlreadyExistsException(location.toString());
        }

        boolean lockCreated = false;
        Path lockPath = path.resolveSibling(path.getFileName() + ".lock");
        try (FileChannel channel = FileChannel.open(lockPath, CREATE_NEW, WRITE);
                FileLock lock = channel.lock()) {
            lockCreated = true;
            if (Files.exists(path)) {
                throw new FileAlreadyExistsException(location.toString());
            }

            Path tmpFilePath = path.resolveSibling(path.getFileName() + "." + randomUUID() + ".tmp");
            try (OutputStream out = Files.newOutputStream(tmpFilePath, CREATE_NEW, WRITE)) {
                out.write(data);
            }

            // Use atomic move to ensure that the file is only visible when fully written
            Files.move(tmpFilePath, path, ATOMIC_MOVE);
        }
        catch (IOException e) {
            throw handleException(location, e);
        }
        finally {
            if (lockCreated) {
                try {
                    Files.deleteIfExists(lockPath);
                }
                catch (IOException lockDeleteException) {
                    throw handleException(location, lockDeleteException);
                }
            }
        }
    }

    @Override
    public void createOrOverwrite(byte[] data)
            throws IOException
    {
        try {
            Files.createDirectories(path.getParent());
            OutputStream stream = Files.newOutputStream(path);
            try (OutputStream out = new LocalOutputStream(location, stream)) {
                out.write(data);
            }
        }
        catch (IOException e) {
            throw handleException(location, e);
        }
    }

    @Override
    public Location location()
    {
        return location;
    }

    @Override
    public String toString()
    {
        return location.toString();
    }
}
