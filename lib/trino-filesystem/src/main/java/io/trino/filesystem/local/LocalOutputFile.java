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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static io.trino.filesystem.local.LocalUtils.handleException;
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
            OutputStream stream = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            return new LocalOutputStream(location, stream);
        }
        catch (IOException e) {
            throw handleException(location, e);
        }
    }

    /**
     * Writes to a uniquely-named staging file first, then publishes it with a hard link — the
     * standard Unix idiom for exclusive-create-with-content (as used by e.g. Maildir delivery),
     * chosen because a plain {@code open(2) O_CREAT|O_EXCL}-then-write would let a concurrent
     * reader observe the file before it is fully written. {@link Files#createLink} fails with
     * {@link java.nio.file.FileAlreadyExistsException} if the target already exists and is
     * atomic, so readers only ever see either no file or the fully-written one.
     * <p>
     * This relies on the underlying mount correctly implementing hard links and atomic link
     * creation, which holds for local disks and NFS. Administrators using NFSv3 mounts must
     * ensure their client/server correctly implement the exclusive-create verifier (RFC 1813
     * §3.3.8) for {@code link(2)} — modern Linux NFS clients handle this transparently, but this
     * is an environment concern that Trino cannot detect or control.
     */
    @Override
    public void createExclusive(byte[] data)
            throws IOException
    {
        Path parent = path.getParent();
        Path staging = parent.resolve(path.getFileName() + "." + randomUUID() + ".tmp");
        try {
            Files.createDirectories(parent);
            Files.write(staging, data, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            Files.createLink(path, staging);
        }
        catch (IOException e) {
            throw handleException(location, e);
        }
        finally {
            try {
                Files.deleteIfExists(staging);
            }
            catch (IOException _) {
                // best-effort cleanup of the staging file; the publish itself already succeeded or failed
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
