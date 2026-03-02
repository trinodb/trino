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
package io.trino.plugin.deltalake.transactionlog.writer;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.plugin.deltalake.DeltaLakeFileSystemFactory;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.spi.connector.ConnectorSession;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class TestingLocalTransactionLogSynchronizer
        implements TransactionLogSynchronizer
{
    private final DeltaLakeFileSystemFactory fileSystemFactory;

    public TestingLocalTransactionLogSynchronizer(DeltaLakeFileSystemFactory fileSystemFactory)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public void write(ConnectorSession session, VendedCredentialsHandle credentialsHandle, String clusterId, Location newLogEntryPath, byte[] entryContents)
    {
        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(session, credentialsHandle);
            Path targetPath = ((LocalFileSystem) fileSystem).toFilePath(newLogEntryPath);
            Files.createDirectories(targetPath.getParent());
            Path tmpPath = targetPath.resolveSibling(".tmp.%s.%s".formatted(targetPath.getFileName().toString(), randomUUID()));
            Files.write(tmpPath, entryContents, CREATE_NEW);
            try {
                // It's important that file is renamed atomically (hence the ATOMIC_MOVE option), as
                // reads do not synchronize with the log writer in any way.
                // Files.move with ATOMIC_MOVE is not guaranteed to be exclusive.
                // In fact, it can be seen to overwrite the target file if it already exists.
                // In-memory synchronization should be sufficient for tests,
                // assuming single TestingDeltaLakePlugin instance is used.
                synchronized (this) {
                    if (Files.exists(targetPath)) {
                        throw new FileAlreadyExistsException(targetPath.toString());
                    }
                    Files.move(tmpPath, targetPath, ATOMIC_MOVE);
                }
            }
            catch (Throwable t) {
                closeAllSuppress(t, () -> Files.deleteIfExists(tmpPath));
                throw t;
            }
        }
        catch (FileAlreadyExistsException e) {
            throw new TransactionConflictException("Conflict detected while writing Transaction Log entry " + newLogEntryPath, e);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isUnsafe()
    {
        return false;
    }
}
