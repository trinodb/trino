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

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;

import static java.util.Objects.requireNonNull;

public class NoIsolationSynchronizer
        implements TransactionLogSynchronizer
{
    private final TrinoFileSystemFactory fileSystemFactory;

    @Inject
    public NoIsolationSynchronizer(TrinoFileSystemFactory fileSystemFactory)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public void write(ConnectorSession session, String clusterId, Path newLogEntryPath, byte[] entryContents)
            throws UncheckedIOException
    {
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        try {
            TrinoOutputFile outputFile = fileSystem.newOutputFile(newLogEntryPath.toString());
            try (OutputStream outputStream = outputFile.create()) {
                outputStream.write(entryContents);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isUnsafe()
    {
        return true;
    }
}
