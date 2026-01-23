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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.FileMayHaveAlreadyExistedException;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.plugin.deltalake.DeltaLakeFileSystemFactory;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.spi.connector.ConnectorSession;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class S3ConditionalWriteLogSynchronizer
        implements TransactionLogSynchronizer
{
    private static final Logger log = Logger.get(S3ConditionalWriteLogSynchronizer.class);

    private final DeltaLakeFileSystemFactory fileSystemFactory;

    @Inject
    S3ConditionalWriteLogSynchronizer(DeltaLakeFileSystemFactory fileSystemFactory)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public void write(ConnectorSession session, VendedCredentialsHandle credentialsHandle, String clusterId, Location newLogEntryPath, byte[] entryContents)
    {
        // This is key to ensuring that two different queries/clusters can never write the same transaction log entry contents.
        // Contents-based checks are used to recover when S3 write succeeds on the server, but is retried on the client and the fails.
        String contentsAsString = new String(entryContents, UTF_8);
        checkArgument(contentsAsString.contains(session.getQueryId()), "Committed transaction log contents must contain the query ID: [%s]", contentsAsString);

        TrinoFileSystem fileSystem = fileSystemFactory.create(session, credentialsHandle);
        try {
            fileSystem.newOutputFile(newLogEntryPath).createExclusive(entryContents);
        }
        catch (FileAlreadyExistsException e) {
            throw new TransactionConflictException("Conflict detected while writing Transaction Log entry %s to S3".formatted(newLogEntryPath), e);
        }
        catch (FileMayHaveAlreadyExistedException mayExistException) {
            // Check if the object was actually created by a previous attempt of AWS SDK's implicit retries
            // This is workaround for AWS SDK limitation: https://github.com/aws/aws-sdk-java-v2/issues/6580
            try {
                try (TrinoInput trinoInput = fileSystem.newInputFile(newLogEntryPath).newInput()) {
                    Slice readBack = trinoInput.readFully(0, entryContents.length + 1);
                    // The contents are unique per query, so if they match, we know we created it
                    boolean createdByUs = readBack.equals(Slices.wrappedBuffer(entryContents));
                    if (createdByUs) {
                        // This should be a rare situation. Since we're masking a potential problem, log so that it surfaces if it becomes frequent.
                        log.warn(
                                """
                                Potential conflict falsely detected while writing Transaction Log entry %s to S3. \
                                This may happen in rare cases when network retries are involved. \
                                If this happens frequently, consider inspecting network infrastructure between Trino and S3.""",
                                newLogEntryPath);
                    }
                    else {
                        throw new TransactionConflictException("Conflict detected while writing Transaction Log entry %s to S3".formatted(newLogEntryPath), mayExistException);
                    }
                }
            }
            catch (IOException | RuntimeException verificationException) {
                TransactionConflictException conflictException = new TransactionConflictException(
                        "Potential conflict suspected while writing Transaction Log entry %s to S3. Conflicting object provenance could not be verified".formatted(newLogEntryPath),
                        verificationException);
                conflictException.addSuppressed(mayExistException);
                throw conflictException;
            }
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
