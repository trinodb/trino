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
package io.trino.plugin.deltalake.transactionlog;

import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.deltalake.transactionlog.checkpoint.LastCheckpoint;
import io.trino.spi.TrinoException;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_FILESYSTEM_ERROR;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.readLastCheckpoint;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogJsonEntryPath;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail.getEntriesFromJson;
import static java.util.Objects.requireNonNull;

public class DeltaLakeTransactionLogIterator
        implements Iterator<List<DeltaLakeTransactionLogEntry>>
{
    private final TrinoFileSystem fileSystem;
    private final Path transactionLogDir;
    private final Optional<Long> endVersion;
    private final Optional<Long> lastCheckpointVersion;

    private long version;
    private Optional<List<DeltaLakeTransactionLogEntry>> entries = Optional.empty();
    private boolean stopped;

    public DeltaLakeTransactionLogIterator(
            TrinoFileSystem fileSystem,
            Path tableLocation,
            Optional<Long> startVersion,
            Optional<Long> endVersion)
    {
        verify(startVersion.orElse(0L) <= endVersion.orElse(startVersion.orElse(0L)), "startVersion must be less or equal than endVersion");
        version = startVersion.orElse(0L) - 1;
        this.endVersion = endVersion;
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        transactionLogDir = getTransactionLogDir(requireNonNull(tableLocation, "tableLocation is null"));
        lastCheckpointVersion = readLastCheckpoint(fileSystem, tableLocation).map(LastCheckpoint::getVersion);
    }

    @Override
    public boolean hasNext()
    {
        if (entries.isPresent()) {
            return true;
        }
        entries = retrieveNextEntries();
        return entries.isPresent();
    }

    @Override
    public List<DeltaLakeTransactionLogEntry> next()
    {
        if (entries.isPresent()) {
            List<DeltaLakeTransactionLogEntry> next = entries.get();
            entries = Optional.empty();
            return next;
        }

        return retrieveNextEntries().orElseThrow(NoSuchElementException::new);
    }

    private Optional<List<DeltaLakeTransactionLogEntry>> retrieveNextEntries()
    {
        if (stopped) {
            return Optional.empty();
        }
        if (endVersion.isPresent() && version == endVersion.get()) {
            stopped = true;
            return Optional.empty();
        }
        while (true) {
            version++;
            Optional<List<DeltaLakeTransactionLogEntry>> nextEntries;
            try {
                nextEntries = getEntriesFromJson(version, transactionLogDir, fileSystem);
            }
            catch (IOException e) {
                throw new TrinoException(DELTA_LAKE_FILESYSTEM_ERROR, "Failed accessing transaction log " + getTransactionLogJsonEntryPath(transactionLogDir, version), e);
            }
            if (nextEntries.isPresent()) {
                return nextEntries;
            }
            // The outdated transaction log file may have been removed after adding a subsequent checkpoint
            if (lastCheckpointVersion.isPresent() && version < lastCheckpointVersion.get()) {
                if (endVersion.isPresent() && version == endVersion.get()) {
                    return Optional.empty();
                }
                continue;
            }
            stopped = true;
            return Optional.empty();
        }
    }
}
