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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.ObjectMapperProvider;
import io.trino.filesystem.Location;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.CdcEntry;
import io.trino.plugin.deltalake.transactionlog.CommitInfoEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.RemoveFileEntry;
import io.trino.spi.connector.ConnectorSession;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogJsonEntryPath;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.Preconditions.checkState;

public class FileSystemTransactionLogWriter
        implements TransactionLogWriter
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    private Optional<DeltaLakeTransactionLogEntry> commitInfoEntry = Optional.empty();
    private final List<DeltaLakeTransactionLogEntry> entries = new ArrayList<>();
    private final TransactionLogSynchronizer logSynchronizer;
    private final ConnectorSession session;
    private final String tableLocation;
    private final VendedCredentialsHandle credentialsHandle;

    public FileSystemTransactionLogWriter(ConnectorSession session, TransactionLogSynchronizer logSynchronizer, String tableLocation, VendedCredentialsHandle credentialsHandle)
    {
        this.logSynchronizer = requireNonNull(logSynchronizer, "logSynchronizer is null");
        this.session = requireNonNull(session, "session is null");
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
        this.credentialsHandle = requireNonNull(credentialsHandle, "credentialsHandle is null");
    }

    @Override
    public void appendCommitInfoEntry(CommitInfoEntry commitInfoEntry)
    {
        checkState(this.commitInfoEntry.isEmpty(), "commitInfo already set");
        this.commitInfoEntry = Optional.of(DeltaLakeTransactionLogEntry.commitInfoEntry(commitInfoEntry));
    }

    @Override
    public void appendMetadataEntry(MetadataEntry metadataEntry)
    {
        entries.add(DeltaLakeTransactionLogEntry.metadataEntry(metadataEntry));
    }

    @Override
    public void appendProtocolEntry(ProtocolEntry protocolEntry)
    {
        entries.add(DeltaLakeTransactionLogEntry.protocolEntry(protocolEntry));
    }

    @Override
    public void appendAddFileEntry(AddFileEntry addFileEntry)
    {
        entries.add(DeltaLakeTransactionLogEntry.addFileEntry(addFileEntry));
    }

    @Override
    public void appendRemoveFileEntry(RemoveFileEntry removeFileEntry)
    {
        entries.add(DeltaLakeTransactionLogEntry.removeFileEntry(removeFileEntry));
    }

    @Override
    public void appendCdcEntry(CdcEntry cdcEntry)
    {
        entries.add(DeltaLakeTransactionLogEntry.cdcEntry(cdcEntry));
    }

    @Override
    public boolean isUnsafe()
    {
        return logSynchronizer.isUnsafe();
    }

    @Override
    public void flush()
            throws IOException
    {
        checkState(commitInfoEntry.isPresent(), "commitInfo not set");

        String transactionLogLocation = getTransactionLogDir(tableLocation);
        CommitInfoEntry commitInfo = requireNonNull(commitInfoEntry.get().getCommitInfo(), "commitInfoEntry.get().getCommitInfo() is null");
        Location logEntry = getTransactionLogJsonEntryPath(transactionLogLocation, commitInfo.version());
        writeLog(logEntry);
    }

    public void writeLog(Location logEntry)
            throws IOException
    {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            writeEntry(bos, commitInfoEntry.get());
            for (DeltaLakeTransactionLogEntry entry : entries) {
                writeEntry(bos, entry);
            }

            String clusterId = commitInfoEntry.get().getCommitInfo().clusterId();
            logSynchronizer.write(session, credentialsHandle, clusterId, logEntry, bos.toByteArray());
        }
    }

    private static void writeEntry(OutputStream outputStream, DeltaLakeTransactionLogEntry deltaLakeTransactionLogEntry)
            throws IOException
    {
        outputStream.write(OBJECT_MAPPER.writeValueAsString(deltaLakeTransactionLogEntry).getBytes(UTF_8));
        outputStream.write("\n".getBytes(UTF_8));
    }
}
