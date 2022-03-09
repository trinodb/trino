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
package io.trino.plugin.deltalake.transactionlog.checkpoint;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.RemoveFileEntry;
import io.trino.plugin.deltalake.transactionlog.TransactionEntry;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public class CheckpointBuilder
{
    private MetadataEntry metadataEntry;
    private ProtocolEntry protocolEntry;
    private final Map<String, TransactionEntry> transactionEntries = new HashMap<>();
    private final Map<String, AddFileEntry> addFileEntries = new HashMap<>();
    private final Map<String, RemoveFileEntry> removeFileEntries = new HashMap<>();

    public void addLogEntry(DeltaLakeTransactionLogEntry logEntry)
    {
        if (logEntry.getMetaData() != null) {
            // TODO: validate if schema in this entry and previous one did not diverge.
            metadataEntry = logEntry.getMetaData();
        }
        if (logEntry.getProtocol() != null) {
            protocolEntry = logEntry.getProtocol();
        }
        handleTransactionEntry(logEntry.getTxn());
        handleAddFileEntry(logEntry.getAdd());
        handleRemoveFileEntry(logEntry.getRemove());
    }

    private void handleTransactionEntry(@Nullable TransactionEntry entry)
    {
        if (entry == null) {
            return;
        }
        TransactionEntry currentEntry = transactionEntries.get(entry.getAppId());
        // TODO: Investigate if logic is ok.
        // It is not super obvious from documentation if we should compare version to find a winner
        // Or should just latest entry for given appId win.
        if (currentEntry != null && currentEntry.getVersion() > entry.getVersion()) {
            return;
        }
        transactionEntries.put(entry.getAppId(), entry);
    }

    private void handleAddFileEntry(@Nullable AddFileEntry entry)
    {
        if (entry == null) {
            return;
        }
        addFileEntries.put(entry.getPath(), entry);
        removeFileEntries.remove(entry.getPath());
    }

    private void handleRemoveFileEntry(@Nullable RemoveFileEntry entry)
    {
        if (entry == null) {
            return;
        }
        removeFileEntries.put(entry.getPath(), entry);
        addFileEntries.remove(entry.getPath());
    }

    public CheckpointEntries build()
    {
        checkState(metadataEntry != null, "Metadata entry not registered");
        checkState(protocolEntry != null, "Protocol entry not registered");
        return new CheckpointEntries(
                metadataEntry,
                protocolEntry,
                ImmutableSet.copyOf(transactionEntries.values()),
                ImmutableSet.copyOf(addFileEntries.values()),
                ImmutableSet.copyOf(removeFileEntries.values()));
    }
}
