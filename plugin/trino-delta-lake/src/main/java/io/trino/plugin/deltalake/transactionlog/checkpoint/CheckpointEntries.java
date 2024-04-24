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
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.RemoveFileEntry;
import io.trino.plugin.deltalake.transactionlog.TransactionEntry;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public record CheckpointEntries(
        MetadataEntry metadataEntry,
        ProtocolEntry protocolEntry,
        Set<TransactionEntry> transactionEntries,
        Set<AddFileEntry> addFileEntries,
        Set<RemoveFileEntry> removeFileEntries)
{
    public CheckpointEntries
    {
        requireNonNull(metadataEntry, "metadataEntry is null");
        requireNonNull(protocolEntry, "protocolEntry is null");
        transactionEntries = ImmutableSet.copyOf(requireNonNull(transactionEntries, "transactionEntries is null"));
        addFileEntries = ImmutableSet.copyOf(requireNonNull(addFileEntries, "addFileEntries is null"));
        removeFileEntries = ImmutableSet.copyOf(requireNonNull(removeFileEntries, "removeFileEntries is null"));
    }

    public long size()
    {
        // The additional 2 are for the MetadataEntry and ProtocolEntry
        return transactionEntries.size() + addFileEntries.size() + removeFileEntries.size() + 2;
    }
}
