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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.deltalake.transactionlog.CommitInfoEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.TransactionLogEntries;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.canonicalizePartitionValues;
import static java.util.Objects.requireNonNull;

public class DeltaLakeCommitSummary
{
    private final long version;
    private final List<MetadataEntry> metadataUpdates;
    private final Optional<ProtocolEntry> protocol;
    private final boolean containsRemoveFileWithoutPartitionValues;
    private final Set<Map<String, Optional<String>>> removedFilesCanonicalPartitionValues;
    private final Set<Map<String, Optional<String>>> addedFilesCanonicalPartitionValues;
    private final Optional<Boolean> isBlindAppend;

    public DeltaLakeCommitSummary(long version, TransactionLogEntries transactionLogEntries, TrinoFileSystem fileSystem)
    {
        requireNonNull(transactionLogEntries, "transactionLogEntries is null");
        ImmutableList.Builder<MetadataEntry> metadataUpdatesBuilder = ImmutableList.builder();
        Optional<ProtocolEntry> optionalProtocol = Optional.empty();
        Optional<CommitInfoEntry> optionalCommitInfo = Optional.empty();
        ImmutableSet.Builder<Map<String, Optional<String>>> addedFilesCanonicalPartitionValuesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<Map<String, Optional<String>>> removedFilesCanonicalPartitionValuesBuilder = ImmutableSet.builder();
        boolean containsRemoveFileWithoutPartitionValues = false;

        try (Stream<DeltaLakeTransactionLogEntry> logEntryStream = transactionLogEntries.getEntries(fileSystem)) {
            for (Iterator<DeltaLakeTransactionLogEntry> it = logEntryStream.iterator(); it.hasNext(); ) {
                DeltaLakeTransactionLogEntry transactionLogEntry = it.next();
                if (transactionLogEntry.getMetaData() != null) {
                    metadataUpdatesBuilder.add(transactionLogEntry.getMetaData());
                }
                else if (transactionLogEntry.getProtocol() != null) {
                    optionalProtocol = Optional.of(transactionLogEntry.getProtocol());
                }
                else if (transactionLogEntry.getCommitInfo() != null) {
                    optionalCommitInfo = Optional.of(transactionLogEntry.getCommitInfo());
                }
                else if (transactionLogEntry.getAdd() != null) {
                    addedFilesCanonicalPartitionValuesBuilder.add(transactionLogEntry.getAdd().getCanonicalPartitionValues());
                }
                else if (transactionLogEntry.getRemove() != null) {
                    Map<String, String> partitionValues = transactionLogEntry.getRemove().partitionValues();
                    if (partitionValues == null) {
                        containsRemoveFileWithoutPartitionValues = true;
                    }
                    else {
                        removedFilesCanonicalPartitionValuesBuilder.add(canonicalizePartitionValues(partitionValues));
                    }
                }
            }
        }

        this.version = version;
        metadataUpdates = metadataUpdatesBuilder.build();
        protocol = optionalProtocol;
        addedFilesCanonicalPartitionValues = addedFilesCanonicalPartitionValuesBuilder.build();
        removedFilesCanonicalPartitionValues = removedFilesCanonicalPartitionValuesBuilder.build();
        this.containsRemoveFileWithoutPartitionValues = containsRemoveFileWithoutPartitionValues;
        isBlindAppend = optionalCommitInfo.flatMap(CommitInfoEntry::isBlindAppend);
    }

    public long getVersion()
    {
        return version;
    }

    public List<MetadataEntry> getMetadataUpdates()
    {
        return metadataUpdates;
    }

    public Optional<ProtocolEntry> getProtocol()
    {
        return protocol;
    }

    public boolean isContainsRemoveFileWithoutPartitionValues()
    {
        return containsRemoveFileWithoutPartitionValues;
    }

    public Set<Map<String, Optional<String>>> getRemovedFilesCanonicalPartitionValues()
    {
        return removedFilesCanonicalPartitionValues;
    }

    public Set<Map<String, Optional<String>>> getAddedFilesCanonicalPartitionValues()
    {
        return addedFilesCanonicalPartitionValues;
    }

    public Optional<Boolean> getIsBlindAppend()
    {
        return isBlindAppend;
    }
}
