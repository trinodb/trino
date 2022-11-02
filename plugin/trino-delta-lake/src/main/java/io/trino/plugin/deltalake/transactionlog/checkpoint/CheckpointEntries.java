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

import java.math.BigInteger;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public class CheckpointEntries
{
    private final MetadataEntry metadataEntry;
    private final ProtocolEntry protocolEntry;
    private final Set<TransactionEntry> transactionEntries;
    private final Set<AddFileEntry> addFileEntries;
    private final Set<RemoveFileEntry> removeFileEntries;

    CheckpointEntries(
            MetadataEntry metadataEntry,
            ProtocolEntry protocolEntry,
            Set<TransactionEntry> transactionEntries,
            Set<AddFileEntry> addFileEntries,
            Set<RemoveFileEntry> removeFileEntries)
    {
        this.metadataEntry = requireNonNull(metadataEntry, "metadataEntry is null");
        this.protocolEntry = requireNonNull(protocolEntry, "protocolEntry is null");
        this.transactionEntries = ImmutableSet.copyOf(transactionEntries);
        this.addFileEntries = ImmutableSet.copyOf(addFileEntries);
        this.removeFileEntries = ImmutableSet.copyOf(removeFileEntries);
    }

    public MetadataEntry getMetadataEntry()
    {
        return metadataEntry;
    }

    public ProtocolEntry getProtocolEntry()
    {
        return protocolEntry;
    }

    public Set<TransactionEntry> getTransactionEntries()
    {
        return transactionEntries;
    }

    public Set<AddFileEntry> getAddFileEntries()
    {
        return addFileEntries;
    }

    public Set<RemoveFileEntry> getRemoveFileEntries()
    {
        return removeFileEntries;
    }

    public BigInteger size()
    {
        // The additional 2 are for the MetadataEntry and ProtocolEntry
        return BigInteger.valueOf(transactionEntries.size() + addFileEntries.size() + removeFileEntries.size() + 2);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CheckpointEntries that = (CheckpointEntries) o;
        return Objects.equals(metadataEntry, that.metadataEntry)
                && Objects.equals(protocolEntry, that.protocolEntry)
                && Objects.equals(transactionEntries, that.transactionEntries)
                && Objects.equals(addFileEntries, that.addFileEntries)
                && Objects.equals(removeFileEntries, that.removeFileEntries);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(metadataEntry, protocolEntry, transactionEntries, addFileEntries, removeFileEntries);
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", CheckpointEntries.class.getSimpleName() + "[", "]")
                .add("metadataEntry=" + metadataEntry)
                .add("protocolEntry=" + protocolEntry)
                .add("transactionEntries=" + transactionEntries)
                .add("addFileEntries=" + addFileEntries)
                .add("removeFileEntries=" + removeFileEntries)
                .toString();
    }
}
