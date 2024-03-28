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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nullable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class DeltaLakeTransactionLogEntry
{
    private final TransactionEntry txn;
    private final AddFileEntry add;
    private final RemoveFileEntry remove;
    private final MetadataEntry metaData;
    private final ProtocolEntry protocol;
    private final CommitInfoEntry commitInfo;
    private final CdcEntry cdcEntry;
    private final SidecarEntry sidecar;
    private final CheckpointMetadataEntry checkpointMetadata;

    private DeltaLakeTransactionLogEntry(
            TransactionEntry txn,
            AddFileEntry add,
            RemoveFileEntry remove,
            MetadataEntry metaData,
            ProtocolEntry protocol,
            CommitInfoEntry commitInfo,
            CdcEntry cdcEntry,
            SidecarEntry sidecar,
            CheckpointMetadataEntry checkpointMetadata)
    {
        this.txn = txn;
        this.add = add;
        this.remove = remove;
        this.metaData = metaData;
        this.protocol = protocol;
        this.commitInfo = commitInfo;
        this.cdcEntry = cdcEntry;
        this.sidecar = sidecar;
        this.checkpointMetadata = checkpointMetadata;
    }

    @JsonCreator
    public static DeltaLakeTransactionLogEntry fromJson(
            @JsonProperty("txn") TransactionEntry txn,
            @JsonProperty("add") AddFileEntry add,
            @JsonProperty("remove") RemoveFileEntry remove,
            @JsonProperty("metaData") MetadataEntry metaData,
            @JsonProperty("protocol") ProtocolEntry protocol,
            @JsonProperty("commitInfo") CommitInfoEntry commitInfo,
            @JsonProperty("cdc") CdcEntry cdcEntry,
            @JsonProperty("sidecar") SidecarEntry sidecarEntry,
            @JsonProperty("checkpointMetadata") CheckpointMetadataEntry checkpointMetadata)
    {
        return new DeltaLakeTransactionLogEntry(txn, add, remove, metaData, protocol, commitInfo, cdcEntry, sidecarEntry, checkpointMetadata);
    }

    public static DeltaLakeTransactionLogEntry transactionEntry(TransactionEntry transaction)
    {
        requireNonNull(transaction, "transaction is null");
        return new DeltaLakeTransactionLogEntry(transaction, null, null, null, null, null, null, null, null);
    }

    public static DeltaLakeTransactionLogEntry commitInfoEntry(CommitInfoEntry commitInfo)
    {
        requireNonNull(commitInfo, "commitInfo is null");
        return new DeltaLakeTransactionLogEntry(null, null, null, null, null, commitInfo, null, null, null);
    }

    public static DeltaLakeTransactionLogEntry protocolEntry(ProtocolEntry protocolEntry)
    {
        requireNonNull(protocolEntry, "protocolEntry is null");
        return new DeltaLakeTransactionLogEntry(null, null, null, null, protocolEntry, null, null, null, null);
    }

    public static DeltaLakeTransactionLogEntry metadataEntry(MetadataEntry metadataEntry)
    {
        requireNonNull(metadataEntry, "metadataEntry is null");
        return new DeltaLakeTransactionLogEntry(null, null, null, metadataEntry, null, null, null, null, null);
    }

    public static DeltaLakeTransactionLogEntry addFileEntry(AddFileEntry addFileEntry)
    {
        requireNonNull(addFileEntry, "addFileEntry is null");
        return new DeltaLakeTransactionLogEntry(null, addFileEntry, null, null, null, null, null, null, null);
    }

    public static DeltaLakeTransactionLogEntry removeFileEntry(RemoveFileEntry removeFileEntry)
    {
        requireNonNull(removeFileEntry, "removeFileEntry is null");
        return new DeltaLakeTransactionLogEntry(null, null, removeFileEntry, null, null, null, null, null, null);
    }

    public static DeltaLakeTransactionLogEntry cdcEntry(CdcEntry cdcEntry)
    {
        requireNonNull(cdcEntry, "cdcEntry is null");
        return new DeltaLakeTransactionLogEntry(null, null, null, null, null, null, cdcEntry, null, null);
    }

    public static DeltaLakeTransactionLogEntry sidecarEntry(SidecarEntry sidecarEntry)
    {
        requireNonNull(sidecarEntry, "sidecarEntry is null");
        return new DeltaLakeTransactionLogEntry(null, null, null, null, null, null, null, sidecarEntry, null);
    }

    @Nullable
    @JsonProperty
    public TransactionEntry getTxn()
    {
        return txn;
    }

    @Nullable
    @JsonProperty
    public AddFileEntry getAdd()
    {
        return add;
    }

    @Nullable
    @JsonProperty
    public RemoveFileEntry getRemove()
    {
        return remove;
    }

    @Nullable
    @JsonProperty
    public MetadataEntry getMetaData()
    {
        return metaData;
    }

    @Nullable
    @JsonProperty
    public ProtocolEntry getProtocol()
    {
        return protocol;
    }

    @Nullable
    @JsonProperty
    public CommitInfoEntry getCommitInfo()
    {
        return commitInfo;
    }

    @Nullable
    @JsonProperty
    public CdcEntry getCDC()
    {
        return cdcEntry;
    }

    @Nullable
    @JsonProperty
    public SidecarEntry getSidecar()
    {
        return sidecar;
    }

    @Nullable
    @JsonProperty
    public CheckpointMetadataEntry getCheckpointMetadata()
    {
        return checkpointMetadata;
    }

    public DeltaLakeTransactionLogEntry withCommitInfo(CommitInfoEntry commitInfo)
    {
        return new DeltaLakeTransactionLogEntry(txn, add, remove, metaData, protocol, commitInfo, cdcEntry, sidecar, checkpointMetadata);
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
        DeltaLakeTransactionLogEntry that = (DeltaLakeTransactionLogEntry) o;
        return Objects.equals(txn, that.txn) &&
                Objects.equals(add, that.add) &&
                Objects.equals(remove, that.remove) &&
                Objects.equals(metaData, that.metaData) &&
                Objects.equals(protocol, that.protocol) &&
                Objects.equals(commitInfo, that.commitInfo) &&
                Objects.equals(cdcEntry, that.cdcEntry) &&
                Objects.equals(sidecar, that.sidecar) &&
                Objects.equals(checkpointMetadata, that.checkpointMetadata);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(txn, add, remove, metaData, protocol, commitInfo, cdcEntry, sidecar, checkpointMetadata);
    }

    @Override
    public String toString()
    {
        return String.format("DeltaLakeTransactionLogEntry{%s, %s, %s, %s, %s, %s, %s, %s, %s}", txn, add, remove, metaData, protocol, commitInfo, cdcEntry, sidecar, checkpointMetadata);
    }
}
