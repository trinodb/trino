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

import static com.google.common.base.MoreObjects.toStringHelper;

// Ref. https://github.com/delta-io/delta/blob/master/PROTOCOL.md#version-checksum-file
public class DeltaLakeVersionChecksum
{
    private final Long tableSizeBytes;
    private final Long numFiles;
    private final Long numMetadata;
    private final Long numProtocol;
    private final MetadataEntry metadata;
    private final ProtocolEntry protocol;

    @JsonCreator
    public DeltaLakeVersionChecksum(
            @JsonProperty("tableSizeBytes") @Nullable Long tableSizeBytes,
            @JsonProperty("numFiles") @Nullable Long numFiles,
            @JsonProperty("numMetadata") @Nullable Long numMetadata,
            @JsonProperty("numProtocol") @Nullable Long numProtocol,
            @JsonProperty("metadata") @Nullable MetadataEntry metadata,
            @JsonProperty("protocol") @Nullable ProtocolEntry protocol)
    {
        this.tableSizeBytes = tableSizeBytes;
        this.numFiles = numFiles;
        this.numMetadata = numMetadata;
        this.numProtocol = numProtocol;
        this.metadata = metadata;
        this.protocol = protocol;
    }

    @JsonProperty
    @Nullable
    public Long getTableSizeBytes()
    {
        return tableSizeBytes;
    }

    @JsonProperty
    @Nullable
    public Long getNumFiles()
    {
        return numFiles;
    }

    @JsonProperty
    @Nullable
    public Long getNumMetadata()
    {
        return numMetadata;
    }

    @JsonProperty
    @Nullable
    public Long getNumProtocol()
    {
        return numProtocol;
    }

    @Nullable
    @JsonProperty
    public MetadataEntry getMetadata()
    {
        return metadata;
    }

    @Nullable
    @JsonProperty
    public ProtocolEntry getProtocol()
    {
        return protocol;
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
        DeltaLakeVersionChecksum that = (DeltaLakeVersionChecksum) o;
        return Objects.equals(tableSizeBytes, that.tableSizeBytes) &&
                Objects.equals(numFiles, that.numFiles) &&
                Objects.equals(numMetadata, that.numMetadata) &&
                Objects.equals(numProtocol, that.numProtocol) &&
                Objects.equals(metadata, that.metadata) &&
                Objects.equals(protocol, that.protocol);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableSizeBytes, numFiles, numMetadata, numProtocol, metadata, protocol);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableSizeBytes", tableSizeBytes)
                .add("numFiles", numFiles)
                .add("numMetadata", numMetadata)
                .add("numProtocol", numProtocol)
                .add("metadata", metadata)
                .add("protocol", protocol)
                .toString();
    }
}
