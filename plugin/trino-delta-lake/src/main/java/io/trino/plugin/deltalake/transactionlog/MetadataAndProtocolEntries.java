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

import java.util.Optional;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;

public record MetadataAndProtocolEntries(Optional<MetadataEntry> metadata, Optional<ProtocolEntry> protocol)
{
    private static final int INSTANCE_SIZE = instanceSize(MetadataAndProtocolEntries.class);

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + sizeOf(metadata, MetadataEntry::getRetainedSizeInBytes)
                + sizeOf(protocol, ProtocolEntry::getRetainedSizeInBytes);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Optional<MetadataEntry> metadataEntry = Optional.empty();
        private Optional<ProtocolEntry> protocolEntry = Optional.empty();

        public boolean hasMetadata()
        {
            return metadataEntry.isPresent();
        }

        public boolean hasProtocol()
        {
            return protocolEntry.isPresent();
        }

        public boolean isFull()
        {
            return hasMetadata() && hasProtocol();
        }

        public Builder withMetadataEntry(MetadataEntry metadataEntry)
        {
            this.metadataEntry = Optional.of(metadataEntry);
            return this;
        }

        public Builder withProtocolEntry(ProtocolEntry protocolEntry)
        {
            this.protocolEntry = Optional.of(protocolEntry);
            return this;
        }

        public Builder withEntries(MetadataAndProtocolEntries entries)
        {
            if (!hasMetadata() && entries.metadata().isPresent()) {
                withMetadataEntry(entries.metadata().get());
            }
            if (!hasProtocol() && entries.protocol().isPresent()) {
                withProtocolEntry(entries.protocol().get());
            }
            return this;
        }

        public Builder withTransactionLogEntry(DeltaLakeTransactionLogEntry transactionLogEntry)
        {
            if (metadataEntry.isEmpty() && transactionLogEntry.getMetaData() != null) {
                withMetadataEntry(transactionLogEntry.getMetaData());
            }
            if (protocolEntry.isEmpty() && transactionLogEntry.getProtocol() != null) {
                withProtocolEntry(transactionLogEntry.getProtocol());
            }
            return this;
        }

        public MetadataAndProtocolEntries build()
        {
            return new MetadataAndProtocolEntries(metadataEntry, protocolEntry);
        }
    }
}
