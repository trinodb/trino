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

import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;

import java.util.Optional;
import java.util.stream.Stream;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;

public record MetadataAndProtocolEntries(Optional<MetadataEntry> metadata, Optional<ProtocolEntry> protocol)
{
    private static final int INSTANCE_SIZE = instanceSize(MetadataAndProtocolEntries.class);

    public MetadataAndProtocolEntries(MetadataEntry metadata, ProtocolEntry protocol)
    {
        this(Optional.ofNullable(metadata), Optional.ofNullable(protocol));
    }

    public Stream<Object> stream()
    {
        if (metadata.isPresent() && protocol.isPresent()) {
            return Stream.of(metadata.get(), protocol.get());
        }
        if (metadata.isPresent()) {
            return Stream.of(metadata.get());
        }
        if (protocol.isPresent()) {
            return Stream.of(protocol.get());
        }
        return Stream.of();
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + sizeOf(metadata, MetadataEntry::getRetainedSizeInBytes)
                + sizeOf(protocol, ProtocolEntry::getRetainedSizeInBytes);
    }
}
