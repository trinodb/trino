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

import java.util.OptionalInt;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vector-descriptor-schema
public record DeletionVectorEntry(String storageType, String pathOrInlineDv, OptionalInt offset, int sizeInBytes, long cardinality)
{
    private static final int INSTANCE_SIZE = instanceSize(DeletionVectorEntry.class);

    public DeletionVectorEntry
    {
        requireNonNull(storageType, "storageType is null");
        requireNonNull(pathOrInlineDv, "pathOrInlineDv is null");
        requireNonNull(offset, "offset is null");
    }

    // https://github.com/delta-io/delta/blob/34f02d8/kernel/kernel-api/src/main/java/io/delta/kernel/internal/actions/DeletionVectorDescriptor.java#L167-L174
    public String uniqueId()
    {
        String uniqueFileId = storageType + pathOrInlineDv;
        if (offset.isPresent()) {
            return uniqueFileId + "@" + offset;
        }
        return uniqueFileId;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(storageType)
                + estimatedSizeOf(pathOrInlineDv)
                + sizeOf(offset)
                + SIZE_OF_INT
                + SIZE_OF_LONG;
    }
}
