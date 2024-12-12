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

import io.airlift.slice.SizeOf;
import jakarta.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public record RemoveFileEntry(
        String path,
        @Nullable Map<String, String> partitionValues,
        long deletionTimestamp,
        boolean dataChange,
        Optional<DeletionVectorEntry> deletionVector)
{
    private static final int INSTANCE_SIZE = instanceSize(RemoveFileEntry.class);

    public RemoveFileEntry
    {
        requireNonNull(path, "path is null");
        requireNonNull(deletionVector, "deletionVector is null");
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(path)
                + estimatedSizeOf(partitionValues, SizeOf::estimatedSizeOf, SizeOf::estimatedSizeOf)
                + SIZE_OF_LONG
                + sizeOf(deletionVector, DeletionVectorEntry::getRetainedSizeInBytes);
    }
}
