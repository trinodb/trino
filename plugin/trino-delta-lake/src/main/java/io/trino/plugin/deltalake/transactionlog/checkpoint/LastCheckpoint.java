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

import io.airlift.slice.SizeOf;
import io.trino.plugin.deltalake.transactionlog.V2Checkpoint;

import java.util.Optional;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public record LastCheckpoint(long version, long size, Optional<Integer> parts, Optional<V2Checkpoint> v2Checkpoint)
{
    private static final int INSTANCE_SIZE = instanceSize(LastCheckpoint.class);

    public LastCheckpoint
    {
        requireNonNull(parts, "parts is null");
        requireNonNull(v2Checkpoint, "v2Checkpoint is null");
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + SIZE_OF_LONG
                + SIZE_OF_LONG
                + sizeOf(parts, SizeOf::sizeOf)
                + sizeOf(v2Checkpoint, V2Checkpoint::getRetainedSizeInBytes);
    }
}
