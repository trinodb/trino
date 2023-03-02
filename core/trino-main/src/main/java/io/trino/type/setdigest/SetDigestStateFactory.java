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

package io.trino.type.setdigest;

import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;

import static io.airlift.slice.SizeOf.instanceSize;

public class SetDigestStateFactory
        implements AccumulatorStateFactory<SetDigestState>
{
    private static final int SIZE_OF_SINGLE = instanceSize(SingleSetDigestState.class);
    private static final int SIZE_OF_GROUPED = instanceSize(GroupedSetDigestState.class);

    @Override
    public SetDigestState createSingleState()
    {
        return new SingleSetDigestState();
    }

    @Override
    public SetDigestState createGroupedState()
    {
        return new GroupedSetDigestState();
    }

    public static class GroupedSetDigestState
            implements GroupedAccumulatorState, SetDigestState
    {
        private final ObjectBigArray<SetDigest> digests = new ObjectBigArray<>();
        private long groupId;
        private long size;

        @Override
        public void setGroupId(long groupId)
        {
            this.groupId = groupId;
        }

        @Override
        public void ensureCapacity(long size)
        {
            digests.ensureCapacity(size);
        }

        @Override
        public SetDigest getDigest()
        {
            return digests.get(groupId);
        }

        @Override
        public void setDigest(SetDigest value)
        {
            if (getDigest() != null) {
                size -= getDigest().estimatedInMemorySize();
            }
            size += value.estimatedInMemorySize();
            digests.set(groupId, value);
        }

        @Override
        public long getEstimatedSize()
        {
            return SIZE_OF_GROUPED + size + digests.sizeOf();
        }
    }

    public static class SingleSetDigestState
            implements SetDigestState
    {
        private SetDigest digest;

        @Override
        public SetDigest getDigest()
        {
            return digest;
        }

        @Override
        public void setDigest(SetDigest value)
        {
            this.digest = value;
        }

        @Override
        public long getEstimatedSize()
        {
            if (digest == null) {
                return SIZE_OF_SINGLE;
            }
            return SIZE_OF_SINGLE + digest.estimatedInMemorySize();
        }
    }
}
