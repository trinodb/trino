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
package io.trino.operator.aggregation.state;

import io.airlift.stats.TDigest;
import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class TDigestStateFactory
        implements AccumulatorStateFactory<TDigestState>
{
    @Override
    public TDigestState createSingleState()
    {
        return new SingleTDigestState();
    }

    @Override
    public TDigestState createGroupedState()
    {
        return new GroupedTDigestState();
    }

    public static class GroupedTDigestState
            extends AbstractGroupedAccumulatorState
            implements TDigestState
    {
        private static final int INSTANCE_SIZE = instanceSize(GroupedTDigestState.class);
        private final ObjectBigArray<TDigest> digests = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            digests.ensureCapacity(size);
        }

        @Override
        public TDigest getTDigest()
        {
            return digests.get(getGroupId());
        }

        @Override
        public void setTDigest(TDigest value)
        {
            requireNonNull(value, "value is null");
            digests.set(getGroupId(), value);
        }

        @Override
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + digests.sizeOf();
        }
    }

    public static class SingleTDigestState
            implements TDigestState
    {
        private static final int INSTANCE_SIZE = instanceSize(SingleTDigestState.class);
        private TDigest digest;

        @Override
        public TDigest getTDigest()
        {
            return digest;
        }

        @Override
        public void setTDigest(TDigest value)
        {
            digest = value;
        }

        @Override
        public void addMemoryUsage(int value)
        {
            // noop
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (digest != null) {
                estimatedSize += digest.estimatedInMemorySizeInBytes();
            }
            return estimatedSize;
        }
    }
}
