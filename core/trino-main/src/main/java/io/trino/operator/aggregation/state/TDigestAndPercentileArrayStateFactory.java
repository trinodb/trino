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

import io.airlift.slice.SizeOf;
import io.airlift.stats.TDigest;
import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;

import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class TDigestAndPercentileArrayStateFactory
        implements AccumulatorStateFactory<TDigestAndPercentileArrayState>
{
    @Override
    public TDigestAndPercentileArrayState createSingleState()
    {
        return new SingleTDigestAndPercentileArrayState();
    }

    @Override
    public TDigestAndPercentileArrayState createGroupedState()
    {
        return new GroupedTDigestAndPercentileArrayState();
    }

    public static class GroupedTDigestAndPercentileArrayState
            extends AbstractGroupedAccumulatorState
            implements TDigestAndPercentileArrayState
    {
        private static final int INSTANCE_SIZE = instanceSize(GroupedTDigestAndPercentileArrayState.class);
        private final ObjectBigArray<TDigest> digests = new ObjectBigArray<>();
        private final ObjectBigArray<List<Double>> percentilesArray = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            digests.ensureCapacity(size);
            percentilesArray.ensureCapacity(size);
        }

        @Override
        public TDigest getDigest()
        {
            return digests.get(getGroupId());
        }

        @Override
        public void setDigest(TDigest digest)
        {
            digests.set(getGroupId(), requireNonNull(digest, "digest is null"));
        }

        @Override
        public List<Double> getPercentiles()
        {
            return percentilesArray.get(getGroupId());
        }

        @Override
        public void setPercentiles(List<Double> percentiles)
        {
            percentilesArray.set(getGroupId(), requireNonNull(percentiles, "percentiles is null"));
        }

        @Override
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + digests.sizeOf() + percentilesArray.sizeOf();
        }
    }

    public static class SingleTDigestAndPercentileArrayState
            implements TDigestAndPercentileArrayState
    {
        public static final int INSTANCE_SIZE = instanceSize(SingleTDigestAndPercentileArrayState.class);
        private TDigest digest;
        private List<Double> percentiles;

        @Override
        public TDigest getDigest()
        {
            return digest;
        }

        @Override
        public void setDigest(TDigest digest)
        {
            this.digest = requireNonNull(digest, "digest is null");
        }

        @Override
        public List<Double> getPercentiles()
        {
            return percentiles;
        }

        @Override
        public void setPercentiles(List<Double> percentiles)
        {
            this.percentiles = requireNonNull(percentiles, "percentiles is null");
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
            if (percentiles != null) {
                estimatedSize += SizeOf.sizeOfDoubleArray(percentiles.size());
            }
            return estimatedSize;
        }
    }
}
