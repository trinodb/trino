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

import io.airlift.stats.QuantileDigest;
import io.trino.array.DoubleBigArray;
import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class QuantileDigestAndPercentileStateFactory
        implements AccumulatorStateFactory<QuantileDigestAndPercentileState>
{
    @Override
    public QuantileDigestAndPercentileState createSingleState()
    {
        return new SingleQuantileDigestAndPercentileState();
    }

    @Override
    public QuantileDigestAndPercentileState createGroupedState()
    {
        return new GroupedQuantileDigestAndPercentileState();
    }

    public static class GroupedQuantileDigestAndPercentileState
            extends AbstractGroupedAccumulatorState
            implements QuantileDigestAndPercentileState
    {
        private static final int INSTANCE_SIZE = instanceSize(GroupedQuantileDigestAndPercentileState.class);
        private final ObjectBigArray<QuantileDigest> digests = new ObjectBigArray<>();
        private final DoubleBigArray percentiles = new DoubleBigArray();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            digests.ensureCapacity(size);
            percentiles.ensureCapacity(size);
        }

        @Override
        public QuantileDigest getDigest()
        {
            return digests.get(getGroupId());
        }

        @Override
        public void setDigest(QuantileDigest digest)
        {
            requireNonNull(digest, "digest is null");
            digests.set(getGroupId(), digest);
        }

        @Override
        public double getPercentile()
        {
            return percentiles.get(getGroupId());
        }

        @Override
        public void setPercentile(double percentile)
        {
            percentiles.set(getGroupId(), percentile);
        }

        @Override
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + digests.sizeOf() + percentiles.sizeOf();
        }
    }

    public static class SingleQuantileDigestAndPercentileState
            implements QuantileDigestAndPercentileState
    {
        public static final int INSTANCE_SIZE = instanceSize(SingleQuantileDigestAndPercentileState.class);
        private QuantileDigest digest;
        private double percentile;

        @Override
        public QuantileDigest getDigest()
        {
            return digest;
        }

        @Override
        public void setDigest(QuantileDigest digest)
        {
            this.digest = digest;
        }

        @Override
        public double getPercentile()
        {
            return percentile;
        }

        @Override
        public void setPercentile(double percentile)
        {
            this.percentile = percentile;
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
