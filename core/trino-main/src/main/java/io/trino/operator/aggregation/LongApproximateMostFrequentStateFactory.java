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
package io.trino.operator.aggregation;

import io.trino.array.ObjectBigArray;
import io.trino.operator.aggregation.state.AbstractGroupedAccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;

public class LongApproximateMostFrequentStateFactory
        implements AccumulatorStateFactory<BigintApproximateMostFrequent.State>
{
    @Override
    public BigintApproximateMostFrequent.State createSingleState()
    {
        return new SingleLongApproximateMostFrequentState();
    }

    @Override
    public BigintApproximateMostFrequent.State createGroupedState()
    {
        return new GroupedLongApproximateMostFrequentState();
    }

    public static class SingleLongApproximateMostFrequentState
            implements BigintApproximateMostFrequent.State
    {
        private ApproximateMostFrequentHistogram<Long> histogram;
        private long size;

        @Override
        public ApproximateMostFrequentHistogram<Long> get()
        {
            return histogram;
        }

        @Override
        public void set(ApproximateMostFrequentHistogram<Long> histogram)
        {
            this.histogram = histogram;
            size = histogram.estimatedInMemorySize();
        }

        @Override
        public long getEstimatedSize()
        {
            return size;
        }
    }

    public static class GroupedLongApproximateMostFrequentState
            extends AbstractGroupedAccumulatorState
            implements BigintApproximateMostFrequent.State
    {
        private final ObjectBigArray<ApproximateMostFrequentHistogram<Long>> histograms = new ObjectBigArray<>();
        private long size;

        @Override
        public ApproximateMostFrequentHistogram<Long> get()
        {
            return histograms.get(getGroupId());
        }

        @Override
        public void set(ApproximateMostFrequentHistogram<Long> histogram)
        {
            ApproximateMostFrequentHistogram<Long> previous = histograms.getAndSet(getGroupId(), histogram);
            size += histogram.estimatedInMemorySize();
            if (previous != null) {
                size -= previous.estimatedInMemorySize();
            }
        }

        @Override
        public void ensureCapacity(int size)
        {
            histograms.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize()
        {
            return size + histograms.sizeOf();
        }
    }
}
