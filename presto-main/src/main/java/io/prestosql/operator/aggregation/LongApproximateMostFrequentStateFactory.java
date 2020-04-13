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
package io.prestosql.operator.aggregation;

import io.prestosql.array.ObjectBigArray;
import io.prestosql.operator.aggregation.state.AbstractGroupedAccumulatorState;
import io.prestosql.spi.function.AccumulatorStateFactory;

public class LongApproximateMostFrequentStateFactory
        implements AccumulatorStateFactory<ApproximateMostFrequentFunction.LongState>
{
    @Override
    public ApproximateMostFrequentFunction.LongState createSingleState()
    {
        return new SingleLongApproximateMostFrequentState();
    }

    @Override
    public Class<? extends ApproximateMostFrequentFunction.LongState> getSingleStateClass()
    {
        return SingleLongApproximateMostFrequentState.class;
    }

    @Override
    public ApproximateMostFrequentFunction.LongState createGroupedState()
    {
        return new GroupedLongApproximateMostFrequentState();
    }

    @Override
    public Class<? extends ApproximateMostFrequentFunction.LongState> getGroupedStateClass()
    {
        return GroupedLongApproximateMostFrequentState.class;
    }

    public static class SingleLongApproximateMostFrequentState
            implements ApproximateMostFrequentFunction.LongState
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
            implements ApproximateMostFrequentFunction.LongState
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
            ApproximateMostFrequentHistogram<Long> previous = get();
            if (previous != null) {
                size -= previous.estimatedInMemorySize();
            }

            histograms.set(getGroupId(), histogram);
            size += histogram.estimatedInMemorySize();
        }

        @Override
        public void ensureCapacity(long size)
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
