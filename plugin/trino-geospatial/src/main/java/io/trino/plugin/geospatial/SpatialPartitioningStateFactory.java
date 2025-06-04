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
package io.trino.plugin.geospatial;

import com.esri.core.geometry.Envelope;
import io.trino.array.IntBigArray;
import io.trino.array.LongBigArray;
import io.trino.array.ObjectBigArray;
import io.trino.geospatial.Rectangle;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;

import java.util.ArrayList;
import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.Math.toIntExact;

public class SpatialPartitioningStateFactory
        implements AccumulatorStateFactory<SpatialPartitioningState>
{
    @Override
    public SpatialPartitioningState createSingleState()
    {
        return new SingleSpatialPartitioningState();
    }

    @Override
    public SpatialPartitioningState createGroupedState()
    {
        return new GroupedSpatialPartitioningState();
    }

    public static final class GroupedSpatialPartitioningState
            implements GroupedAccumulatorState, SpatialPartitioningState
    {
        private static final int INSTANCE_SIZE = instanceSize(GroupedSpatialPartitioningState.class);
        private static final int ENVELOPE_SIZE = toIntExact(new Envelope(1, 2, 3, 4).estimateMemorySize());

        private long groupId;
        private final IntBigArray partitionCounts = new IntBigArray();
        private final LongBigArray counts = new LongBigArray();
        private final ObjectBigArray<Rectangle> envelopes = new ObjectBigArray<>();
        private final ObjectBigArray<List<Rectangle>> samples = new ObjectBigArray<>();
        private int envelopeCount;
        private int samplesCount;

        @Override
        public int getPartitionCount()
        {
            return partitionCounts.get(groupId);
        }

        @Override
        public void setPartitionCount(int partitionCount)
        {
            this.partitionCounts.set(groupId, partitionCount);
        }

        @Override
        public long getCount()
        {
            return counts.get(groupId);
        }

        @Override
        public void setCount(long count)
        {
            counts.set(groupId, count);
        }

        @Override
        public Rectangle getExtent()
        {
            return envelopes.get(groupId);
        }

        @Override
        public void setExtent(Rectangle envelope)
        {
            Rectangle previousEnvelope = envelopes.getAndSet(groupId, envelope);
            if (previousEnvelope == null) {
                envelopeCount++;
            }
        }

        @Override
        public List<Rectangle> getSamples()
        {
            return samples.get(groupId);
        }

        @Override
        public void setSamples(List<Rectangle> samples)
        {
            List<Rectangle> previousSamples = this.samples.getAndSet(groupId, samples);
            samplesCount += samples.size();
            if (previousSamples != null) {
                samplesCount -= previousSamples.size();
            }
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + partitionCounts.sizeOf() + counts.sizeOf() + envelopes.sizeOf() + samples.sizeOf() + (long) ENVELOPE_SIZE * (envelopeCount + samplesCount);
        }

        @Override
        public void setGroupId(int groupId)
        {
            this.groupId = groupId;
        }

        @Override
        public void ensureCapacity(int size)
        {
            partitionCounts.ensureCapacity(size);
            counts.ensureCapacity(size);
            envelopes.ensureCapacity(size);
            samples.ensureCapacity(size);
        }
    }

    public static final class SingleSpatialPartitioningState
            implements SpatialPartitioningState
    {
        private static final int INSTANCE_SIZE = instanceSize(SingleSpatialPartitioningState.class);

        private int partitionCount;
        private long count;
        private Rectangle envelope;
        private List<Rectangle> samples = new ArrayList<>();

        @Override
        public int getPartitionCount()
        {
            return partitionCount;
        }

        @Override
        public void setPartitionCount(int partitionCount)
        {
            this.partitionCount = partitionCount;
        }

        @Override
        public long getCount()
        {
            return count;
        }

        @Override
        public void setCount(long count)
        {
            this.count = count;
        }

        @Override
        public Rectangle getExtent()
        {
            return envelope;
        }

        @Override
        public void setExtent(Rectangle envelope)
        {
            this.envelope = envelope;
        }

        @Override
        public List<Rectangle> getSamples()
        {
            return samples;
        }

        @Override
        public void setSamples(List<Rectangle> samples)
        {
            this.samples = samples;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + (envelope != null ? (long) envelope.estimateMemorySize() * (1 + samples.size()) : 0);
        }
    }
}
