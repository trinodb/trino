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
package io.trino.plugin.datasketches.state;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;
import org.apache.datasketches.theta.ThetaSetOperation;
import org.apache.datasketches.theta.ThetaSketch;
import org.apache.datasketches.theta.ThetaUnion;

import java.lang.foreign.MemorySegment;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;

public class SketchStateFactory
        implements AccumulatorStateFactory<SketchState>
{
    @Override
    public SketchState createSingleState()
    {
        return new SingleSketchState();
    }

    @Override
    public SketchState createGroupedState()
    {
        return new GroupedSketchState();
    }

    public static class GroupedSketchState
            implements GroupedAccumulatorState, SketchState
    {
        private static final long INSTANCE_SIZE = instanceSize(GroupedSketchState.class);
        private int nominalEntries;
        private long size;
        private long seed;
        private long groupId;
        private final ObjectBigArray<ThetaUnion> unions = new ObjectBigArray<>();

        @Override
        public void ensureCapacity(int size)
        {
            unions.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + unions.sizeOf();
        }

        @Override
        public Slice getSketch()
        {
            ThetaUnion union = getUnion();
            if (union != null) {
                return Slices.wrappedBuffer(union.getResult().toByteArray());
            }
            return null;
        }

        @Override
        public int getNominalEntries()
        {
            return nominalEntries;
        }

        @Override
        public long getSeed()
        {
            return seed;
        }

        public ThetaUnion getUnion()
        {
            return unions.get(groupId);
        }

        public void setMemoryUsage(int value)
        {
            size = value;
        }

        @Override
        public void setGroupId(int groupId)
        {
            this.groupId = groupId;
        }

        @Override
        public void setNominalEntries(int value)
        {
            nominalEntries = value;
        }

        @Override
        public void setSeed(long value)
        {
            seed = value;
        }

        @Override
        public void setSketch(Slice value)
        {
            checkState(nominalEntries > 0, "nominalEntries is not set");
            checkState(seed != 0, "seed is not set");
            addSketchToUnion(value, nominalEntries);
        }

        private void addSketchToUnion(Slice value, int nominalEntries)
        {
            checkState(value != null, "sketch is null");
            checkState(nominalEntries > 0, "nominalEntries is not set");
            ThetaSketch sketch = ThetaSketch.wrap(MemorySegment.ofArray(value.getBytes()), seed);
            ThetaUnion groupedUnion = getUnion();
            if (groupedUnion == null) {
                groupedUnion = ThetaSetOperation.builder()
                        .setSeed(seed)
                        .setNominalEntries(nominalEntries)
                        .buildUnion();
                groupedUnion.union(sketch);
                unions.set(groupId, groupedUnion);
                setMemoryUsage(value.length());
                return;
            }
            groupedUnion.union(sketch);
            setMemoryUsage(Math.max(value.length(), (int) size));
        }

        @Override
        public void merge(SketchState otherState)
        {
            addSketchToUnion(otherState.getSketch(), otherState.getNominalEntries());
        }
    }

    public static class SingleSketchState
            implements SketchState
    {
        private static final long INSTANCE_SIZE = instanceSize(SingleSketchState.class);
        private Slice sketch;
        private int nominalEntries;
        private long seed;

        @Override
        public Slice getSketch()
        {
            return sketch;
        }

        @Override
        public int getNominalEntries()
        {
            return nominalEntries;
        }

        @Override
        public long getSeed()
        {
            return seed;
        }

        @Override
        public void setSketch(Slice value)
        {
            if (value == null) {
                sketch = null;
                return;
            }

            if (sketch == null) {
                sketch = value;
                return;
            }

            long seed = this.seed;
            ThetaUnion union = ThetaSetOperation.builder()
                    .setSeed(seed)
                    .setNominalEntries(nominalEntries)
                    .buildUnion();
            union.union(ThetaSketch.wrap(MemorySegment.ofArray(sketch.getBytes()), seed));
            union.union(ThetaSketch.wrap(MemorySegment.ofArray(value.getBytes()), seed));
            sketch = Slices.wrappedBuffer(union.getResult().toByteArray());
        }

        @Override
        public void setNominalEntries(int value)
        {
            nominalEntries = value;
        }

        @Override
        public void setSeed(long value)
        {
            seed = value;
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (sketch != null) {
                estimatedSize += sketch.getRetainedSize();
            }
            return estimatedSize;
        }

        @Override
        public void merge(SketchState otherState)
        {
            if (otherState.getSketch() == null) {
                return;
            }

            if (getSketch() == null) {
                setNominalEntries(otherState.getNominalEntries());
                setSeed(otherState.getSeed());
                sketch = otherState.getSketch();
                return;
            }

            long seed = this.getSeed() == 0 ? otherState.getSeed() : this.getSeed();
            int nominalEntries = Math.max(this.getNominalEntries(), otherState.getNominalEntries());
            ThetaUnion union = ThetaSetOperation.builder()
                    .setSeed(seed)
                    .setNominalEntries(nominalEntries)
                    .buildUnion();
            union.union(ThetaSketch.wrap(MemorySegment.ofArray(getSketch().getBytes()), seed));
            union.union(ThetaSketch.wrap(MemorySegment.ofArray(otherState.getSketch().getBytes()), seed));
            ThetaSketch unionResult = union.getResult();
            sketch = Slices.wrappedBuffer(unionResult.toByteArray());
            setNominalEntries(nominalEntries);
            setSeed(seed);
        }
    }
}
