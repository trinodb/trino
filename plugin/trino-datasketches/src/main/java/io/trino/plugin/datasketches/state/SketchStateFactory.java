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
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Union;
import org.openjdk.jol.info.ClassLayout;

public class SketchStateFactory
        implements AccumulatorStateFactory<SketchState>
{
    @Override
    public SketchState createSingleState()
    {
        return new SingleSketchState();
    }

    @Override
    public Class<? extends SketchState> getSingleStateClass()
    {
        return SingleSketchState.class;
    }

    @Override
    public SketchState createGroupedState()
    {
        return new GroupedSketchState();
    }

    @Override
    public Class<? extends SketchState> getGroupedStateClass()
    {
        return GroupedSketchState.class;
    }

    public static class GroupedSketchState
            implements GroupedAccumulatorState, SketchState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedSketchState.class).instanceSize();
        private int nominalEntries;
        private long size;
        private long seed;
        private long groupId;
        private ObjectBigArray<Union> unions;

        public GroupedSketchState()
        {
            unions = new ObjectBigArray();
        }

        @Override
        public void ensureCapacity(long size)
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
            Union union = getUnion();
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

        public Union getUnion()
        {
            return unions.get(groupId);
        }

        public void setMemoryUsage(int value)
        {
            size = value;
        }

        @Override
        public void setGroupId(long groupId)
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
            addSketchToUnion(value, nominalEntries);
        }

        private void addSketchToUnion(Slice value, int nominalEntries)
        {
            Union groupedUnion = getUnion();
            if (groupedUnion == null) {
                groupedUnion = SetOperation.builder().setNominalEntries(nominalEntries).buildUnion();
                groupedUnion.update(Memory.wrap(value.getBytes()));
                unions.set(groupId, groupedUnion);
                setMemoryUsage(value.length());
                return;
            }
            groupedUnion.update(Memory.wrap(value.getBytes()));
            setMemoryUsage(Math.max(value.length(), (int) size));
        }

        public void merge(SketchState otherState)
        {
            addSketchToUnion(otherState.getSketch(), otherState.getNominalEntries());
        }
    }

    public static class SingleSketchState
            implements SketchState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleSketchState.class).instanceSize();
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
            sketch = value;
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

        public void merge(SketchState otherState)
        {
            int normEntries = Math.max(this.getNominalEntries(), otherState.getNominalEntries());
            Union union = SetOperation.builder().setSeed(this.getSeed()).setNominalEntries(normEntries).buildUnion();
            union.update(Memory.wrap(this.getSketch().getBytes()));
            union.update(Memory.wrap(otherState.getSketch().getBytes()));
            Sketch unionResult = union.getResult();
            this.setSketch(Slices.wrappedBuffer(unionResult.toByteArray()));
        }
    }
}
