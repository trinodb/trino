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
import org.apache.datasketches.theta.ThetaIntersection;
import org.apache.datasketches.theta.ThetaSetOperation;
import org.apache.datasketches.theta.ThetaSketch;

import java.lang.foreign.MemorySegment;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;

public class IntersectionStateFactory
        implements AccumulatorStateFactory<SketchState>
{
    @Override
    public SketchState createSingleState()
    {
        return new SingleIntersectionState();
    }

    @Override
    public SketchState createGroupedState()
    {
        return new GroupedIntersectionState();
    }

    private abstract static class BaseIntersectionState
            implements IntersectionState
    {
        private long seed;

        @Override
        public long getSeed()
        {
            return seed;
        }

        @Override
        public void setSeed(long value)
        {
            this.seed = value;
        }

        @Override
        public void merge(SketchState other)
        {
            // Seed may not be set on the receiving state when combine runs before any local input.
            if (getSeed() == 0) {
                setSeed(other.getSeed());
            }
            addSketch(other.getSketch());
        }

        protected static Slice intersect(Slice a, Slice b, long seed)
        {
            ThetaSketch sa = ThetaSketch.wrap(MemorySegment.ofArray(a.getBytes()), seed);
            ThetaSketch sb = ThetaSketch.wrap(MemorySegment.ofArray(b.getBytes()), seed);
            ThetaIntersection inter = ThetaSetOperation.builder().setSeed(seed).buildIntersection();
            inter.intersect(sa);
            inter.intersect(sb);
            return Slices.wrappedBuffer(inter.getResult().toByteArray());
        }
    }

    public static class SingleIntersectionState
            extends BaseIntersectionState
    {
        private static final long INSTANCE_SIZE = instanceSize(SingleIntersectionState.class);
        // Matches sizeOf(byte[]) = ARRAY_HEADER_SIZE + length, computed once to avoid per-call allocation.
        private static final long BYTE_ARRAY_OVERHEAD = sizeOf(new byte[0]);
        private Slice sketch;

        @Override
        public Slice getSketch()
        {
            return sketch;
        }

        @Override
        public void addSketch(Slice value)
        {
            if (value == null) {
                return;
            }
            checkState(getSeed() != 0, "seed is not set");
            sketch = sketch == null ? value : intersect(sketch, value, getSeed());
        }

        @Override
        public void setSketch(Slice value)
        {
            sketch = value;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + (sketch == null ? 0 : BYTE_ARRAY_OVERHEAD + sketch.length());
        }
    }

    public static class GroupedIntersectionState
            extends BaseIntersectionState
            implements GroupedAccumulatorState
    {
        private static final long INSTANCE_SIZE = instanceSize(GroupedIntersectionState.class);
        private long groupId;
        private final ObjectBigArray<Slice> sketches = new ObjectBigArray<>();
        private long totalSize;

        @Override
        public void setGroupId(int groupId)
        {
            this.groupId = groupId;
        }

        @Override
        public void ensureCapacity(int size)
        {
            sketches.ensureCapacity(size);
        }

        @Override
        public Slice getSketch()
        {
            return sketches.get(groupId);
        }

        @Override
        public void addSketch(Slice value)
        {
            if (value == null) {
                return;
            }
            checkState(getSeed() != 0, "seed is not set");
            Slice current = sketches.get(groupId);
            int previousSize = current == null ? 0 : current.length();
            Slice next = current == null ? value : intersect(current, value, getSeed());
            sketches.set(groupId, next);
            totalSize += next.length() - previousSize;
        }

        @Override
        public void setSketch(Slice value)
        {
            Slice current = sketches.get(groupId);
            int previousSize = current == null ? 0 : current.length();
            sketches.set(groupId, value);
            totalSize += (value == null ? 0 : value.length()) - previousSize;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + totalSize + sketches.sizeOf();
        }
    }
}
