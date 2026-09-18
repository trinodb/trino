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
package io.trino.plugin.geospatial.aggregation;

import io.trino.array.IntBigArray;
import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;
import org.locationtech.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.geospatial.GeometryUtils.estimateMemorySize;

public class GeometryListStateFactory
        implements AccumulatorStateFactory<GeometryListState>
{
    @Override
    public GeometryListState createSingleState()
    {
        return new SingleGeometryListState();
    }

    @Override
    public GeometryListState createGroupedState()
    {
        return new GroupedGeometryListState();
    }

    public static class GroupedGeometryListState
            implements GeometryListState, GroupedAccumulatorState
    {
        private static final long INSTANCE_SIZE = instanceSize(GroupedGeometryListState.class);

        private final ObjectBigArray<List<Geometry>> geometries = new ObjectBigArray<>();
        private final IntBigArray srids = new IntBigArray();

        private int groupId;
        private long size;

        @Override
        public List<Geometry> getGeometries()
        {
            return geometries.get(groupId);
        }

        @Override
        public void setGeometries(List<Geometry> geometries)
        {
            List<Geometry> previousValue = this.geometries.getAndSet(groupId, geometries);
            size -= estimatedMemorySize(previousValue);
            size += estimatedMemorySize(geometries);
        }

        @Override
        public int getSrid()
        {
            return srids.get(groupId);
        }

        @Override
        public void setSrid(int srid)
        {
            srids.set(groupId, srid);
        }

        @Override
        public void addGeometry(Geometry geometry)
        {
            List<Geometry> geometries = this.geometries.get(groupId);
            if (geometries == null) {
                geometries = new ArrayList<>();
                this.geometries.set(groupId, geometries);
            }
            geometries.add(geometry);
            size += estimateMemorySize(geometry);
        }

        @Override
        public void ensureCapacity(int size)
        {
            geometries.ensureCapacity(size);
            srids.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + geometries.sizeOf() + srids.sizeOf();
        }

        @Override
        public final void setGroupId(int groupId)
        {
            this.groupId = groupId;
        }
    }

    public static class SingleGeometryListState
            implements GeometryListState
    {
        private List<Geometry> geometries;
        private int srid;

        @Override
        public List<Geometry> getGeometries()
        {
            return geometries;
        }

        @Override
        public void setGeometries(List<Geometry> geometries)
        {
            this.geometries = geometries;
        }

        @Override
        public int getSrid()
        {
            return srid;
        }

        @Override
        public void setSrid(int srid)
        {
            this.srid = srid;
        }

        @Override
        public void addGeometry(Geometry geometry)
        {
            if (geometries == null) {
                geometries = new ArrayList<>();
            }
            geometries.add(geometry);
        }

        @Override
        public long getEstimatedSize()
        {
            return estimatedMemorySize(geometries);
        }
    }

    private static long estimatedMemorySize(List<Geometry> geometries)
    {
        if (geometries == null) {
            return 0;
        }
        long size = 0;
        for (Geometry geometry : geometries) {
            size += estimateMemorySize(geometry);
        }
        return size;
    }
}
