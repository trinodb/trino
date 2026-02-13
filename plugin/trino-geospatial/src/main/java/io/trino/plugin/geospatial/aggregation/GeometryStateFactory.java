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

import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;
import org.locationtech.jts.geom.Geometry;

import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.instanceSize;

public class GeometryStateFactory
        implements AccumulatorStateFactory<GeometryState>
{
    // Base overhead for a JTS Geometry object (approximate)
    private static final long GEOMETRY_BASE_INSTANCE_SIZE = instanceSize(Geometry.class);
    // Size of each coordinate (x, y = 2 doubles)
    private static final long COORDINATE_SIZE = 2 * SIZE_OF_DOUBLE;

    @Override
    public GeometryState createSingleState()
    {
        return new SingleGeometryState();
    }

    @Override
    public GeometryState createGroupedState()
    {
        return new GroupedGeometryState();
    }

    public static class GroupedGeometryState
            implements GeometryState, GroupedAccumulatorState
    {
        private final ObjectBigArray<Geometry> geometries = new ObjectBigArray<>();

        private int groupId;
        private long size;

        @Override
        public Geometry getGeometry()
        {
            return geometries.get(groupId);
        }

        @Override
        public void setGeometry(Geometry geometry)
        {
            Geometry previousValue = this.geometries.getAndSet(groupId, geometry);
            size -= getGeometryMemorySize(previousValue);
            size += getGeometryMemorySize(geometry);
        }

        @Override
        public void ensureCapacity(int size)
        {
            geometries.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize()
        {
            return size + geometries.sizeOf();
        }

        @Override
        public final void setGroupId(int groupId)
        {
            this.groupId = groupId;
        }
    }

    // Do a best-effort attempt to estimate the memory size
    private static long getGeometryMemorySize(Geometry geometry)
    {
        if (geometry == null) {
            return 0;
        }
        if (geometry.isEmpty()) {
            return GEOMETRY_BASE_INSTANCE_SIZE;
        }
        // Estimate: base size + size per coordinate
        return GEOMETRY_BASE_INSTANCE_SIZE + (long) geometry.getNumPoints() * COORDINATE_SIZE;
    }

    public static class SingleGeometryState
            implements GeometryState
    {
        private Geometry geometry;

        @Override
        public Geometry getGeometry()
        {
            return geometry;
        }

        @Override
        public void setGeometry(Geometry geometry)
        {
            this.geometry = geometry;
        }

        @Override
        public long getEstimatedSize()
        {
            return getGeometryMemorySize(geometry);
        }
    }
}
