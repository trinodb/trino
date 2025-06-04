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

import com.esri.core.geometry.ogc.OGCGeometry;
import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;

import static io.airlift.slice.SizeOf.instanceSize;

public class GeometryStateFactory
        implements AccumulatorStateFactory<GeometryState>
{
    private static final long OGC_GEOMETRY_BASE_INSTANCE_SIZE = instanceSize(OGCGeometry.class);

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
        private final ObjectBigArray<OGCGeometry> geometries = new ObjectBigArray<>();

        private int groupId;
        private long size;

        @Override
        public OGCGeometry getGeometry()
        {
            return geometries.get(groupId);
        }

        @Override
        public void setGeometry(OGCGeometry geometry)
        {
            OGCGeometry previousValue = this.geometries.getAndSet(groupId, geometry);
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
    private static long getGeometryMemorySize(OGCGeometry geometry)
    {
        if (geometry == null) {
            return 0;
        }
        // Due to the following issue:
        // https://github.com/Esri/geometry-api-java/issues/192
        // We must check if the geometry is empty before calculating its size.  Once the issue is resolved
        // and we bring the fix into our codebase, we can remove this check.
        if (geometry.isEmpty()) {
            return OGC_GEOMETRY_BASE_INSTANCE_SIZE;
        }
        return geometry.estimateMemorySize();
    }

    public static class SingleGeometryState
            implements GeometryState
    {
        private OGCGeometry geometry;

        @Override
        public OGCGeometry getGeometry()
        {
            return geometry;
        }

        @Override
        public void setGeometry(OGCGeometry geometry)
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
