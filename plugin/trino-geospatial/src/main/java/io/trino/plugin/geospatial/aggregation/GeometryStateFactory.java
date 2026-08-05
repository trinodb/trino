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

import static io.trino.geospatial.GeometryUtils.estimateMemorySize;

public class GeometryStateFactory
        implements AccumulatorStateFactory<GeometryState>
{
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
            size -= estimateMemorySize(previousValue);
            size += estimateMemorySize(geometry);
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
            return estimateMemorySize(geometry);
        }
    }
}
