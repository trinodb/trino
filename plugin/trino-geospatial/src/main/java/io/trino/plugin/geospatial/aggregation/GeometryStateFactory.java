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
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfObjectArray;

public class GeometryStateFactory
        implements AccumulatorStateFactory<GeometryState>
{
    private static final long POINT_INSTANCE_SIZE = instanceSize(Point.class);
    private static final long LINE_STRING_INSTANCE_SIZE = instanceSize(LineString.class);
    private static final long LINEAR_RING_INSTANCE_SIZE = instanceSize(LinearRing.class);
    private static final long POLYGON_INSTANCE_SIZE = instanceSize(Polygon.class);
    private static final long GEOMETRY_COLLECTION_INSTANCE_SIZE = instanceSize(GeometryCollection.class);

    private static final long COORDINATE_ARRAY_SEQUENCE_INSTANCE_SIZE = instanceSize(CoordinateArraySequence.class);
    private static final long COORDINATE_INSTANCE_SIZE = instanceSize(Coordinate.class);

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

        if (geometry instanceof Point point) {
            return POINT_INSTANCE_SIZE + getCoordinateSequenceMemorySize(point.getCoordinateSequence());
        }
        if (geometry instanceof LinearRing linearRing) {
            return LINEAR_RING_INSTANCE_SIZE + getCoordinateSequenceMemorySize(linearRing.getCoordinateSequence());
        }
        if (geometry instanceof LineString lineString) {
            return LINE_STRING_INSTANCE_SIZE + getCoordinateSequenceMemorySize(lineString.getCoordinateSequence());
        }
        if (geometry instanceof Polygon polygon) {
            long size = POLYGON_INSTANCE_SIZE + sizeOfObjectArray(polygon.getNumInteriorRing());
            size += getGeometryMemorySize(polygon.getExteriorRing());
            for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
                size += getGeometryMemorySize(polygon.getInteriorRingN(i));
            }
            return size;
        }
        if (geometry instanceof GeometryCollection geometryCollection) {
            long size = GEOMETRY_COLLECTION_INSTANCE_SIZE + sizeOfObjectArray(geometryCollection.getNumGeometries());
            for (int i = 0; i < geometryCollection.getNumGeometries(); i++) {
                size += getGeometryMemorySize(geometryCollection.getGeometryN(i));
            }
            return size;
        }

        return instanceSize(geometry.getClass());
    }

    private static long getCoordinateSequenceMemorySize(CoordinateSequence coordinateSequence)
    {
        return COORDINATE_ARRAY_SEQUENCE_INSTANCE_SIZE +
                sizeOfObjectArray(coordinateSequence.size()) +
                (long) coordinateSequence.size() * COORDINATE_INSTANCE_SIZE;
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
