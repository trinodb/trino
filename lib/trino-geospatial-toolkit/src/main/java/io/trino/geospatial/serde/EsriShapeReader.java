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
package io.trino.geospatial.serde;

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.List;

/**
 * Parser for ESRI Shape binary format.
 * <p>
 * ESRI Shape format is used in ESRI Shapefiles and by the ESRI Geometry API.
 * This parser supports the 2D shape types required for Hadoop Spatial Framework compatibility.
 * <p>
 * Supported shape types:
 * <ul>
 *   <li>0 - Null Shape (empty geometry)</li>
 *   <li>1 - Point</li>
 *   <li>3 - PolyLine (becomes LineString or MultiLineString)</li>
 *   <li>5 - Polygon (becomes Polygon or MultiPolygon)</li>
 *   <li>8 - MultiPoint</li>
 * </ul>
 * <p>
 * Note: Z and M variants (PointZ, PolyLineZ, etc.) are not supported.
 *
 * @see <a href="https://www.esri.com/library/whitepapers/pdfs/shapefile.pdf">ESRI Shapefile Technical Description</a>
 */
public final class EsriShapeReader
{
    // Shape types from ESRI Shapefile specification
    private static final int NULL_SHAPE = 0;
    private static final int POINT = 1;
    private static final int POLYLINE = 3;
    private static final int POLYGON = 5;
    private static final int MULTIPOINT = 8;

    // Bounding box size: 4 doubles (Xmin, Ymin, Xmax, Ymax)
    private static final int BOUNDING_BOX_SIZE = 32;

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    private EsriShapeReader() {}

    public static Geometry read(Slice slice)
    {
        BasicSliceInput input = slice.getInput();

        int shapeType = input.readInt();

        return switch (shapeType) {
            case NULL_SHAPE -> GEOMETRY_FACTORY.createPoint();
            case POINT -> readPoint(input);
            case POLYLINE -> readPolyLine(input);
            case POLYGON -> readPolygon(input);
            case MULTIPOINT -> readMultiPoint(input);
            default -> throw new IllegalArgumentException("Unsupported ESRI shape type: " + shapeType);
        };
    }

    private static Geometry readPoint(BasicSliceInput input)
    {
        double x = input.readDouble();
        double y = input.readDouble();

        // ESRI represents empty points using NaN or extreme values
        if (Double.isNaN(x) || Double.isNaN(y) || isEmptyValue(x) || isEmptyValue(y)) {
            return GEOMETRY_FACTORY.createPoint();
        }

        return GEOMETRY_FACTORY.createPoint(new Coordinate(x, y));
    }

    /**
     * Checks if a value represents an empty coordinate in ESRI format.
     * ESRI uses extreme values close to Double.MAX_VALUE to indicate empty.
     */
    private static boolean isEmptyValue(double value)
    {
        return value <= -Double.MAX_VALUE || value >= Double.MAX_VALUE;
    }

    private static Geometry readMultiPoint(BasicSliceInput input)
    {
        // Check bounding box for empty (4 doubles: Xmin, Ymin, Xmax, Ymax)
        if (isEmptyBoundingBox(input)) {
            input.skip(BOUNDING_BOX_SIZE);
            input.readInt(); // skip numPoints (should be 0)
            return GEOMETRY_FACTORY.createMultiPoint();
        }
        input.skip(BOUNDING_BOX_SIZE);

        int numPoints = input.readInt();
        if (numPoints == 0) {
            return GEOMETRY_FACTORY.createMultiPoint();
        }

        Coordinate[] coords = new Coordinate[numPoints];
        for (int i = 0; i < numPoints; i++) {
            double x = input.readDouble();
            double y = input.readDouble();
            coords[i] = new Coordinate(x, y);
        }

        return GEOMETRY_FACTORY.createMultiPointFromCoords(coords);
    }

    /**
     * Checks if the bounding box indicates an empty geometry.
     * Does not advance the input position.
     */
    private static boolean isEmptyBoundingBox(BasicSliceInput input)
    {
        long pos = input.position();
        double xMin = input.readDouble();
        input.setPosition(pos); // reset to original position
        return Double.isNaN(xMin) || isEmptyValue(xMin);
    }

    private static Geometry readPolyLine(BasicSliceInput input)
    {
        // Check bounding box for empty
        if (isEmptyBoundingBox(input)) {
            input.skip(BOUNDING_BOX_SIZE);
            input.readInt(); // skip numParts
            input.readInt(); // skip numPoints (should be 0)
            return GEOMETRY_FACTORY.createLineString();
        }
        input.skip(BOUNDING_BOX_SIZE);

        int numParts = input.readInt();
        int numPoints = input.readInt();

        if (numParts == 0 || numPoints == 0) {
            return GEOMETRY_FACTORY.createLineString();
        }

        // Read part indices
        int[] partIndices = new int[numParts];
        for (int i = 0; i < numParts; i++) {
            partIndices[i] = input.readInt();
        }

        // Read all points
        Coordinate[] allCoords = new Coordinate[numPoints];
        for (int i = 0; i < numPoints; i++) {
            double x = input.readDouble();
            double y = input.readDouble();
            allCoords[i] = new Coordinate(x, y);
        }

        // Build LineStrings for each part
        LineString[] lineStrings = new LineString[numParts];
        for (int i = 0; i < numParts; i++) {
            int startIndex = partIndices[i];
            int endIndex = (i + 1 < numParts) ? partIndices[i + 1] : numPoints;
            int partLength = endIndex - startIndex;

            Coordinate[] partCoords = new Coordinate[partLength];
            System.arraycopy(allCoords, startIndex, partCoords, 0, partLength);
            lineStrings[i] = GEOMETRY_FACTORY.createLineString(partCoords);
        }

        if (numParts == 1) {
            return lineStrings[0];
        }
        return GEOMETRY_FACTORY.createMultiLineString(lineStrings);
    }

    private static Geometry readPolygon(BasicSliceInput input)
    {
        // Check bounding box for empty
        if (isEmptyBoundingBox(input)) {
            input.skip(BOUNDING_BOX_SIZE);
            input.readInt(); // skip numParts
            input.readInt(); // skip numPoints (should be 0)
            return GEOMETRY_FACTORY.createPolygon();
        }
        input.skip(BOUNDING_BOX_SIZE);

        int numParts = input.readInt();
        int numPoints = input.readInt();

        if (numParts == 0 || numPoints == 0) {
            return GEOMETRY_FACTORY.createPolygon();
        }

        // Read part indices
        int[] partIndices = new int[numParts];
        for (int i = 0; i < numParts; i++) {
            partIndices[i] = input.readInt();
        }

        // Read all points
        Coordinate[] allCoords = new Coordinate[numPoints];
        for (int i = 0; i < numPoints; i++) {
            double x = input.readDouble();
            double y = input.readDouble();
            allCoords[i] = new Coordinate(x, y);
        }

        // Build rings for each part
        LinearRing[] rings = new LinearRing[numParts];
        for (int i = 0; i < numParts; i++) {
            int startIndex = partIndices[i];
            int endIndex = (i + 1 < numParts) ? partIndices[i + 1] : numPoints;
            int partLength = endIndex - startIndex;

            Coordinate[] partCoords = new Coordinate[partLength];
            System.arraycopy(allCoords, startIndex, partCoords, 0, partLength);
            rings[i] = GEOMETRY_FACTORY.createLinearRing(partCoords);
        }

        // Organize rings into polygons
        // ESRI format: exterior rings are clockwise, interior rings (holes) are counter-clockwise
        // JTS format: exterior rings are counter-clockwise, interior rings are clockwise
        // We need to identify which rings are exterior (shells) and which are interior (holes)
        return createPolygonsFromRings(rings);
    }

    private static Geometry createPolygonsFromRings(LinearRing[] rings)
    {
        if (rings.length == 1) {
            return GEOMETRY_FACTORY.createPolygon(rings[0]);
        }

        // Separate exterior and interior rings
        // ESRI: clockwise = exterior, counter-clockwise = interior
        List<Polygon> polygons = new ArrayList<>();
        LinearRing currentShell = null;
        List<LinearRing> currentHoles = new ArrayList<>();

        for (LinearRing ring : rings) {
            boolean isClockwise = isClockwise(ring.getCoordinates());

            if (isClockwise) {
                // Clockwise = exterior ring in ESRI format
                if (currentShell != null) {
                    polygons.add(GEOMETRY_FACTORY.createPolygon(currentShell, currentHoles.toArray(new LinearRing[0])));
                    currentHoles.clear();
                }
                currentShell = ring;
            }
            else {
                // Counter-clockwise = interior ring (hole) in ESRI format
                if (currentShell != null) {
                    currentHoles.add(ring);
                }
            }
        }

        // Don't forget the last polygon
        if (currentShell != null) {
            polygons.add(GEOMETRY_FACTORY.createPolygon(currentShell, currentHoles.toArray(new LinearRing[0])));
        }

        if (polygons.size() == 1) {
            return polygons.getFirst();
        }
        return GEOMETRY_FACTORY.createMultiPolygon(polygons.toArray(new Polygon[0]));
    }

    /**
     * Determines if a ring is clockwise using the shoelace formula.
     * Positive signed area = counter-clockwise, negative signed area = clockwise.
     */
    private static boolean isClockwise(Coordinate[] ring)
    {
        double sum = 0;
        for (int i = 0; i < ring.length - 1; i++) {
            sum += (ring[i + 1].x - ring[i].x) * (ring[i + 1].y + ring[i].y);
        }
        return sum > 0;
    }
}
