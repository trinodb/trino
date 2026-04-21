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
package io.trino.hive.formats.esri;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.END_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.FIELD_NAME;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NUMBER_FLOAT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NUMBER_INT;

/**
 * Parser for ESRI JSON geometry format.
 * <p>
 * ESRI JSON format examples:
 * <ul>
 *   <li>Point: {@code {"x": 10, "y": 20}}</li>
 *   <li>MultiPoint: {@code {"points": [[x1,y1], [x2,y2], ...]}}</li>
 *   <li>Polyline: {@code {"paths": [[[x1,y1], [x2,y2], ...], ...]}}</li>
 *   <li>Polygon: {@code {"rings": [[[x1,y1], [x2,y2], ...], ...]}}</li>
 * </ul>
 *
 * @see <a href="https://developers.arcgis.com/documentation/common-data-types/geometry-objects.htm">ESRI Geometry Objects</a>
 */
public final class EsriJsonParser
{
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    private EsriJsonParser() {}

    /**
     * Parses an ESRI JSON geometry object from the parser.
     * Parser must be positioned at the START_OBJECT token of the geometry.
     * After parsing, the parser will be positioned at the END_OBJECT token.
     */
    public static Geometry parseGeometry(JsonParser parser)
            throws IOException
    {
        if (parser.currentToken() != JsonToken.START_OBJECT) {
            throw new IOException("Expected START_OBJECT, got " + parser.currentToken());
        }

        Double x = null;
        Double y = null;
        List<Coordinate[]> paths = null;
        List<Coordinate[]> rings = null;
        List<Coordinate> points = null;
        int srid = 0;

        while (parser.nextToken() != END_OBJECT) {
            if (parser.currentToken() != FIELD_NAME) {
                throw new IOException("Expected field name, got " + parser.currentToken());
            }

            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case "x" -> x = parseDouble(parser);
                case "y" -> y = parseDouble(parser);
                case "paths" -> paths = parseCoordinateArrays(parser);
                case "rings" -> rings = parseCoordinateArrays(parser);
                case "points" -> points = parseCoordinateArray(parser);
                case "spatialReference" -> srid = parseSpatialReference(parser);
                default -> skipValue(parser);
            }
        }

        Geometry geometry;
        // Determine geometry type from the fields present
        if (x != null && y != null && !x.isNaN() && !y.isNaN()) {
            // Point
            geometry = GEOMETRY_FACTORY.createPoint(new Coordinate(x, y));
        }
        else if (x != null || y != null) {
            // Empty point
            geometry = GEOMETRY_FACTORY.createPoint();
        }
        else if (points != null) {
            // MultiPoint
            if (points.isEmpty()) {
                geometry = GEOMETRY_FACTORY.createMultiPoint();
            }
            else {
                geometry = GEOMETRY_FACTORY.createMultiPointFromCoords(points.toArray(new Coordinate[0]));
            }
        }
        else if (paths != null) {
            // Polyline (LineString or MultiLineString)
            if (paths.isEmpty()) {
                geometry = GEOMETRY_FACTORY.createMultiLineString();
            }
            else if (paths.size() == 1) {
                geometry = GEOMETRY_FACTORY.createLineString(paths.getFirst());
            }
            else {
                geometry = GEOMETRY_FACTORY.createMultiLineString(
                        paths.stream()
                                .map(GEOMETRY_FACTORY::createLineString)
                                .toArray(LineString[]::new));
            }
        }
        else if (rings != null) {
            // Polygon (may contain holes)
            if (rings.isEmpty()) {
                geometry = GEOMETRY_FACTORY.createPolygon();
            }
            else {
                geometry = createPolygonFromRings(rings);
            }
        }
        else {
            throw new IOException("Unknown geometry type: no recognized fields found");
        }

        geometry.setSRID(srid);
        return geometry;
    }

    private static Double parseDouble(JsonParser parser)
            throws IOException
    {
        JsonToken token = parser.currentToken();
        if (token == JsonToken.VALUE_NULL) {
            return null;
        }
        if (token == VALUE_NUMBER_FLOAT ||
                token == VALUE_NUMBER_INT ||
                token == JsonToken.VALUE_STRING ||
                token == JsonToken.VALUE_TRUE ||
                token == JsonToken.VALUE_FALSE) {
            return parser.getValueAsDouble();
        }
        throw new IOException("Expected number, got " + token);
    }

    private static int parseSpatialReference(JsonParser parser)
            throws IOException
    {
        if (parser.currentToken() == JsonToken.VALUE_NULL) {
            return 0;
        }
        if (parser.currentToken() != JsonToken.START_OBJECT) {
            throw new IOException("Expected START_OBJECT or VALUE_NULL for spatialReference, got " + parser.currentToken());
        }

        Integer wkid = null;
        Integer latestWkid = null;
        while (parser.nextToken() != END_OBJECT) {
            if (parser.currentToken() != FIELD_NAME) {
                throw new IOException("Expected field name, got " + parser.currentToken());
            }

            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case "wkid" -> wkid = parseInteger(parser);
                case "latestWkid" -> latestWkid = parseInteger(parser);
                default -> skipValue(parser);
            }
        }

        if (latestWkid != null && latestWkid > 0) {
            return latestWkid;
        }
        if (wkid != null && wkid > 0) {
            return wkid;
        }
        return 0;
    }

    private static Integer parseInteger(JsonParser parser)
            throws IOException
    {
        if (parser.currentToken() == VALUE_NUMBER_INT) {
            return parser.getIntValue();
        }
        skipValue(parser);
        return null;
    }

    private static List<Coordinate[]> parseCoordinateArrays(JsonParser parser)
            throws IOException
    {
        if (parser.currentToken() != START_ARRAY) {
            throw new IOException("Expected START_ARRAY for paths/rings, got " + parser.currentToken());
        }

        List<Coordinate[]> result = new ArrayList<>();
        while (parser.nextToken() != END_ARRAY) {
            if (parser.currentToken() == START_ARRAY) {
                result.add(parseCoordinateArray(parser).toArray(new Coordinate[0]));
            }
            else {
                throw new IOException("Expected START_ARRAY for path/ring, got " + parser.currentToken());
            }
        }
        return result;
    }

    private static List<Coordinate> parseCoordinateArray(JsonParser parser)
            throws IOException
    {
        if (parser.currentToken() != START_ARRAY) {
            throw new IOException("Expected START_ARRAY for coordinates, got " + parser.currentToken());
        }

        List<Coordinate> result = new ArrayList<>();
        while (parser.nextToken() != END_ARRAY) {
            if (parser.currentToken() == START_ARRAY) {
                result.add(parseCoordinate(parser));
            }
            else {
                throw new IOException("Expected START_ARRAY for coordinate, got " + parser.currentToken());
            }
        }
        return result;
    }

    private static Coordinate parseCoordinate(JsonParser parser)
            throws IOException
    {
        // Coordinate is an array: [x, y] or [x, y, z] or [x, y, z, m]
        if (parser.currentToken() != START_ARRAY) {
            throw new IOException("Expected START_ARRAY for coordinate, got " + parser.currentToken());
        }

        // Read x
        parser.nextToken();
        double x = parser.getDoubleValue();

        // Read y
        parser.nextToken();
        double y = parser.getDoubleValue();

        // Skip any remaining values (z, m) and consume END_ARRAY
        while (parser.nextToken() != END_ARRAY) {
            // Skip z and m values
        }

        return new Coordinate(x, y);
    }

    private static Geometry createPolygonFromRings(List<Coordinate[]> rings)
    {
        if (rings.size() == 1) {
            LinearRing shell = GEOMETRY_FACTORY.createLinearRing(rings.getFirst());
            return GEOMETRY_FACTORY.createPolygon(shell);
        }

        // Treat the first ring's orientation as the shell orientation for this geometry.
        // A ring with the same orientation starts a new polygon, while the opposite
        // orientation becomes a hole in the current polygon. This preserves Hive
        // behavior for mixed and non-standard ring orderings.
        boolean shellOrientation = isClockwise(rings.getFirst());
        List<Polygon> polygons = new ArrayList<>();
        LinearRing currentShell = GEOMETRY_FACTORY.createLinearRing(rings.getFirst());
        List<LinearRing> currentHoles = new ArrayList<>();

        for (Coordinate[] ring : rings.subList(1, rings.size())) {
            if (isClockwise(ring) == shellOrientation) {
                polygons.add(GEOMETRY_FACTORY.createPolygon(currentShell, currentHoles.toArray(new LinearRing[0])));
                currentHoles.clear();
                currentShell = GEOMETRY_FACTORY.createLinearRing(ring);
            }
            else {
                currentHoles.add(GEOMETRY_FACTORY.createLinearRing(ring));
            }
        }

        polygons.add(GEOMETRY_FACTORY.createPolygon(currentShell, currentHoles.toArray(new LinearRing[0])));

        if (polygons.size() == 1) {
            return polygons.getFirst();
        }
        return GEOMETRY_FACTORY.createMultiPolygon(polygons.toArray(new Polygon[0]));
    }

    /**
     * Determines if a ring is clockwise using the shoelace formula.
     * Positive area = counter-clockwise, negative area = clockwise.
     */
    private static boolean isClockwise(Coordinate[] ring)
    {
        double sum = 0;
        for (int i = 0; i < ring.length - 1; i++) {
            sum += (ring[i + 1].x - ring[i].x) * (ring[i + 1].y + ring[i].y);
        }
        return sum < 0;
    }

    private static void skipValue(JsonParser parser)
            throws IOException
    {
        JsonToken token = parser.currentToken();
        if (token == START_ARRAY || token == JsonToken.START_OBJECT) {
            parser.skipChildren();
        }
    }
}
