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
package io.trino.plugin.geospatial;

import com.google.common.base.Joiner;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.geospatial.GeometryType;
import io.trino.geospatial.serde.JtsGeometrySerde;
import io.trino.spi.TrinoException;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.trino.geospatial.GeometryType.LINE_STRING;
import static io.trino.geospatial.GeometryType.MULTI_POINT;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;

/**
 * A set of functions to convert between geometries and encoded polylines.
 *
 * @see <a href="https://developers.google.com/maps/documentation/utilities/polylinealgorithm">
 * https://developers.google.com/maps/documentation/utilities/polylinealgorithm</a> for a description of encoded polylines.
 */
public final class EncodedPolylineFunctions
{
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    private EncodedPolylineFunctions() {}

    @Description("Decodes a polyline to a linestring")
    @ScalarFunction("from_encoded_polyline")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice fromEncodedPolyline(@SqlType(StandardTypes.VARCHAR) Slice input)
    {
        return JtsGeometrySerde.serialize(decodePolyline(input.toStringUtf8()));
    }

    private static LineString decodePolyline(String polyline)
    {
        List<Coordinate> coordinates = new ArrayList<>();

        int index = 0;
        int latitude = 0;
        int longitude = 0;

        while (index < polyline.length()) {
            int result = 1;
            int shift = 0;
            int bytes;
            do {
                bytes = polyline.charAt(index++) - 63 - 1;
                result += bytes << shift;
                shift += 5;
            }
            while (bytes >= 0x1f);
            latitude += (result & 1) != 0 ? ~(result >> 1) : (result >> 1);

            result = 1;
            shift = 0;
            do {
                bytes = polyline.charAt(index++) - 63 - 1;
                result += bytes << shift;
                shift += 5;
            }
            while (bytes >= 0x1f);
            longitude += (result & 1) != 0 ? ~(result >> 1) : (result >> 1);

            coordinates.add(new Coordinate(longitude * 1e-5, latitude * 1e-5));
        }

        // JTS LineString requires 0 or >= 2 points, so a single point decodes to empty
        if (coordinates.size() < 2) {
            return GEOMETRY_FACTORY.createLineString();
        }

        CoordinateSequence sequence = new CoordinateArraySequence(coordinates.toArray(new Coordinate[0]));
        return new LineString(sequence, GEOMETRY_FACTORY);
    }

    @Description("Encodes a linestring or multipoint geometry to a polyline")
    @ScalarFunction("to_encoded_polyline")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice toEncodedPolyline(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Geometry geometry = JtsGeometrySerde.deserialize(input);
        validateType("encode_polyline", geometry, Set.of(LINE_STRING, MULTI_POINT));
        GeometryType geometryType = GeometryType.getForJtsGeometryType(geometry.getGeometryType());
        return switch (geometryType) {
            case LINE_STRING, MULTI_POINT -> encodePolyline(geometry);
            default -> throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Unexpected geometry type: " + geometryType);
        };
    }

    private static Slice encodePolyline(Geometry geometry)
    {
        long lastLatitude = 0;
        long lastLongitude = 0;

        DynamicSliceOutput output = new DynamicSliceOutput(0);

        Coordinate[] coordinates = geometry.getCoordinates();
        for (Coordinate coordinate : coordinates) {
            long latitude = Math.round(coordinate.getY() * 1e5);
            long longitude = Math.round(coordinate.getX() * 1e5);

            long latitudeDelta = latitude - lastLatitude;
            long longitudeDelta = longitude - lastLongitude;

            encode(latitudeDelta, output);
            encode(longitudeDelta, output);

            lastLatitude = latitude;
            lastLongitude = longitude;
        }
        return output.slice();
    }

    private static void encode(long value, DynamicSliceOutput output)
    {
        value = value < 0 ? ~(value << 1) : value << 1;
        while (value >= 0x20) {
            output.appendByte((byte) ((0x20 | (value & 0x1f)) + 63));
            value >>= 5;
        }
        output.appendByte((byte) (value + 63));
    }

    private static void validateType(String function, Geometry geometry, Set<GeometryType> validTypes)
    {
        GeometryType type = GeometryType.getForJtsGeometryType(geometry.getGeometryType());
        if (!validTypes.contains(type)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("%s only applies to %s. Input type is: %s", function, Joiner.on(" or ").join(validTypes), type));
        }
    }
}
