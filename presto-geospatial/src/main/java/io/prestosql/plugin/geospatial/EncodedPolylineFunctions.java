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
package io.prestosql.plugin.geospatial;

import com.esri.core.geometry.MultiPath;
import com.esri.core.geometry.MultiVertexGeometry;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCLineString;
import com.google.common.base.Joiner;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.prestosql.geospatial.GeometryType;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.util.EnumSet;
import java.util.Set;

import static io.prestosql.geospatial.GeometryType.LINE_STRING;
import static io.prestosql.geospatial.GeometryType.MULTI_POINT;
import static io.prestosql.geospatial.serde.GeometrySerde.deserialize;
import static io.prestosql.geospatial.serde.GeometrySerde.serialize;
import static io.prestosql.plugin.geospatial.GeometryType.GEOMETRY_TYPE_NAME;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;

/**
 * A set of functions to convert between geometries and encoded polylines.
 *
 * @see <a href="https://developers.google.com/maps/documentation/utilities/polylinealgorithm">
 * https://developers.google.com/maps/documentation/utilities/polylinealgorithm</a> for a description of encoded polylines.
 */
public final class EncodedPolylineFunctions
{
    private EncodedPolylineFunctions() {}

    @Description("Decodes a polyline to a linestring")
    @ScalarFunction("from_encoded_polyline")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice fromEncodedPolyline(@SqlType(StandardTypes.VARCHAR) Slice input)
    {
        return serialize(decodePolyline(input.toStringUtf8()));
    }

    private static OGCLineString decodePolyline(String polyline)
    {
        MultiPath multipath = new Polyline();
        boolean isFirstPoint = true;

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

            if (isFirstPoint) {
                multipath.startPath(longitude * 1e-5, latitude * 1e-5);
                isFirstPoint = false;
            }
            else {
                multipath.lineTo(longitude * 1e-5, latitude * 1e-5);
            }
        }

        return new OGCLineString(multipath, 0, null);
    }

    @Description("Encodes a linestring or multipoint geometry to a polyline")
    @ScalarFunction("to_encoded_polyline")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice toEncodedPolyline(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        validateType("encode_polyline", geometry, EnumSet.of(LINE_STRING, MULTI_POINT));
        GeometryType geometryType = GeometryType.getForEsriGeometryType(geometry.geometryType());
        switch (geometryType) {
            case LINE_STRING:
            case MULTI_POINT:
                return encodePolyline((MultiVertexGeometry) geometry.getEsriGeometry());
            default:
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Unexpected geometry type: " + geometryType);
        }
    }

    private static Slice encodePolyline(MultiVertexGeometry multiVertexGeometry)
    {
        long lastLatitude = 0;
        long lastLongitude = 0;

        DynamicSliceOutput output = new DynamicSliceOutput(0);

        for (int i = 0; i < multiVertexGeometry.getPointCount(); i++) {
            Point point = multiVertexGeometry.getPoint(i);

            long latitude = Math.round(point.getY() * 1e5);
            long longitude = Math.round(point.getX() * 1e5);

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

    private static void validateType(String function, OGCGeometry geometry, Set<GeometryType> validTypes)
    {
        GeometryType type = GeometryType.getForEsriGeometryType(geometry.geometryType());
        if (!validTypes.contains(type)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("%s only applies to %s. Input type is: %s", function, Joiner.on(" or ").join(validTypes), type));
        }
    }
}
