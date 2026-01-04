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
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.geospatial.GeometryType;
import io.trino.geospatial.KdbTree;
import io.trino.geospatial.Rectangle;
import io.trino.geospatial.serde.EsriShapeReader;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.io.kml.KMLReader;
import org.locationtech.jts.linearref.LengthIndexedLine;
import org.locationtech.jts.operation.distance.DistanceOp;
import org.locationtech.jts.operation.linemerge.LineMerger;
import org.locationtech.jts.operation.overlayng.OverlayNG;
import org.locationtech.jts.operation.overlayng.OverlayNGRobust;
import org.locationtech.jts.operation.relateng.RelateNG;
import org.locationtech.jts.operation.union.UnaryUnionOp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.geospatial.GeometryType.GEOMETRY_COLLECTION;
import static io.trino.geospatial.GeometryType.LINE_STRING;
import static io.trino.geospatial.GeometryType.MULTI_LINE_STRING;
import static io.trino.geospatial.GeometryType.MULTI_POINT;
import static io.trino.geospatial.GeometryType.MULTI_POLYGON;
import static io.trino.geospatial.GeometryType.POINT;
import static io.trino.geospatial.GeometryType.POLYGON;
import static io.trino.geospatial.GeometryUtils.jsonFromJtsGeometry;
import static io.trino.geospatial.GeometryUtils.jtsGeometryFromJson;
import static io.trino.geospatial.serde.JtsGeometrySerde.deserialize;
import static io.trino.geospatial.serde.JtsGeometrySerde.deserializeEnvelope;
import static io.trino.geospatial.serde.JtsGeometrySerde.deserializeType;
import static io.trino.geospatial.serde.JtsGeometrySerde.serialize;
import static io.trino.geospatial.serde.JtsGeometrySerde.serializeBinaryOp;
import static io.trino.geospatial.serde.JtsGeometrySerde.serializeWithSrid;
import static io.trino.geospatial.serde.JtsGeometrySerde.validateAndGetSrid;
import static io.trino.plugin.geospatial.GeometryType.GEOMETRY;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.BOOLEAN;
import static io.trino.spi.type.StandardTypes.DOUBLE;
import static io.trino.spi.type.StandardTypes.INTEGER;
import static io.trino.spi.type.StandardTypes.TINYINT;
import static io.trino.spi.type.StandardTypes.VARBINARY;
import static io.trino.spi.type.StandardTypes.VARCHAR;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;
import static java.lang.Math.PI;
import static java.lang.Math.atan2;
import static java.lang.Math.cos;
import static java.lang.Math.sin;
import static java.lang.Math.sqrt;
import static java.lang.Math.toIntExact;
import static java.lang.Math.toRadians;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.locationtech.jts.simplify.TopologyPreservingSimplifier.simplify;

public final class GeoFunctions
{
    private static final Joiner OR_JOINER = Joiner.on(" or ");
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private static final Slice EMPTY_POLYGON = serialize(GEOMETRY_FACTORY.createPolygon());
    private static final Slice EMPTY_MULTIPOINT = serialize(GEOMETRY_FACTORY.createMultiPoint());
    private static final double EARTH_RADIUS_KM = 6371.01;
    private static final double EARTH_RADIUS_M = EARTH_RADIUS_KM * 1000.0;
    private static final Block EMPTY_ARRAY_OF_INTS = IntegerType.INTEGER.createFixedSizeBlockBuilder(0).build();

    private static final float MIN_LATITUDE = -90;
    private static final float MAX_LATITUDE = 90;
    private static final float MIN_LONGITUDE = -180;
    private static final float MAX_LONGITUDE = 180;

    private static final int HADOOP_SHAPE_SIZE_WKID = 4;
    private static final int HADOOP_SHAPE_SIZE_TYPE = 1;

    private static final Set<String> VALID_SPHERICAL_GEOGRAPHY_LEAF_TYPES = Set.of(
            "Point", "LineString", "LinearRing", "Polygon");

    private static final EnumSet<GeometryType> VALID_TYPES_FOR_ST_POINTS = EnumSet.of(
            LINE_STRING, POLYGON, POINT, MULTI_POINT, MULTI_LINE_STRING, MULTI_POLYGON, GEOMETRY_COLLECTION);

    private GeoFunctions() {}

    @Description("Returns a Geometry type LineString object from Well-Known Text representation (WKT)")
    @ScalarFunction("ST_LineFromText")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice parseLine(@SqlType(VARCHAR) Slice input)
    {
        Geometry geometry = geometryFromText(input);
        validateType("ST_LineFromText", geometry, EnumSet.of(LINE_STRING));
        return serialize(geometry);
    }

    @Description("Returns a LineString from an array of points")
    @ScalarFunction("ST_LineString")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice stLineString(@SqlType("array(" + StandardTypes.GEOMETRY + ")") Block input)
    {
        List<Coordinate> coordinates = new ArrayList<>();
        Coordinate previousCoordinate = null;
        for (int i = 0; i < input.getPositionCount(); i++) {
            Slice slice = GEOMETRY.getSlice(input, i);

            if (slice.getInput().available() == 0) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Invalid input to ST_LineString: null point at index %s", i + 1));
            }

            Geometry geometry = deserialize(slice);
            if (!(geometry instanceof Point point)) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("ST_LineString takes only an array of valid points, %s was passed", geometry.getGeometryType()));
            }

            if (point.isEmpty()) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Invalid input to ST_LineString: empty point at index %s", i + 1));
            }

            Coordinate coordinate = point.getCoordinate();
            if (coordinate.equals(previousCoordinate)) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT,
                        format("Invalid input to ST_LineString: consecutive duplicate points at index %s", i + 1));
            }
            coordinates.add(coordinate);
            previousCoordinate = coordinate;
        }
        // A linestring needs 0 or >= 2 points; single point returns empty
        if (coordinates.size() == 1) {
            return serialize(GEOMETRY_FACTORY.createLineString());
        }
        return serialize(GEOMETRY_FACTORY.createLineString(coordinates.toArray(new Coordinate[0])));
    }

    @Description("Returns a Geometry type Point object with the given coordinate values")
    @ScalarFunction("ST_Point")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice stPoint(@SqlType(DOUBLE) double x, @SqlType(DOUBLE) double y)
    {
        return serialize(GEOMETRY_FACTORY.createPoint(new Coordinate(x, y)));
    }

    @SqlNullable
    @Description("Returns a multi-point geometry formed from input points")
    @ScalarFunction("ST_MultiPoint")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice stMultiPoint(@SqlType("array(" + StandardTypes.GEOMETRY + ")") Block input)
    {
        List<Point> points = new ArrayList<>();
        for (int i = 0; i < input.getPositionCount(); i++) {
            if (input.isNull(i)) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Invalid input to ST_MultiPoint: null at index %s", i + 1));
            }

            Slice slice = GEOMETRY.getSlice(input, i);
            Geometry geometry = deserialize(slice);
            if (!(geometry instanceof Point point)) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Invalid input to ST_MultiPoint: geometry is not a point: %s at index %s", geometry.getGeometryType(), i + 1));
            }
            if (point.isEmpty()) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Invalid input to ST_MultiPoint: empty point at index %s", i + 1));
            }

            points.add(point);
        }
        if (points.isEmpty()) {
            return null;
        }
        return serialize(GEOMETRY_FACTORY.createMultiPoint(points.toArray(new Point[0])));
    }

    @Description("Returns a Geometry type Polygon object from Well-Known Text representation (WKT)")
    @ScalarFunction("ST_Polygon")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice stPolygon(@SqlType(VARCHAR) Slice input)
    {
        Geometry geometry = geometryFromText(input);
        validateType("ST_Polygon", geometry, EnumSet.of(POLYGON));
        return serialize(geometry);
    }

    @Description("Returns the 2D Euclidean area of a geometry")
    @ScalarFunction("ST_Area")
    @SqlType(DOUBLE)
    public static double stArea(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        return deserialize(input).getArea();
    }

    @Description("Returns a Geometry type object from Well-Known Text representation (WKT)")
    @ScalarFunction("ST_GeometryFromText")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice stGeometryFromText(@SqlType(VARCHAR) Slice input)
    {
        return serialize(geometryFromText(input));
    }

    @Description("Returns a Geometry type object from Well-Known Binary representation (WKB or EWKB)")
    @ScalarFunction("ST_GeomFromBinary")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice stGeomFromBinary(@SqlType(VARBINARY) Slice input)
    {
        // Parse and re-serialize to ensure EWKB format
        // WKBReader handles both WKB (SRID=0) and EWKB (preserves SRID)
        try {
            return serialize(deserialize(input));
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
    }

    @Description("Returns the spatial reference identifier for the geometry")
    @ScalarFunction("ST_SRID")
    @SqlType(INTEGER)
    public static long stSrid(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        return deserialize(input).getSRID();
    }

    @Description("Sets the spatial reference identifier for the geometry")
    @ScalarFunction("ST_SetSRID")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice stSetSrid(@SqlType(StandardTypes.GEOMETRY) Slice input, @SqlType(INTEGER) long srid)
    {
        Geometry geometry = deserialize(input);
        geometry.setSRID(toIntExact(srid));
        return serialize(geometry);
    }

    @Description("Returns the Extended Well-Known Binary (EWKB) representation of the geometry")
    @ScalarFunction("ST_AsEWKB")
    @SqlType(VARBINARY)
    public static Slice stAsEwkb(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        // EWKB is our native format, no transformation needed
        return input;
    }

    @Description("Returns a Geometry type object from OGC KML representation")
    @ScalarFunction("ST_GeomFromKML")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice stGeomFromKML(@SqlType(VARCHAR) Slice input)
    {
        return serialize(geomFromKML(input));
    }

    @Description("Returns a Geometry type object from Spatial Framework for Hadoop representation")
    @ScalarFunction("geometry_from_hadoop_shape")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice geometryFromHadoopShape(@SqlType(VARBINARY) Slice input)
    {
        requireNonNull(input, "input is null");

        // Check minimum length (SRID + type + at least some shape data)
        int minOffset = HADOOP_SHAPE_SIZE_WKID + HADOOP_SHAPE_SIZE_TYPE;
        if (input.length() <= minOffset) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Hadoop shape input is too short");
        }

        // Validate OGC type (valid types are 0-6)
        byte hadoopShapeType = input.getByte(HADOOP_SHAPE_SIZE_WKID);
        if (hadoopShapeType < 0 || hadoopShapeType > 6) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid Hadoop shape type: " + hadoopShapeType);
        }

        try {
            Slice shapeSlice = input.slice(minOffset, input.length() - minOffset);

            // Peek at ESRI shape type to validate it matches the OGC type
            int esriShapeType = shapeSlice.getInt(0);  // peek at first 4 bytes
            validateShapeTypeMatch(hadoopShapeType, esriShapeType);

            Geometry geometry = EsriShapeReader.read(shapeSlice);

            // For empty geometries, use the OGC type to determine the correct Multi type
            // OGC types: 0=unknown, 1=point, 2=linestring, 3=polygon, 4=multipoint, 5=multilinestring, 6=multipolygon
            if (geometry.isEmpty()) {
                geometry = switch (hadoopShapeType) {
                    case 4 -> GEOMETRY_FACTORY.createMultiPoint();
                    case 5 -> GEOMETRY_FACTORY.createMultiLineString();
                    case 6 -> GEOMETRY_FACTORY.createMultiPolygon();
                    default -> geometry;
                };
            }

            return serialize(geometry);
        }
        catch (IndexOutOfBoundsException | UnsupportedOperationException | IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid Hadoop shape", e);
        }
    }

    @Description("Converts a Geometry object to a SphericalGeography object")
    @ScalarFunction("to_spherical_geography")
    @SqlType(StandardTypes.SPHERICAL_GEOGRAPHY)
    public static Slice toSphericalGeography(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        // "every point in input is in range" <=> "the envelope of input is in range"
        Envelope envelope = deserializeEnvelope(input);
        if (!envelope.isNull()) {
            checkLatitude(envelope.getMinY());
            checkLatitude(envelope.getMaxY());
            checkLongitude(envelope.getMinX());
            checkLongitude(envelope.getMaxX());
        }
        Geometry geometry = deserialize(input);

        // Check for 3D geometry
        for (Coordinate coord : geometry.getCoordinates()) {
            if (!isNaN(coord.getZ())) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Cannot convert 3D geometry to a spherical geography");
            }
        }

        // Validate geometry types
        validateSphericalGeographyTypes(geometry);

        return input;
    }

    private static void validateSphericalGeographyTypes(Geometry geometry)
    {
        // For collections (including MultiPoint, MultiLineString, MultiPolygon), recursively check each component
        if (geometry instanceof GeometryCollection gc) {
            for (int i = 0; i < gc.getNumGeometries(); i++) {
                validateSphericalGeographyTypes(gc.getGeometryN(i));
            }
        }
        else {
            // Leaf geometry types: Point, LineString, LinearRing, Polygon
            String type = geometry.getGeometryType();
            if (!VALID_SPHERICAL_GEOGRAPHY_LEAF_TYPES.contains(type)) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT,
                        "Cannot convert geometry of this type to spherical geography: " + type);
            }
        }
    }

    @Description("Converts a SphericalGeography object to a Geometry object.")
    @ScalarFunction("to_geometry")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice toGeometry(@SqlType(StandardTypes.SPHERICAL_GEOGRAPHY) Slice input)
    {
        // Every SphericalGeography object is a valid geometry object
        return input;
    }

    @Description("Returns the Well-Known Text (WKT) representation of the geometry")
    @ScalarFunction("ST_AsText")
    @SqlType(VARCHAR)
    public static Slice stAsText(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        return utf8Slice(new WKTWriter().write(deserialize(input)));
    }

    @Description("Returns the Extended Well-Known Text (EWKT) representation of the geometry, including SRID")
    @ScalarFunction("ST_AsEWKT")
    @SqlType(VARCHAR)
    public static Slice stAsEwkt(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Geometry geometry = deserialize(input);
        String wkt = new WKTWriter().write(geometry);
        int srid = geometry.getSRID();
        if (srid != 0) {
            return utf8Slice("SRID=" + srid + ";" + wkt);
        }
        return utf8Slice(wkt);
    }

    @Description("Returns the Well-Known Binary (WKB) representation of the geometry")
    @ScalarFunction("ST_AsBinary")
    @SqlType(VARBINARY)
    public static Slice stAsBinary(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        // Strip SRID for OGC WKB compatibility (external systems expect standard WKB)
        return Slices.wrappedBuffer(new WKBWriter(2, false).write(deserialize(input)));
    }

    @SqlNullable
    @Description("Returns the geometry that represents all points whose distance from the specified geometry is less than or equal to the specified distance")
    @ScalarFunction("ST_Buffer")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice stBuffer(@SqlType(StandardTypes.GEOMETRY) Slice input, @SqlType(DOUBLE) double distance)
    {
        if (isNaN(distance)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "distance is NaN");
        }

        if (distance < 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "distance is negative");
        }

        if (distance == 0) {
            return input;
        }

        Geometry geometry = deserialize(input);
        if (geometry.isEmpty()) {
            return null;
        }
        return serializeWithSrid(geometry.buffer(distance), geometry);
    }

    @Description("Returns the Point value that is the mathematical centroid of a Geometry")
    @ScalarFunction("ST_Centroid")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice stCentroid(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_Centroid", geometry, EnumSet.of(POINT, MULTI_POINT, LINE_STRING, MULTI_LINE_STRING, POLYGON, MULTI_POLYGON));
        GeometryType geometryType = GeometryType.getForJtsGeometryType(geometry.getGeometryType());
        if (geometryType == POINT) {
            return input;
        }

        if (geometry.isEmpty()) {
            return serializeWithSrid(geometry.getFactory().createPoint(), geometry);
        }

        return serializeWithSrid(geometry.getCentroid(), geometry);
    }

    @Description("Returns the minimum convex geometry that encloses all input geometries")
    @ScalarFunction("ST_ConvexHull")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice stConvexHull(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Geometry geometry = deserialize(input);
        if (geometry.isEmpty()) {
            return input;
        }
        if (GeometryType.getForJtsGeometryType(geometry.getGeometryType()) == POINT) {
            return input;
        }
        return serializeWithSrid(geometry.convexHull(), geometry);
    }

    @Description("Return the coordinate dimension of the Geometry")
    @ScalarFunction("ST_CoordDim")
    @SqlType(TINYINT)
    public static long stCoordinateDimension(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Geometry geometry = deserialize(input);
        Coordinate[] coordinates = geometry.getCoordinates();
        // Check if any coordinate has a valid Z value (non-NaN)
        for (Coordinate coordinate : coordinates) {
            if (!isNaN(coordinate.getZ())) {
                return 3;
            }
        }
        return 2;
    }

    @Description("Returns the inherent dimension of this Geometry object, which must be less than or equal to the coordinate dimension")
    @ScalarFunction("ST_Dimension")
    @SqlType(TINYINT)
    public static long stDimension(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        return deserialize(input).getDimension();
    }

    @SqlNullable
    @Description("Returns TRUE if the LineString or Multi-LineString's start and end points are coincident")
    @ScalarFunction("ST_IsClosed")
    @SqlType(BOOLEAN)
    public static Boolean stIsClosed(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_IsClosed", geometry, EnumSet.of(LINE_STRING, MULTI_LINE_STRING));
        if (geometry instanceof LineString lineString) {
            return lineString.isClosed();
        }
        org.locationtech.jts.geom.MultiLineString multiLineString = (org.locationtech.jts.geom.MultiLineString) geometry;
        for (int i = 0; i < multiLineString.getNumGeometries(); i++) {
            if (!((LineString) multiLineString.getGeometryN(i)).isClosed()) {
                return false;
            }
        }
        return true;
    }

    @SqlNullable
    @Description("Returns TRUE if this Geometry is an empty geometrycollection, polygon, point etc")
    @ScalarFunction("ST_IsEmpty")
    @SqlType(BOOLEAN)
    public static Boolean stIsEmpty(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        return deserializeEnvelope(input).isNull();
    }

    @Description("Returns TRUE if this Geometry has no anomalous geometric points, such as self intersection or self tangency")
    @ScalarFunction("ST_IsSimple")
    @SqlType(BOOLEAN)
    public static boolean stIsSimple(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Geometry geometry = deserialize(input);
        return geometry.isEmpty() || geometry.isSimple();
    }

    @Description("Returns true if the input geometry is well formed")
    @ScalarFunction("ST_IsValid")
    @SqlType(BOOLEAN)
    public static boolean stIsValid(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        return new org.locationtech.jts.operation.valid.IsValidOp(deserialize(input)).isValid();
    }

    @Description("Returns the reason for why the input geometry is not valid. Returns null if the input is valid.")
    @ScalarFunction("geometry_invalid_reason")
    @SqlType(VARCHAR)
    @SqlNullable
    public static Slice invalidReason(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Geometry geometry = deserialize(input);
        org.locationtech.jts.operation.valid.IsValidOp validOp = new org.locationtech.jts.operation.valid.IsValidOp(geometry);
        if (validOp.isValid()) {
            return null;
        }

        org.locationtech.jts.operation.valid.TopologyValidationError error = validOp.getValidationError();
        if (error == null) {
            return null;
        }

        Coordinate coordinate = error.getCoordinate();
        if (coordinate != null) {
            return utf8Slice(format("%s at or near (%s %s)", error.getMessage(), coordinate.getX(), coordinate.getY()));
        }
        return utf8Slice(error.getMessage());
    }

    @Description("Returns the length of a LineString or Multi-LineString using Euclidean measurement on a 2D plane (based on spatial ref) in projected units")
    @ScalarFunction("ST_Length")
    @SqlType(DOUBLE)
    public static double stLength(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_Length", geometry, EnumSet.of(LINE_STRING, MULTI_LINE_STRING));
        return geometry.getLength();
    }

    @SqlNullable
    @Description("Returns the great-circle length in meters of a linestring or multi-linestring on Earth's surface")
    @ScalarFunction("ST_Length")
    @SqlType(DOUBLE)
    public static Double stSphericalLength(@SqlType(StandardTypes.SPHERICAL_GEOGRAPHY) Slice input)
    {
        Geometry geometry = deserialize(input);
        if (geometry.isEmpty()) {
            return null;
        }

        validateSphericalType("ST_Length", geometry, EnumSet.of(LINE_STRING, MULTI_LINE_STRING));

        double sum = 0;

        // Handle both LineString and MultiLineString
        int numGeometries = geometry.getNumGeometries();
        for (int g = 0; g < numGeometries; g++) {
            LineString lineString = (LineString) geometry.getGeometryN(g);
            Coordinate[] coordinates = lineString.getCoordinates();
            if (coordinates.length < 2) {
                continue;
            }

            // sum up distances between adjacent points on this linestring
            for (int i = 1; i < coordinates.length; i++) {
                Coordinate previous = coordinates[i - 1];
                Coordinate next = coordinates[i];
                sum += greatCircleDistance(previous.getY(), previous.getX(), next.getY(), next.getX());
            }
        }

        return sum * 1000;
    }

    @SqlNullable
    @Description("Returns a float between 0 and 1 representing the location of the closest point on the LineString to the given Point, as a fraction of total 2d line length.")
    @ScalarFunction("line_locate_point")
    @SqlType(DOUBLE)
    public static Double lineLocatePoint(@SqlType(StandardTypes.GEOMETRY) Slice lineSlice, @SqlType(StandardTypes.GEOMETRY) Slice pointSlice)
    {
        Geometry line = deserialize(lineSlice);
        Geometry point = deserialize(pointSlice);

        if (line.isEmpty() || point.isEmpty()) {
            return null;
        }

        GeometryType lineType = GeometryType.getForJtsGeometryType(line.getGeometryType());
        if (lineType != LINE_STRING && lineType != MULTI_LINE_STRING) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("First argument to line_locate_point must be a LineString or a MultiLineString. Got: %s", line.getGeometryType()));
        }

        GeometryType pointType = GeometryType.getForJtsGeometryType(point.getGeometryType());
        if (pointType != POINT) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Second argument to line_locate_point must be a Point. Got: %s", point.getGeometryType()));
        }

        return new LengthIndexedLine(line).indexOf(point.getCoordinate()) / line.getLength();
    }

    @SqlNullable
    @Description("Returns a Point interpolated along a LineString at the fraction given.")
    @ScalarFunction("line_interpolate_point")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice lineInterpolatePoint(
            @SqlType(StandardTypes.GEOMETRY) Slice input,
            @SqlType(StandardTypes.DOUBLE) double distanceFraction)
    {
        Geometry geometry = deserialize(input);
        if (geometry.isEmpty()) {
            return null;
        }

        List<Point> interpolatedPoints = interpolatePoints(geometry, distanceFraction, false);
        return serialize(interpolatedPoints.getFirst());
    }

    @SqlNullable
    @Description("Returns an array of Points interpolated along a LineString.")
    @ScalarFunction("line_interpolate_points")
    @SqlType("array(" + StandardTypes.GEOMETRY + ")")
    public static Block lineInterpolatePoints(
            @SqlType(StandardTypes.GEOMETRY) Slice input,
            @SqlType(DOUBLE) double fractionStep)
    {
        Geometry geometry = deserialize(input);
        if (geometry.isEmpty()) {
            return null;
        }

        List<Point> interpolatedPoints = interpolatePoints(geometry, fractionStep, true);
        BlockBuilder blockBuilder = GEOMETRY.createBlockBuilder(null, interpolatedPoints.size());
        for (Point point : interpolatedPoints) {
            GEOMETRY.writeSlice(blockBuilder, serialize(point));
        }
        return blockBuilder.build();
    }

    private static List<Point> interpolatePoints(Geometry geometry, double fractionStep, boolean repeated)
    {
        validateType("line_interpolate_point", geometry, EnumSet.of(LINE_STRING));
        if (fractionStep < 0 || fractionStep > 1) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "fraction must be between 0 and 1");
        }

        LineString lineString = (LineString) geometry;
        LengthIndexedLine indexedLine = new LengthIndexedLine(lineString);
        double lineLength = lineString.getLength();

        if (fractionStep == 0) {
            return Collections.singletonList(lineString.getStartPoint());
        }
        if (fractionStep == 1) {
            return Collections.singletonList(lineString.getEndPoint());
        }

        int pointCount = repeated ? (int) Math.floor(1 / fractionStep) : 1;
        List<Point> interpolatedPoints = new ArrayList<>(pointCount);

        double currentFraction = fractionStep;
        while (interpolatedPoints.size() < pointCount) {
            Coordinate coord = indexedLine.extractPoint(currentFraction * lineLength);
            interpolatedPoints.add(GEOMETRY_FACTORY.createPoint(coord));
            currentFraction += fractionStep;
        }

        return interpolatedPoints;
    }

    @SqlNullable
    @Description("Returns X maxima of a bounding box of a Geometry")
    @ScalarFunction("ST_XMax")
    @SqlType(DOUBLE)
    public static Double stXMax(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Envelope envelope = deserializeEnvelope(input);
        if (envelope.isNull()) {
            return null;
        }
        return envelope.getMaxX();
    }

    @SqlNullable
    @Description("Returns Y maxima of a bounding box of a Geometry")
    @ScalarFunction("ST_YMax")
    @SqlType(DOUBLE)
    public static Double stYMax(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Envelope envelope = deserializeEnvelope(input);
        if (envelope.isNull()) {
            return null;
        }
        return envelope.getMaxY();
    }

    @SqlNullable
    @Description("Returns X minima of a bounding box of a Geometry")
    @ScalarFunction("ST_XMin")
    @SqlType(DOUBLE)
    public static Double stXMin(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Envelope envelope = deserializeEnvelope(input);
        if (envelope.isNull()) {
            return null;
        }
        return envelope.getMinX();
    }

    @SqlNullable
    @Description("Returns Y minima of a bounding box of a Geometry")
    @ScalarFunction("ST_YMin")
    @SqlType(DOUBLE)
    public static Double stYMin(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Envelope envelope = deserializeEnvelope(input);
        if (envelope.isNull()) {
            return null;
        }
        return envelope.getMinY();
    }

    @SqlNullable
    @Description("Returns the cardinality of the collection of interior rings of a polygon")
    @ScalarFunction("ST_NumInteriorRing")
    @SqlType(BIGINT)
    public static Long stNumInteriorRings(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_NumInteriorRing", geometry, EnumSet.of(POLYGON));
        if (geometry.isEmpty()) {
            return null;
        }
        return (long) ((org.locationtech.jts.geom.Polygon) geometry).getNumInteriorRing();
    }

    @SqlNullable
    @Description("Returns an array of interior rings of a polygon")
    @ScalarFunction("ST_InteriorRings")
    @SqlType("array(" + StandardTypes.GEOMETRY + ")")
    public static Block stInteriorRings(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_InteriorRings", geometry, EnumSet.of(POLYGON));
        if (geometry.isEmpty()) {
            return null;
        }

        org.locationtech.jts.geom.Polygon polygon = (org.locationtech.jts.geom.Polygon) geometry;
        BlockBuilder blockBuilder = GEOMETRY.createBlockBuilder(null, polygon.getNumInteriorRing());
        for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
            GEOMETRY.writeSlice(blockBuilder, serialize(polygon.getInteriorRingN(i)));
        }
        return blockBuilder.build();
    }

    @Description("Returns the cardinality of the geometry collection")
    @ScalarFunction("ST_NumGeometries")
    @SqlType(INTEGER)
    public static long stNumGeometries(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Geometry geometry = deserialize(input);
        if (geometry.isEmpty()) {
            return 0;
        }
        GeometryType type = GeometryType.getForJtsGeometryType(geometry.getGeometryType());
        if (!type.isMultitype()) {
            return 1;
        }
        return geometry.getNumGeometries();
    }

    @Description("Returns a geometry that represents the point set union of the input geometries.")
    @ScalarFunction("ST_Union")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice stUnion(@SqlType(StandardTypes.GEOMETRY) Slice left, @SqlType(StandardTypes.GEOMETRY) Slice right)
    {
        return stUnion(ImmutableList.of(left, right));
    }

    @Description("Returns a geometry that represents the point set union of the input geometries.")
    @ScalarFunction("geometry_union")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice geometryUnion(@SqlType("array(" + StandardTypes.GEOMETRY + ")") Block input)
    {
        return stUnion(getGeometrySlicesFromBlock(input));
    }

    private static Slice stUnion(Iterable<Slice> slices)
    {
        List<Geometry> geometries = new ArrayList<>();
        int expectedSrid = 0;
        for (Slice slice : slices) {
            // Ignore null inputs
            if (slice.getInput().available() == 0) {
                continue;
            }
            Geometry geometry = deserialize(slice);
            // Validate and track SRID
            int srid = geometry.getSRID();
            if (expectedSrid == 0) {
                expectedSrid = srid;
            }
            else if (srid != 0 && srid != expectedSrid) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT,
                        format("SRID mismatch: %d vs %d", expectedSrid, srid));
            }
            if (!geometry.isEmpty()) {
                // Flatten geometry collections to get individual geometries
                flattenGeometry(geometry, geometries);
            }
        }

        if (geometries.isEmpty()) {
            // Return empty geometry collection instead of null for empty inputs
            return serialize(GEOMETRY_FACTORY.createGeometryCollection());
        }

        // JTS UnaryUnionOp handles mixed dimensions properly
        Geometry result = UnaryUnionOp.union(geometries);

        // Post-process to match ESRI behavior:
        // 1. Merge connected line segments
        // 2. Reduce homogeneous geometry collections to Multi* types
        result = postProcessUnion(result);

        result.setSRID(expectedSrid);
        return serialize(result);
    }

    /**
     * Post-processes union result to match ESRI behavior:
     * 1. Merge connected line segments
     * 2. Reduce homogeneous geometry collections to Multi* types
     */
    private static Geometry postProcessUnion(Geometry geometry)
    {
        // Handle MultiLineString specially - merge connected lines
        if (geometry instanceof org.locationtech.jts.geom.MultiLineString mls) {
            LineMerger lineMerger = new LineMerger();
            lineMerger.add(mls);
            @SuppressWarnings("unchecked")
            Collection<LineString> merged = lineMerger.getMergedLineStrings();
            if (merged.size() == 1) {
                return merged.iterator().next();
            }
            return GEOMETRY_FACTORY.createMultiLineString(merged.toArray(new LineString[0]));
        }

        if (!(geometry instanceof GeometryCollection gc) ||
                geometry instanceof org.locationtech.jts.geom.MultiPoint ||
                geometry instanceof org.locationtech.jts.geom.MultiPolygon) {
            return geometry;
        }

        List<Point> points = new ArrayList<>();
        List<LineString> lineStrings = new ArrayList<>();
        List<org.locationtech.jts.geom.Polygon> polygons = new ArrayList<>();
        List<Geometry> others = new ArrayList<>();

        for (int i = 0; i < gc.getNumGeometries(); i++) {
            Geometry g = gc.getGeometryN(i);
            if (g instanceof Point p) {
                points.add(p);
            }
            else if (g instanceof LineString ls) {
                lineStrings.add(ls);
            }
            else if (g instanceof org.locationtech.jts.geom.Polygon p) {
                polygons.add(p);
            }
            else if (g instanceof org.locationtech.jts.geom.MultiLineString mls) {
                for (int j = 0; j < mls.getNumGeometries(); j++) {
                    lineStrings.add((LineString) mls.getGeometryN(j));
                }
            }
            else {
                others.add(g);
            }
        }

        List<Geometry> result = new ArrayList<>();

        // Merge line strings and add to result
        if (!lineStrings.isEmpty()) {
            LineMerger lineMerger = new LineMerger();
            lineStrings.forEach(lineMerger::add);
            @SuppressWarnings("unchecked")
            Collection<LineString> merged = lineMerger.getMergedLineStrings();
            if (merged.size() == 1) {
                result.add(merged.iterator().next());
            }
            else if (merged.size() > 1) {
                result.add(GEOMETRY_FACTORY.createMultiLineString(merged.toArray(new LineString[0])));
            }
        }

        // Reduce points to MultiPoint
        if (!points.isEmpty()) {
            if (points.size() == 1) {
                result.add(points.get(0));
            }
            else {
                result.add(GEOMETRY_FACTORY.createMultiPoint(points.toArray(new Point[0])));
            }
        }

        // Reduce polygons to MultiPolygon
        if (!polygons.isEmpty()) {
            if (polygons.size() == 1) {
                result.add(polygons.get(0));
            }
            else {
                result.add(GEOMETRY_FACTORY.createMultiPolygon(polygons.toArray(new org.locationtech.jts.geom.Polygon[0])));
            }
        }

        // Add any other geometry types
        result.addAll(others);

        if (result.size() == 1) {
            return result.get(0);
        }
        return GEOMETRY_FACTORY.createGeometryCollection(result.toArray(new Geometry[0]));
    }

    private static void flattenGeometry(Geometry geometry, List<Geometry> result)
    {
        if (geometry instanceof GeometryCollection gc) {
            for (int i = 0; i < gc.getNumGeometries(); i++) {
                flattenGeometry(gc.getGeometryN(i), result);
            }
        }
        else if (!geometry.isEmpty()) {
            result.add(geometry);
        }
    }

    @SqlNullable
    @Description("Returns the geometry element at the specified index (indices started with 1)")
    @ScalarFunction("ST_GeometryN")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice stGeometryN(@SqlType(StandardTypes.GEOMETRY) Slice input, @SqlType(INTEGER) long index)
    {
        Geometry geometry = deserialize(input);
        if (geometry.isEmpty()) {
            return null;
        }
        GeometryType type = GeometryType.getForJtsGeometryType(geometry.getGeometryType());
        if (!type.isMultitype()) {
            if (index == 1) {
                return input;
            }
            return null;
        }
        if (index < 1 || index > geometry.getNumGeometries()) {
            return null;
        }
        return serializeWithSrid(geometry.getGeometryN((int) index - 1), geometry);
    }

    @SqlNullable
    @Description("Returns the vertex of a linestring at the specified index (indices started with 1) ")
    @ScalarFunction("ST_PointN")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice stPointN(@SqlType(StandardTypes.GEOMETRY) Slice input, @SqlType(INTEGER) long index)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_PointN", geometry, EnumSet.of(LINE_STRING));

        LineString linestring = (LineString) geometry;
        if (index < 1 || index > linestring.getNumPoints()) {
            return null;
        }
        return serializeWithSrid(linestring.getPointN(toIntExact(index) - 1), geometry);
    }

    @SqlNullable
    @Description("Returns an array of geometries in the specified collection")
    @ScalarFunction("ST_Geometries")
    @SqlType("array(" + StandardTypes.GEOMETRY + ")")
    public static Block stGeometries(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Geometry geometry = deserialize(input);
        if (geometry.isEmpty()) {
            return null;
        }

        GeometryType type = GeometryType.getForJtsGeometryType(geometry.getGeometryType());
        if (!type.isMultitype()) {
            BlockBuilder blockBuilder = GEOMETRY.createBlockBuilder(null, 1);
            GEOMETRY.writeSlice(blockBuilder, serialize(geometry));
            return blockBuilder.build();
        }

        BlockBuilder blockBuilder = GEOMETRY.createBlockBuilder(null, geometry.getNumGeometries());
        for (int i = 0; i < geometry.getNumGeometries(); i++) {
            GEOMETRY.writeSlice(blockBuilder, serialize(geometry.getGeometryN(i)));
        }
        return blockBuilder.build();
    }

    @SqlNullable
    @Description("Returns the interior ring element at the specified index (indices start at 1)")
    @ScalarFunction("ST_InteriorRingN")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice stInteriorRingN(@SqlType(StandardTypes.GEOMETRY) Slice input, @SqlType(INTEGER) long index)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_InteriorRingN", geometry, EnumSet.of(POLYGON));
        org.locationtech.jts.geom.Polygon polygon = (org.locationtech.jts.geom.Polygon) geometry;
        if (index < 1 || index > polygon.getNumInteriorRing()) {
            return null;
        }
        Geometry interiorRing = polygon.getInteriorRingN(toIntExact(index) - 1);
        return serializeWithSrid(interiorRing, geometry);
    }

    @Description("Returns the number of points in a Geometry")
    @ScalarFunction("ST_NumPoints")
    @SqlType(BIGINT)
    public static long stNumPoints(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        return deserialize(input).getNumPoints();
    }

    @SqlNullable
    @Description("Returns TRUE if and only if the line is closed and simple")
    @ScalarFunction("ST_IsRing")
    @SqlType(BOOLEAN)
    public static Boolean stIsRing(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_IsRing", geometry, EnumSet.of(LINE_STRING));
        return ((LineString) geometry).isRing();
    }

    @SqlNullable
    @Description("Returns the first point of a LINESTRING geometry as a Point")
    @ScalarFunction("ST_StartPoint")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice stStartPoint(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_StartPoint", geometry, EnumSet.of(LINE_STRING));
        if (geometry.isEmpty()) {
            return null;
        }
        return serializeWithSrid(((LineString) geometry).getStartPoint(), geometry);
    }

    @Description("Returns a \"simplified\" version of the given geometry")
    @ScalarFunction("simplify_geometry")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice simplifyGeometry(@SqlType(StandardTypes.GEOMETRY) Slice input, @SqlType(DOUBLE) double distanceTolerance)
    {
        if (isNaN(distanceTolerance)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "distanceTolerance is NaN");
        }

        if (distanceTolerance < 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "distanceTolerance is negative");
        }

        if (distanceTolerance == 0) {
            return input;
        }

        Geometry geometry = deserialize(input);
        return serializeWithSrid(simplify(geometry, distanceTolerance), geometry);
    }

    @SqlNullable
    @Description("Returns the last point of a LINESTRING geometry as a Point")
    @ScalarFunction("ST_EndPoint")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice stEndPoint(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_EndPoint", geometry, EnumSet.of(LINE_STRING));
        if (geometry.isEmpty()) {
            return null;
        }
        return serializeWithSrid(((LineString) geometry).getEndPoint(), geometry);
    }

    @SqlNullable
    @Description("Returns an array of points in a geometry")
    @ScalarFunction("ST_Points")
    @SqlType("array(" + StandardTypes.GEOMETRY + ")")
    public static Block stPoints(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_Points", geometry, VALID_TYPES_FOR_ST_POINTS);
        if (geometry.isEmpty()) {
            return null;
        }

        int pointCount = geometry.getNumPoints();
        BlockBuilder blockBuilder = GEOMETRY.createBlockBuilder(null, pointCount);
        buildPointsBlock(geometry, blockBuilder);

        return blockBuilder.build();
    }

    private static void buildPointsBlock(Geometry geometry, BlockBuilder blockBuilder)
    {
        GeometryType type = GeometryType.getForJtsGeometryType(geometry.getGeometryType());
        if (type == POINT) {
            GEOMETRY.writeSlice(blockBuilder, serialize(geometry));
        }
        else if (type == GEOMETRY_COLLECTION) {
            GeometryCollection collection = (GeometryCollection) geometry;
            for (int i = 0; i < collection.getNumGeometries(); i++) {
                Geometry entry = collection.getGeometryN(i);
                buildPointsBlock(entry, blockBuilder);
            }
        }
        else {
            GeometryFactory geometryFactory = geometry.getFactory();
            Coordinate[] vertices = geometry.getCoordinates();
            for (Coordinate coordinate : vertices) {
                GEOMETRY.writeSlice(blockBuilder, serialize(geometryFactory.createPoint(coordinate)));
            }
        }
    }

    @SqlNullable
    @Description("Return the X coordinate of the point")
    @ScalarFunction("ST_X")
    @SqlType(DOUBLE)
    public static Double stX(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_X", geometry, EnumSet.of(POINT));
        if (geometry.isEmpty()) {
            return null;
        }
        return ((Point) geometry).getX();
    }

    @SqlNullable
    @Description("Return the Y coordinate of the point")
    @ScalarFunction("ST_Y")
    @SqlType(DOUBLE)
    public static Double stY(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_Y", geometry, EnumSet.of(POINT));
        if (geometry.isEmpty()) {
            return null;
        }
        return ((Point) geometry).getY();
    }

    @Description("Returns the closure of the combinatorial boundary of this Geometry")
    @ScalarFunction("ST_Boundary")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice stBoundary(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Geometry geometry = deserialize(input);
        return serializeWithSrid(geometry.getBoundary(), geometry);
    }

    @Description("Returns the bounding rectangular polygon of a Geometry")
    @ScalarFunction("ST_Envelope")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice stEnvelope(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Geometry geometry = deserialize(input);
        Envelope envelope = geometry.getEnvelopeInternal();
        if (envelope.isNull()) {
            return EMPTY_POLYGON;
        }
        Geometry envelopeGeometry = geometry.getFactory().toGeometry(envelope);
        return serializeWithSrid(envelopeGeometry, geometry);
    }

    @SqlNullable
    @Description("Returns the lower left and upper right corners of bounding rectangular polygon of a Geometry")
    @ScalarFunction("ST_EnvelopeAsPts")
    @SqlType("array(" + StandardTypes.GEOMETRY + ")")
    public static Block stEnvelopeAsPts(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Envelope envelope = deserializeEnvelope(input);
        if (envelope.isNull()) {
            return null;
        }
        BlockBuilder blockBuilder = GEOMETRY.createBlockBuilder(null, 2);
        Point lowerLeftCorner = GEOMETRY_FACTORY.createPoint(new Coordinate(envelope.getMinX(), envelope.getMinY()));
        Point upperRightCorner = GEOMETRY_FACTORY.createPoint(new Coordinate(envelope.getMaxX(), envelope.getMaxY()));
        GEOMETRY.writeSlice(blockBuilder, serialize(lowerLeftCorner));
        GEOMETRY.writeSlice(blockBuilder, serialize(upperRightCorner));
        return blockBuilder.build();
    }

    @Description("Returns the Geometry value that represents the point set difference of two geometries")
    @ScalarFunction("ST_Difference")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice stDifference(@SqlType(StandardTypes.GEOMETRY) Slice left, @SqlType(StandardTypes.GEOMETRY) Slice right)
    {
        Geometry leftGeometry = deserialize(left);
        Geometry rightGeometry = deserialize(right);
        // Use OverlayNGRobust for better handling of edge cases and invalid geometries
        Geometry result = OverlayNGRobust.overlay(leftGeometry, rightGeometry, OverlayNG.DIFFERENCE);
        return serializeBinaryOp(result, leftGeometry, rightGeometry);
    }

    @SqlNullable
    @Description("Returns the 2-dimensional cartesian minimum distance (based on spatial ref) between two geometries in projected units")
    @ScalarFunction("ST_Distance")
    @SqlType(DOUBLE)
    public static Double stDistance(@SqlType(StandardTypes.GEOMETRY) Slice left, @SqlType(StandardTypes.GEOMETRY) Slice right)
    {
        Geometry leftGeometry = deserialize(left);
        Geometry rightGeometry = deserialize(right);
        validateAndGetSrid(leftGeometry, rightGeometry);
        return leftGeometry.isEmpty() || rightGeometry.isEmpty() ? null : leftGeometry.distance(rightGeometry);
    }

    @SqlNullable
    @Description("Return the closest points on the two geometries")
    @ScalarFunction("geometry_nearest_points")
    @SqlType("row(" + StandardTypes.GEOMETRY + "," + StandardTypes.GEOMETRY + ")")
    public static SqlRow geometryNearestPoints(@SqlType(StandardTypes.GEOMETRY) Slice left, @SqlType(StandardTypes.GEOMETRY) Slice right)
    {
        Geometry leftGeometry = deserialize(left);
        Geometry rightGeometry = deserialize(right);
        int srid = validateAndGetSrid(leftGeometry, rightGeometry);
        if (leftGeometry.isEmpty() || rightGeometry.isEmpty()) {
            return null;
        }

        RowType rowType = RowType.anonymous(ImmutableList.of(GEOMETRY, GEOMETRY));
        GeometryFactory geometryFactory = leftGeometry.getFactory();
        Coordinate[] nearestCoordinates = DistanceOp.nearestPoints(leftGeometry, rightGeometry);

        return buildRowValue(rowType, fieldBuilders -> {
            Point point0 = geometryFactory.createPoint(nearestCoordinates[0]);
            point0.setSRID(srid);
            GEOMETRY.writeSlice(fieldBuilders.get(0), serialize(point0));
            Point point1 = geometryFactory.createPoint(nearestCoordinates[1]);
            point1.setSRID(srid);
            GEOMETRY.writeSlice(fieldBuilders.get(1), serialize(point1));
        });
    }

    @SqlNullable
    @Description("Returns a line string representing the exterior ring of the POLYGON")
    @ScalarFunction("ST_ExteriorRing")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice stExteriorRing(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_ExteriorRing", geometry, EnumSet.of(POLYGON));
        if (geometry.isEmpty()) {
            return null;
        }
        return serializeWithSrid(((org.locationtech.jts.geom.Polygon) geometry).getExteriorRing(), geometry);
    }

    @Description("Returns the Geometry value that represents the point set intersection of two Geometries")
    @ScalarFunction("ST_Intersection")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice stIntersection(@SqlType(StandardTypes.GEOMETRY) Slice left, @SqlType(StandardTypes.GEOMETRY) Slice right)
    {
        Geometry leftGeometry = deserialize(left);
        Geometry rightGeometry = deserialize(right);
        Geometry result = OverlayNGRobust.overlay(leftGeometry, rightGeometry, OverlayNG.INTERSECTION);
        return serializeBinaryOp(result, leftGeometry, rightGeometry);
    }

    @Description("Returns the Geometry value that represents the point set symmetric difference of two Geometries")
    @ScalarFunction("ST_SymDifference")
    @SqlType(StandardTypes.GEOMETRY)
    public static Slice stSymmetricDifference(@SqlType(StandardTypes.GEOMETRY) Slice left, @SqlType(StandardTypes.GEOMETRY) Slice right)
    {
        Geometry leftGeometry = deserialize(left);
        Geometry rightGeometry = deserialize(right);
        // Use OverlayNGRobust for better handling of edge cases and invalid geometries
        Geometry result = OverlayNGRobust.overlay(leftGeometry, rightGeometry, OverlayNG.SYMDIFFERENCE);
        return serializeBinaryOp(result, leftGeometry, rightGeometry);
    }

    @SqlNullable
    @Description("Returns TRUE if and only if no points of right lie in the exterior of left, and at least one point of the interior of left lies in the interior of right")
    @ScalarFunction("ST_Contains")
    @SqlType(BOOLEAN)
    public static Boolean stContains(@SqlType(StandardTypes.GEOMETRY) Slice left, @SqlType(StandardTypes.GEOMETRY) Slice right)
    {
        Geometry leftGeometry = deserialize(left);
        Geometry rightGeometry = deserialize(right);
        validateAndGetSrid(leftGeometry, rightGeometry);
        if (!leftGeometry.getEnvelopeInternal().contains(rightGeometry.getEnvelopeInternal())) {
            return false;
        }
        // Use RelateNG for better handling of edge cases and invalid geometries
        return RelateNG.relate(leftGeometry, rightGeometry).isContains();
    }

    @SqlNullable
    @Description("Returns TRUE if the supplied geometries have some, but not all, interior points in common")
    @ScalarFunction("ST_Crosses")
    @SqlType(BOOLEAN)
    public static Boolean stCrosses(@SqlType(StandardTypes.GEOMETRY) Slice left, @SqlType(StandardTypes.GEOMETRY) Slice right)
    {
        Geometry leftGeometry = deserialize(left);
        Geometry rightGeometry = deserialize(right);
        validateAndGetSrid(leftGeometry, rightGeometry);
        if (!leftGeometry.getEnvelopeInternal().intersects(rightGeometry.getEnvelopeInternal())) {
            return false;
        }
        // Use RelateNG for better handling of edge cases and invalid geometries
        return RelateNG.relate(leftGeometry, rightGeometry).isCrosses(leftGeometry.getDimension(), rightGeometry.getDimension());
    }

    @SqlNullable
    @Description("Returns TRUE if the Geometries do not spatially intersect - if they do not share any space together")
    @ScalarFunction("ST_Disjoint")
    @SqlType(BOOLEAN)
    public static Boolean stDisjoint(@SqlType(StandardTypes.GEOMETRY) Slice left, @SqlType(StandardTypes.GEOMETRY) Slice right)
    {
        Geometry leftGeometry = deserialize(left);
        Geometry rightGeometry = deserialize(right);
        validateAndGetSrid(leftGeometry, rightGeometry);
        if (!leftGeometry.getEnvelopeInternal().intersects(rightGeometry.getEnvelopeInternal())) {
            return true;
        }
        // Use RelateNG for better handling of edge cases and invalid geometries
        return RelateNG.relate(leftGeometry, rightGeometry).isDisjoint();
    }

    @SqlNullable
    @Description("Returns TRUE if the given geometries represent the same geometry")
    @ScalarFunction("ST_Equals")
    @SqlType(BOOLEAN)
    public static Boolean stEquals(@SqlType(StandardTypes.GEOMETRY) Slice left, @SqlType(StandardTypes.GEOMETRY) Slice right)
    {
        Geometry leftGeometry = deserialize(left);
        Geometry rightGeometry = deserialize(right);
        validateAndGetSrid(leftGeometry, rightGeometry);
        // Use RelateNG for better handling of edge cases and invalid geometries
        return RelateNG.relate(leftGeometry, rightGeometry).isEquals(leftGeometry.getDimension(), rightGeometry.getDimension());
    }

    @SqlNullable
    @Description("Returns TRUE if the Geometries spatially intersect in 2D - (share any portion of space) and FALSE if they don't (they are Disjoint)")
    @ScalarFunction("ST_Intersects")
    @SqlType(BOOLEAN)
    public static Boolean stIntersects(@SqlType(StandardTypes.GEOMETRY) Slice left, @SqlType(StandardTypes.GEOMETRY) Slice right)
    {
        Geometry leftGeometry = deserialize(left);
        Geometry rightGeometry = deserialize(right);
        validateAndGetSrid(leftGeometry, rightGeometry);
        if (!leftGeometry.getEnvelopeInternal().intersects(rightGeometry.getEnvelopeInternal())) {
            return false;
        }
        // Use RelateNG for better handling of edge cases and invalid geometries
        return RelateNG.relate(leftGeometry, rightGeometry).isIntersects();
    }

    @SqlNullable
    @Description("Returns TRUE if the Geometries share space, are of the same dimension, but are not completely contained by each other")
    @ScalarFunction("ST_Overlaps")
    @SqlType(BOOLEAN)
    public static Boolean stOverlaps(@SqlType(StandardTypes.GEOMETRY) Slice left, @SqlType(StandardTypes.GEOMETRY) Slice right)
    {
        Geometry leftGeometry = deserialize(left);
        Geometry rightGeometry = deserialize(right);
        validateAndGetSrid(leftGeometry, rightGeometry);
        if (!leftGeometry.getEnvelopeInternal().intersects(rightGeometry.getEnvelopeInternal())) {
            return false;
        }
        // Use RelateNG for better handling of edge cases and invalid geometries
        return RelateNG.relate(leftGeometry, rightGeometry).isOverlaps(leftGeometry.getDimension(), rightGeometry.getDimension());
    }

    @SqlNullable
    @Description("Returns TRUE if this Geometry is spatially related to another Geometry")
    @ScalarFunction("ST_Relate")
    @SqlType(BOOLEAN)
    public static Boolean stRelate(@SqlType(StandardTypes.GEOMETRY) Slice left, @SqlType(StandardTypes.GEOMETRY) Slice right, @SqlType(VARCHAR) Slice relation)
    {
        Geometry leftGeometry = deserialize(left);
        Geometry rightGeometry = deserialize(right);
        validateAndGetSrid(leftGeometry, rightGeometry);
        // Use RelateNG for better handling of edge cases and invalid geometries
        return RelateNG.relate(leftGeometry, rightGeometry, relation.toStringUtf8());
    }

    @SqlNullable
    @Description("Returns TRUE if the geometries have at least one point in common, but their interiors do not intersect")
    @ScalarFunction("ST_Touches")
    @SqlType(BOOLEAN)
    public static Boolean stTouches(@SqlType(StandardTypes.GEOMETRY) Slice left, @SqlType(StandardTypes.GEOMETRY) Slice right)
    {
        Geometry leftGeometry = deserialize(left);
        Geometry rightGeometry = deserialize(right);
        validateAndGetSrid(leftGeometry, rightGeometry);
        if (!leftGeometry.getEnvelopeInternal().intersects(rightGeometry.getEnvelopeInternal())) {
            return false;
        }
        // Use RelateNG for better handling of edge cases and invalid geometries
        return RelateNG.relate(leftGeometry, rightGeometry).isTouches(leftGeometry.getDimension(), rightGeometry.getDimension());
    }

    @SqlNullable
    @Description("Returns TRUE if the geometry A is completely inside geometry B")
    @ScalarFunction("ST_Within")
    @SqlType(BOOLEAN)
    public static Boolean stWithin(@SqlType(StandardTypes.GEOMETRY) Slice left, @SqlType(StandardTypes.GEOMETRY) Slice right)
    {
        Geometry leftGeometry = deserialize(left);
        Geometry rightGeometry = deserialize(right);
        validateAndGetSrid(leftGeometry, rightGeometry);
        if (!rightGeometry.getEnvelopeInternal().contains(leftGeometry.getEnvelopeInternal())) {
            return false;
        }
        // Use RelateNG for better handling of edge cases and invalid geometries
        return RelateNG.relate(leftGeometry, rightGeometry).isWithin();
    }

    @Description("Returns the type of the geometry")
    @ScalarFunction("ST_GeometryType")
    @SqlType(VARCHAR)
    public static Slice stGeometryType(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        return deserializeType(input).standardName();
    }

    @ScalarFunction
    @SqlNullable
    @Description("Returns an array of spatial partition IDs for a given geometry")
    @SqlType("array(integer)")
    public static Block spatialPartitions(@SqlType(StandardTypes.KDB_TREE) Object kdbTree, @SqlType(StandardTypes.GEOMETRY) Slice geometry)
    {
        Envelope envelope = deserializeEnvelope(geometry);
        if (envelope.isNull()) {
            // Empty geometry
            return null;
        }

        return spatialPartitions((KdbTree) kdbTree, new Rectangle(envelope.getMinX(), envelope.getMinY(), envelope.getMaxX(), envelope.getMaxY()));
    }

    @ScalarFunction("from_geojson_geometry")
    @Description("Returns a spherical geography from a GeoJSON string")
    @SqlType(StandardTypes.SPHERICAL_GEOGRAPHY)
    public static Slice fromGeoJsonGeometry(@SqlType(VARCHAR) Slice input)
    {
        return serialize(jtsGeometryFromJson(input.toStringUtf8()));
    }

    @SqlNullable
    @ScalarFunction("to_geojson_geometry")
    @Description("Returns GeoJSON string based on the input spherical geography")
    @SqlType(VARCHAR)
    public static Slice geographyToGeoJson(@SqlType(StandardTypes.SPHERICAL_GEOGRAPHY) Slice input)
    {
        return utf8Slice(jsonFromJtsGeometry(deserialize(input)));
    }

    @SqlNullable
    @ScalarFunction("to_geojson_geometry")
    @Description("Returns GeoJSON string based on the input geometry")
    @SqlType(VARCHAR)
    public static Slice geometryToGeoJson(@SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        return utf8Slice(jsonFromJtsGeometry(deserialize(input)));
    }

    @ScalarFunction
    @SqlNullable
    @Description("Returns an array of spatial partition IDs for a geometry representing a set of points within specified distance from the input geometry")
    @SqlType("array(integer)")
    public static Block spatialPartitions(@SqlType(StandardTypes.KDB_TREE) Object kdbTree, @SqlType(StandardTypes.GEOMETRY) Slice geometry, @SqlType(DOUBLE) double distance)
    {
        if (isNaN(distance)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "distance is NaN");
        }

        if (isInfinite(distance)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "distance is infinite");
        }

        if (distance < 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "distance is negative");
        }

        Envelope envelope = deserializeEnvelope(geometry);
        if (envelope.isNull()) {
            return null;
        }

        Rectangle expandedEnvelope2D = new Rectangle(envelope.getMinX() - distance, envelope.getMinY() - distance, envelope.getMaxX() + distance, envelope.getMaxY() + distance);
        return spatialPartitions((KdbTree) kdbTree, expandedEnvelope2D);
    }

    private static Block spatialPartitions(KdbTree kdbTree, Rectangle envelope)
    {
        Map<Integer, Rectangle> partitions = kdbTree.findIntersectingLeaves(envelope);
        if (partitions.isEmpty()) {
            return EMPTY_ARRAY_OF_INTS;
        }

        // For input rectangles that represent a single point, return at most one partition
        // by excluding right and upper sides of partition rectangles. The logic that builds
        // KDB tree needs to make sure to add some padding to the right and upper sides of the
        // overall extent of the tree to avoid missing right-most and top-most points.
        boolean point = (envelope.getWidth() == 0 && envelope.getHeight() == 0);
        if (point) {
            for (Map.Entry<Integer, Rectangle> partition : partitions.entrySet()) {
                if (envelope.getXMin() < partition.getValue().getXMax() && envelope.getYMin() < partition.getValue().getYMax()) {
                    BlockBuilder blockBuilder = IntegerType.INTEGER.createFixedSizeBlockBuilder(1);
                    IntegerType.INTEGER.writeInt(blockBuilder, partition.getKey());
                    return blockBuilder.build();
                }
            }
            throw new VerifyException(format("Cannot find half-open partition extent for a point: (%s, %s)", envelope.getXMin(), envelope.getYMin()));
        }

        BlockBuilder blockBuilder = IntegerType.INTEGER.createFixedSizeBlockBuilder(partitions.size());
        for (int id : partitions.keySet()) {
            IntegerType.INTEGER.writeInt(blockBuilder, id);
        }

        return blockBuilder.build();
    }

    @ScalarFunction
    @Description("Calculates the great-circle distance between two points on the Earth's surface in kilometers")
    @SqlType(DOUBLE)
    public static double greatCircleDistance(
            @SqlType(DOUBLE) double latitude1,
            @SqlType(DOUBLE) double longitude1,
            @SqlType(DOUBLE) double latitude2,
            @SqlType(DOUBLE) double longitude2)
    {
        checkLatitude(latitude1);
        checkLongitude(longitude1);
        checkLatitude(latitude2);
        checkLongitude(longitude2);

        double radianLatitude1 = toRadians(latitude1);
        double radianLatitude2 = toRadians(latitude2);

        double sin1 = sin(radianLatitude1);
        double cos1 = cos(radianLatitude1);
        double sin2 = sin(radianLatitude2);
        double cos2 = cos(radianLatitude2);

        double deltaLongitude = toRadians(longitude1) - toRadians(longitude2);
        double cosDeltaLongitude = cos(deltaLongitude);

        double t1 = cos2 * sin(deltaLongitude);
        double t2 = cos1 * sin2 - sin1 * cos2 * cosDeltaLongitude;
        double t3 = sin1 * sin2 + cos1 * cos2 * cosDeltaLongitude;
        return atan2(sqrt(t1 * t1 + t2 * t2), t3) * EARTH_RADIUS_KM;
    }

    private static void checkLatitude(double latitude)
    {
        if (isNaN(latitude) || isInfinite(latitude) || latitude < MIN_LATITUDE || latitude > MAX_LATITUDE) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Latitude must be between -90 and 90");
        }
    }

    private static void checkLongitude(double longitude)
    {
        if (isNaN(longitude) || isInfinite(longitude) || longitude < MIN_LONGITUDE || longitude > MAX_LONGITUDE) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Longitude must be between -180 and 180");
        }
    }

    private static Geometry geometryFromText(Slice input)
    {
        try {
            return new WKTReader().read(input.toStringUtf8());
        }
        catch (ParseException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid WKT: " + input.toStringUtf8(), e);
        }
    }

    private static Geometry geomFromKML(Slice input)
    {
        try {
            return new KMLReader().read(input.toStringUtf8());
        }
        catch (ParseException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid KML: " + input.toStringUtf8(), e);
        }
    }

    private static void validateType(String function, Geometry geometry, Set<GeometryType> validTypes)
    {
        GeometryType type = GeometryType.getForJtsGeometryType(geometry.getGeometryType());
        if (!validTypes.contains(type)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("%s only applies to %s. Input type is: %s", function, OR_JOINER.join(validTypes), type));
        }
    }

    /**
     * Validates that the ESRI shape type matches the expected OGC type from the Hadoop format.
     * OGC types: 0=unknown, 1=point, 2=linestring, 3=polygon, 4=multipoint, 5=multilinestring, 6=multipolygon
     * ESRI shape types: 0=null, 1=point, 3=polyline, 5=polygon, 8=multipoint
     */
    private static void validateShapeTypeMatch(byte ogcType, int esriShapeType)
    {
        // Map OGC type to expected ESRI shape type(s)
        int expectedEsriType = switch (ogcType) {
            case 0 -> esriShapeType; // Unknown - accept any
            case 1 -> 1;  // Point -> Point
            case 2 -> 3;  // LineString -> Polyline
            case 3 -> 5;  // Polygon -> Polygon
            case 4 -> 8;  // MultiPoint -> MultiPoint
            case 5 -> 3;  // MultiLineString -> Polyline
            case 6 -> 5;  // MultiPolygon -> Polygon
            default -> -1; // Invalid
        };

        // Allow null shape (0) for any type as it represents empty geometry
        if (esriShapeType != 0 && esriShapeType != expectedEsriType) {
            throw new IllegalArgumentException("ESRI shape type " + esriShapeType + " does not match OGC type " + ogcType);
        }
    }

    private static boolean envelopes(Slice left, Slice right, EnvelopesPredicate predicate)
    {
        Envelope leftEnvelope = deserializeEnvelope(left);
        Envelope rightEnvelope = deserializeEnvelope(right);
        if (leftEnvelope.isNull() || rightEnvelope.isNull()) {
            return false;
        }
        return predicate.apply(leftEnvelope, rightEnvelope);
    }

    private interface EnvelopesPredicate
    {
        boolean apply(Envelope left, Envelope right);
    }

    @SqlNullable
    @Description("Returns the great-circle distance in meters between two SphericalGeography points.")
    @ScalarFunction("ST_Distance")
    @SqlType(DOUBLE)
    public static Double stSphericalDistance(@SqlType(StandardTypes.SPHERICAL_GEOGRAPHY) Slice left, @SqlType(StandardTypes.SPHERICAL_GEOGRAPHY) Slice right)
    {
        Geometry leftGeometry = deserialize(left);
        Geometry rightGeometry = deserialize(right);
        if (leftGeometry.isEmpty() || rightGeometry.isEmpty()) {
            return null;
        }

        // TODO: support more SphericalGeography types.
        validateSphericalType("ST_Distance", leftGeometry, EnumSet.of(POINT));
        validateSphericalType("ST_Distance", rightGeometry, EnumSet.of(POINT));
        Point leftPoint = (Point) leftGeometry;
        Point rightPoint = (Point) rightGeometry;

        // greatCircleDistance returns distance in KM.
        return greatCircleDistance(leftPoint.getY(), leftPoint.getX(), rightPoint.getY(), rightPoint.getX()) * 1000;
    }

    private static void validateSphericalType(String function, Geometry geometry, Set<GeometryType> validTypes)
    {
        GeometryType type = GeometryType.getForJtsGeometryType(geometry.getGeometryType());
        if (!validTypes.contains(type)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("When applied to SphericalGeography inputs, %s only supports %s. Input type is: %s", function, OR_JOINER.join(validTypes), type));
        }
    }

    @SqlNullable
    @Description("Returns the area of a geometry on the Earth's surface using spherical model")
    @ScalarFunction("ST_Area")
    @SqlType(DOUBLE)
    public static Double stSphericalArea(@SqlType(StandardTypes.SPHERICAL_GEOGRAPHY) Slice input)
    {
        Geometry geometry = deserialize(input);
        if (geometry.isEmpty()) {
            return null;
        }

        validateSphericalType("ST_Area", geometry, EnumSet.of(POLYGON, MULTI_POLYGON));

        // See https://www.movable-type.co.uk/scripts/latlong.html
        // and http://osgeo-org.1560.x6.nabble.com/Area-of-a-spherical-polygon-td3841625.html
        // and https://www.element84.com/blog/determining-if-a-spherical-polygon-contains-a-pole
        // for the underlying Maths

        double sphericalExcess = 0.0;

        // Handle both Polygon and MultiPolygon
        int numPolygons = geometry.getNumGeometries();
        for (int p = 0; p < numPolygons; p++) {
            org.locationtech.jts.geom.Polygon polygon = (org.locationtech.jts.geom.Polygon) geometry.getGeometryN(p);

            // Exterior ring (positive contribution)
            sphericalExcess += Math.abs(computeSphericalExcess(polygon.getExteriorRing().getCoordinates()));

            // Interior rings (negative contribution - holes)
            for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
                sphericalExcess -= Math.abs(computeSphericalExcess(polygon.getInteriorRingN(i).getCoordinates()));
            }
        }

        // Math.abs is required here because for Polygons with a 2D area of 0
        // isExteriorRing returns false for the exterior ring
        return Math.abs(sphericalExcess * EARTH_RADIUS_M * EARTH_RADIUS_M);
    }

    private static double computeSphericalExcess(Coordinate[] coordinates)
    {
        int end = coordinates.length;
        int start = 0;

        // Our calculations rely on not processing the same point twice
        if (coordinates[end - 1].equals(coordinates[start])) {
            end = end - 1;
        }

        if (end - start < 3) {
            // A path with less than 3 distinct points is not valid for calculating an area
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Polygon is not valid: a loop contains less then 3 vertices.");
        }

        // Initialize the calculator with the last point
        Coordinate lastPoint = coordinates[end - 1];

        double sphericalExcess = 0;
        double courseDelta = 0;
        boolean firstPoint = true;
        double firstInitialBearing = 0;
        double previousFinalBearing = 0;
        double previousPhi = toRadians(lastPoint.getY());
        double previousCos = cos(previousPhi);
        double previousSin = sin(previousPhi);
        double previousTan = Math.tan(previousPhi / 2);
        double previousLongitude = toRadians(lastPoint.getX());

        for (int i = start; i < end; i++) {
            Coordinate point = coordinates[i];
            double phi = toRadians(point.getY());
            double tan = Math.tan(phi / 2);
            double longitude = toRadians(point.getX());

            // We need to check for that specifically
            // Otherwise calculating the bearing is not deterministic
            if (longitude == previousLongitude && phi == previousPhi) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Polygon is not valid: it has two identical consecutive vertices");
            }

            double deltaLongitude = longitude - previousLongitude;
            sphericalExcess += 2 * atan2(Math.tan(deltaLongitude / 2) * (previousTan + tan), 1 + previousTan * tan);

            double cos = cos(phi);
            double sin = sin(phi);
            double sinOfDeltaLongitude = sin(deltaLongitude);
            double cosOfDeltaLongitude = cos(deltaLongitude);

            // Initial bearing from previous to current
            double y = sinOfDeltaLongitude * cos;
            double x = previousCos * sin - previousSin * cos * cosOfDeltaLongitude;
            double initialBearing = (atan2(y, x) + 2 * PI) % (2 * PI);

            // Final bearing from previous to current = opposite of bearing from current to previous
            double finalY = -sinOfDeltaLongitude * previousCos;
            double finalX = previousSin * cos - previousCos * sin * cosOfDeltaLongitude;
            double finalBearing = (atan2(finalY, finalX) + PI) % (2 * PI);

            // When processing our first point we don't yet have a previousFinalBearing
            if (firstPoint) {
                // So keep our initial bearing around, and we'll use it at the end
                // with the last final bearing
                firstInitialBearing = initialBearing;
                firstPoint = false;
            }
            else {
                courseDelta += (initialBearing - previousFinalBearing + 3 * PI) % (2 * PI) - PI;
            }

            courseDelta += (finalBearing - initialBearing + 3 * PI) % (2 * PI) - PI;

            previousFinalBearing = finalBearing;
            previousCos = cos;
            previousSin = sin;
            previousPhi = phi;
            previousTan = tan;
            previousLongitude = longitude;
        }

        // Now that we have our last final bearing, we can calculate the remaining course delta
        courseDelta += (firstInitialBearing - previousFinalBearing + 3 * PI) % (2 * PI) - PI;

        // The courseDelta should be 2Pi or - 2Pi, unless a pole is enclosed (and then it should be ~ 0)
        // In which case we need to correct the spherical excess by 2Pi
        if (Math.abs(courseDelta) < PI / 4) {
            sphericalExcess = Math.abs(sphericalExcess) - 2 * PI;
        }

        return sphericalExcess;
    }

    private static Iterable<Slice> getGeometrySlicesFromBlock(Block block)
    {
        requireNonNull(block, "block is null");
        return () -> new Iterator<>()
        {
            private int iteratorPosition;

            @Override
            public boolean hasNext()
            {
                return iteratorPosition != block.getPositionCount();
            }

            @Override
            public Slice next()
            {
                if (!hasNext()) {
                    throw new NoSuchElementException("Slices have been consumed");
                }
                return GEOMETRY.getSlice(block, iteratorPosition++);
            }
        };
    }
}
