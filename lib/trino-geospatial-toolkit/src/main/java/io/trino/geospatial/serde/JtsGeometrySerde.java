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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.geospatial.GeometryType;
import io.trino.spi.TrinoException;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Serializes JTS Geometry objects to/from EWKB (Extended Well-Known Binary) format.
 * EWKB is the PostGIS extension that includes SRID in the binary format.
 */
public final class JtsGeometrySerde
{
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    public static final int OGC_CRS84_SRID = 4326;

    // EWKB flag for SRID presence (bit 29)
    private static final int EWKB_Z_FLAG = 0x80000000;
    private static final int EWKB_SRID_FLAG = 0x20000000;
    private static final int EWKB_M_FLAG = 0x40000000;
    private static final String UNSUPPORTED_MEASURE_DIMENSION_MESSAGE = "Geospatial values with measure coordinates are not supported";

    // WKB type codes (2D)
    private static final int WKB_POINT = 1;
    private static final int WKB_LINE_STRING = 2;
    private static final int WKB_POLYGON = 3;
    private static final int WKB_MULTI_POINT = 4;
    private static final int WKB_MULTI_LINE_STRING = 5;
    private static final int WKB_MULTI_POLYGON = 6;
    private static final int WKB_GEOMETRY_COLLECTION = 7;

    private JtsGeometrySerde() {}

    /**
     * Deserialize WKB bytes to a JTS Geometry.
     */
    public static Geometry deserialize(Slice shape)
    {
        requireNonNull(shape, "shape is null");
        verify(shape.length() > 0, "shape is empty");
        try {
            return new WKBReader(GEOMETRY_FACTORY).read(shape.getBytes());
        }
        catch (ParseException e) {
            throw new IllegalArgumentException("Invalid WKB", e);
        }
    }

    /**
     * Serialize a JTS Geometry to EWKB bytes (Extended WKB with SRID).
     */
    public static Slice serialize(Geometry geometry)
    {
        return serialize(geometry, true);
    }

    /**
     * Serialize a JTS Geometry to WKB bytes without SRID, preserving Z coordinates when present.
     */
    public static Slice serializeWithoutSrid(Geometry geometry)
    {
        return serialize(geometry, false);
    }

    /**
     * Serialize a JTS Geometry to WKB/EWKB bytes, preserving Z coordinates when present.
     */
    private static Slice serialize(Geometry geometry, boolean includeSrid)
    {
        requireNonNull(geometry, "geometry is null");
        verifySupportedCoordinateDimensions(geometry);
        // WKBWriter(outputDimension, includeSRID)
        boolean hasZ = hasZ(geometry);
        byte[] bytes = new WKBWriter(hasZ ? 3 : 2, includeSrid).write(geometry);
        if (hasZ && !includeSrid) {
            convertEwkbZToIsoWkb(bytes, 0);
        }
        return Slices.wrappedBuffer(bytes);
    }

    private static int convertEwkbZToIsoWkb(byte[] bytes, int offset)
    {
        boolean bigEndian = isBigEndian(bytes[offset]);
        int type = readInt(bytes, offset + 1, bigEndian);
        verify((type & EWKB_Z_FLAG) != 0, "expected EWKB Z type: %s", type);
        verify((type & (EWKB_M_FLAG | EWKB_SRID_FLAG)) == 0, "unexpected EWKB flags: %s", type);

        int baseType = type & ~EWKB_Z_FLAG;
        writeInt(bytes, offset + 1, baseType + 1000, bigEndian);
        int position = offset + 5;
        int coordinateSize = 3 * Double.BYTES;

        return switch (baseType) {
            case WKB_POINT -> position + coordinateSize;
            case WKB_LINE_STRING -> {
                int pointCount = readInt(bytes, position, bigEndian);
                yield position + Integer.BYTES + pointCount * coordinateSize;
            }
            case WKB_POLYGON -> {
                int ringCount = readInt(bytes, position, bigEndian);
                position += Integer.BYTES;
                for (int ring = 0; ring < ringCount; ring++) {
                    int pointCount = readInt(bytes, position, bigEndian);
                    position += Integer.BYTES + pointCount * coordinateSize;
                }
                yield position;
            }
            case WKB_MULTI_POINT, WKB_MULTI_LINE_STRING, WKB_MULTI_POLYGON, WKB_GEOMETRY_COLLECTION -> {
                int geometryCount = readInt(bytes, position, bigEndian);
                position += Integer.BYTES;
                for (int geometry = 0; geometry < geometryCount; geometry++) {
                    position = convertEwkbZToIsoWkb(bytes, position);
                }
                yield position;
            }
            default -> throw new IllegalArgumentException("Unknown WKB type: " + baseType);
        };
    }

    private static int readInt(byte[] bytes, int offset, boolean bigEndian)
    {
        if (bigEndian) {
            return ((bytes[offset] & 0xFF) << 24) |
                    ((bytes[offset + 1] & 0xFF) << 16) |
                    ((bytes[offset + 2] & 0xFF) << 8) |
                    (bytes[offset + 3] & 0xFF);
        }
        return (bytes[offset] & 0xFF) |
                ((bytes[offset + 1] & 0xFF) << 8) |
                ((bytes[offset + 2] & 0xFF) << 16) |
                ((bytes[offset + 3] & 0xFF) << 24);
    }

    private static void writeInt(byte[] bytes, int offset, int value, boolean bigEndian)
    {
        if (bigEndian) {
            bytes[offset] = (byte) (value >>> 24);
            bytes[offset + 1] = (byte) (value >>> 16);
            bytes[offset + 2] = (byte) (value >>> 8);
            bytes[offset + 3] = (byte) value;
            return;
        }
        bytes[offset] = (byte) value;
        bytes[offset + 1] = (byte) (value >>> 8);
        bytes[offset + 2] = (byte) (value >>> 16);
        bytes[offset + 3] = (byte) (value >>> 24);
    }

    private static void verifySupportedCoordinateDimensions(Geometry geometry)
    {
        requireNonNull(geometry, "geometry is null");
        if (geometry instanceof Point point) {
            verifySupportedCoordinateDimensions(point.getCoordinateSequence());
            return;
        }
        if (geometry instanceof LineString lineString) {
            verifySupportedCoordinateDimensions(lineString.getCoordinateSequence());
            return;
        }
        if (geometry instanceof Polygon polygon) {
            verifySupportedCoordinateDimensions(polygon.getExteriorRing().getCoordinateSequence());
            for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
                verifySupportedCoordinateDimensions(polygon.getInteriorRingN(i).getCoordinateSequence());
            }
            return;
        }
        if (geometry instanceof GeometryCollection geometryCollection) {
            for (int i = 0; i < geometryCollection.getNumGeometries(); i++) {
                verifySupportedCoordinateDimensions(geometryCollection.getGeometryN(i));
            }
        }
    }

    private static void verifySupportedCoordinateDimensions(CoordinateSequence coordinates)
    {
        if (coordinates.hasM()) {
            throw unsupportedCoordinateDimensions();
        }
    }

    public static boolean hasZ(Geometry geometry)
    {
        requireNonNull(geometry, "geometry is null");
        if (geometry instanceof Point point) {
            return hasZ(point.getCoordinateSequence());
        }
        if (geometry instanceof LineString lineString) {
            return hasZ(lineString.getCoordinateSequence());
        }
        if (geometry instanceof Polygon polygon) {
            if (hasZ(polygon.getExteriorRing().getCoordinateSequence())) {
                return true;
            }
            for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
                if (hasZ(polygon.getInteriorRingN(i).getCoordinateSequence())) {
                    return true;
                }
            }
            return false;
        }
        if (geometry instanceof GeometryCollection geometryCollection) {
            for (int i = 0; i < geometryCollection.getNumGeometries(); i++) {
                if (hasZ(geometryCollection.getGeometryN(i))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean hasZ(CoordinateSequence coordinates)
    {
        if (!coordinates.hasZ()) {
            return false;
        }
        for (int i = 0; i < coordinates.size(); i++) {
            if (!Double.isNaN(coordinates.getZ(i))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Serialize a JTS Envelope to WKB bytes (as a Polygon).
     */
    public static Slice serialize(Envelope envelope)
    {
        requireNonNull(envelope, "envelope is null");
        verify(!envelope.isNull(), "envelope is null/empty");
        Geometry polygon = GEOMETRY_FACTORY.toGeometry(envelope);
        return serialize(polygon);
    }

    /**
     * Get the geometry type from WKB bytes without full deserialization.
     */
    public static GeometryType deserializeType(Slice shape)
    {
        requireNonNull(shape, "shape is null");
        verify(shape.length() >= 5, "shape too short for WKB header");

        // WKB format: [1 byte endianness] [4 bytes type]
        // endianness: 0 = big endian (XDR), 1 = little endian (NDR)
        byte endianness = shape.getByte(0);
        boolean bigEndian = isBigEndian(endianness);
        int wkbType;
        if (bigEndian) {
            // Big endian - read bytes manually
            wkbType = ((shape.getByte(1) & 0xFF) << 24) |
                    ((shape.getByte(2) & 0xFF) << 16) |
                    ((shape.getByte(3) & 0xFF) << 8) |
                    (shape.getByte(4) & 0xFF);
        }
        else {
            // Little endian - read bytes manually
            wkbType = (shape.getByte(1) & 0xFF) |
                    ((shape.getByte(2) & 0xFF) << 8) |
                    ((shape.getByte(3) & 0xFF) << 16) |
                    ((shape.getByte(4) & 0xFF) << 24);
        }

        // Mask off Z/M/SRID flags to get base type
        // WKB type codes: 1=Point, 2=LineString, 3=Polygon, 4=MultiPoint, etc.
        // Z adds 1000, M adds 2000, ZM adds 3000
        // EWKB SRID flag is 0x20000000
        int baseType = wkbType & 0xFFFF;
        if (baseType > 1000) {
            baseType = baseType % 1000;
        }

        return switch (baseType) {
            case WKB_POINT -> GeometryType.POINT;
            case WKB_LINE_STRING -> GeometryType.LINE_STRING;
            case WKB_POLYGON -> GeometryType.POLYGON;
            case WKB_MULTI_POINT -> GeometryType.MULTI_POINT;
            case WKB_MULTI_LINE_STRING -> GeometryType.MULTI_LINE_STRING;
            case WKB_MULTI_POLYGON -> GeometryType.MULTI_POLYGON;
            case WKB_GEOMETRY_COLLECTION -> GeometryType.GEOMETRY_COLLECTION;
            default -> throw new IllegalArgumentException("Unknown WKB type: " + wkbType);
        };
    }

    /**
     * Get the envelope (bounding box) of a geometry from WKB bytes.
     * This requires parsing the full geometry.
     */
    public static Envelope deserializeEnvelope(Slice shape)
    {
        Geometry geometry = deserialize(shape);
        return geometry.getEnvelopeInternal();
    }

    /**
     * Serialize geometry preserving SRID from source geometry.
     */
    public static Slice serializeWithSrid(Geometry result, Geometry source)
    {
        result.setSRID(source.getSRID());
        return serialize(result);
    }

    /**
     * Validate SRID match for binary operations. Returns the SRID to use.
     * Rules: 0 matches anything, mismatched non-zero SRIDs throw exception.
     */
    public static int validateAndGetSrid(Geometry left, Geometry right)
    {
        int leftSrid = left.getSRID();
        int rightSrid = right.getSRID();

        if (leftSrid == 0) {
            return rightSrid;
        }
        if (rightSrid == 0) {
            return leftSrid;
        }
        if (leftSrid != rightSrid) {
            throw new TrinoException(
                    INVALID_FUNCTION_ARGUMENT,
                    format("SRID mismatch: %d vs %d", leftSrid, rightSrid));
        }
        return leftSrid;
    }

    /**
     * Serialize binary operation result with validated SRID.
     */
    public static Slice serializeBinaryOp(Geometry result, Geometry left, Geometry right)
    {
        result.setSRID(validateAndGetSrid(left, right));
        return serialize(result);
    }

    /**
     * Extract SRID from EWKB without full parsing.
     * Returns 0 if no SRID is embedded.
     */
    public static int extractSrid(Slice ewkb)
    {
        if (ewkb.length() < 9) {
            return 0;
        }
        boolean bigEndian = isBigEndian(ewkb.getByte(0));
        int type = ewkb.getInt(1);
        if (bigEndian) {
            type = Integer.reverseBytes(type);
        }
        if ((type & EWKB_SRID_FLAG) == 0) {
            return 0;
        }
        int srid = ewkb.getInt(5);
        if (bigEndian) {
            srid = Integer.reverseBytes(srid);
        }
        return srid;
    }

    /**
     * Strip SRID from EWKB to produce standard WKB.
     * If the input is already standard WKB (no SRID flag), returns it unchanged.
     */
    public static Slice ewkbToWkb(Slice ewkb)
    {
        if (ewkb.length() < 9) {
            return ewkb;
        }
        boolean bigEndian = isBigEndian(ewkb.getByte(0));
        int type = readType(ewkb, bigEndian);
        verifySupportedCoordinateDimensions(type);
        if ((type & EWKB_SRID_FLAG) == 0) {
            return ewkb;
        }

        // Strip SRID flag and 4 SRID bytes
        int newType = type & ~EWKB_SRID_FLAG;
        Slice wkb = Slices.allocate(ewkb.length() - 4);
        wkb.setByte(0, ewkb.getByte(0)); // endianness
        wkb.setInt(1, bigEndian ? Integer.reverseBytes(newType) : newType);
        wkb.setBytes(5, ewkb, 9, ewkb.length() - 9); // geometry data
        return wkb;
    }

    /**
     * Convert a CRS string to an SRID integer.
     * Supports formats:
     * - "EPSG:XXXX" → XXXX
     * - "OGC:CRS84" or "CRS84" → 4326 (WGS84)
     */
    public static int crsToSrid(String crs)
    {
        if (crs == null || crs.isEmpty()) {
            return 0;
        }
        String upperCrs = crs.toUpperCase(ENGLISH);
        if (upperCrs.equals("OGC:CRS84") || upperCrs.equals("CRS84")) {
            return OGC_CRS84_SRID; // WGS84
        }
        if (upperCrs.startsWith("EPSG:")) {
            try {
                int srid = Integer.parseInt(crs.substring(5));
                if (srid <= 0) {
                    throw new IllegalArgumentException("Invalid EPSG code: " + crs);
                }
                return srid;
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid EPSG code: " + crs);
            }
        }
        throw new IllegalArgumentException("Unsupported CRS format: " + crs);
    }

    /**
     * Inject SRID into WKB to produce EWKB.
     */
    public static Slice wkbToEwkb(Slice wkb, int srid)
    {
        checkArgument(wkb.length() >= 5, "WKB too short");
        boolean bigEndian = isBigEndian(wkb.getByte(0));
        int type = readType(wkb, bigEndian);
        checkArgument((type & EWKB_SRID_FLAG) == 0, "Input already has SRID flag set (expected WKB, got EWKB)");
        verifySupportedCoordinateDimensions(type);

        // Add SRID flag
        int newType = type | EWKB_SRID_FLAG;
        Slice ewkb = Slices.allocate(wkb.length() + 4);
        ewkb.setByte(0, wkb.getByte(0)); // endianness
        ewkb.setInt(1, bigEndian ? Integer.reverseBytes(newType) : newType);
        ewkb.setInt(5, bigEndian ? Integer.reverseBytes(srid) : srid);
        ewkb.setBytes(9, wkb, 5, wkb.length() - 5); // geometry data
        return ewkb;
    }

    private static boolean isBigEndian(byte endianness)
    {
        checkArgument(endianness == 0 || endianness == 1, "invalid WKB endianness: %s", endianness);
        return endianness == 0;
    }

    private static int readType(Slice wkb, boolean bigEndian)
    {
        int type = wkb.getInt(1);
        if (bigEndian) {
            type = Integer.reverseBytes(type);
        }
        return type;
    }

    private static void verifySupportedCoordinateDimensions(int type)
    {
        int isoDimension = (type & 0xFFFF) / 1000;
        if ((type & EWKB_M_FLAG) != 0 || isoDimension > 1) {
            throw unsupportedCoordinateDimensions();
        }
    }

    private static TrinoException unsupportedCoordinateDimensions()
    {
        return new TrinoException(NOT_SUPPORTED, UNSUPPORTED_MEASURE_DIMENSION_MESSAGE);
    }
}
