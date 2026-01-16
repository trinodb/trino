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
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Serializes JTS Geometry objects to/from EWKB (Extended Well-Known Binary) format.
 * EWKB is the PostGIS extension that includes SRID in the binary format.
 */
public final class JtsGeometrySerde
{
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

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
        requireNonNull(geometry, "geometry is null");
        // WKBWriter(outputDimension, includeSRID)
        // Always include SRID in EWKB format
        byte[] bytes = new WKBWriter(2, true).write(geometry);
        return Slices.wrappedBuffer(bytes);
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
        int wkbType;
        if (endianness == 0) {
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
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT,
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
}
