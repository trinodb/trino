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
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;

import static io.trino.geospatial.GeometryType.GEOMETRY_COLLECTION;
import static io.trino.geospatial.GeometryType.LINE_STRING;
import static io.trino.geospatial.GeometryType.MULTI_LINE_STRING;
import static io.trino.geospatial.GeometryType.MULTI_POINT;
import static io.trino.geospatial.GeometryType.MULTI_POLYGON;
import static io.trino.geospatial.GeometryType.POINT;
import static io.trino.geospatial.GeometryType.POLYGON;
import static io.trino.geospatial.serde.JtsGeometrySerde.crsToSrid;
import static io.trino.geospatial.serde.JtsGeometrySerde.deserialize;
import static io.trino.geospatial.serde.JtsGeometrySerde.deserializeEnvelope;
import static io.trino.geospatial.serde.JtsGeometrySerde.deserializeType;
import static io.trino.geospatial.serde.JtsGeometrySerde.ewkbToWkb;
import static io.trino.geospatial.serde.JtsGeometrySerde.extractSrid;
import static io.trino.geospatial.serde.JtsGeometrySerde.serialize;
import static io.trino.geospatial.serde.JtsGeometrySerde.serializeWithoutSrid;
import static io.trino.geospatial.serde.JtsGeometrySerde.wkbToEwkb;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestGeometrySerialization
{
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    @Test
    public void testPoint()
    {
        testSerialization("POINT (1 2)");
        testSerialization("POINT (-1 -2)");
        testSerialization("POINT (0 0)");
        testSerialization("POINT (-2e3 -4e33)");
        testSerialization("POINT EMPTY");
    }

    @Test
    public void testMultiPoint()
    {
        testSerialization("MULTIPOINT ((0 0))");
        testSerialization("MULTIPOINT ((0 0), (0 0))");
        testSerialization("MULTIPOINT ((0 0), (1 1), (2 3))");
        testSerialization("MULTIPOINT EMPTY");
    }

    @Test
    public void testLineString()
    {
        testSerialization("LINESTRING (0 1, 2 3)");
        testSerialization("LINESTRING (0 1, 2 3, 4 5)");
        testSerialization("LINESTRING (0 1, 2 3, 4 5, 0 1)");
        testSerialization("LINESTRING EMPTY");
    }

    @Test
    public void testMultiLineString()
    {
        testSerialization("MULTILINESTRING ((0 1, 2 3, 4 5))");
        testSerialization("MULTILINESTRING ((0 1, 2 3, 4 5), (0 1, 2 3, 4 5))");
        testSerialization("MULTILINESTRING ((0 1, 2 3, 4 5), (0 1, 2 3, 4 6), (0 1, 2 3, 4 7), (0 1, 2 3, 4 7, 0 1))");
        testSerialization("MULTILINESTRING ((0 1, 2 3, 4 5), (0 1, 2 3, 4 6), (0 1, 2 3, 4 7), (0.333 0.74, 0.1 0.2, 2e3 4e-3), (0.333 0.74, 2e3 4e-3))");
        testSerialization("MULTILINESTRING ((0 1, 2 3, 4 5), (1 1, 2 2))");
        testSerialization("MULTILINESTRING EMPTY");
    }

    @Test
    public void testPolygon()
    {
        testSerialization("POLYGON ((30 10, 40 40, 20 40, 30 10))");
        testSerialization("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))");
        testSerialization("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))");
        testSerialization("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))");
        testSerialization("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
        testSerialization("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0.75 0.25, 0.75 0.75, 0.25 0.75, 0.25 0.25, 0.75 0.25))");
        testSerialization("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0.25 0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25, 0.25 0.25))");
        testSerialization("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0.25 0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25, 0.25 0.25), (0.75 0.25, 0.75 0.75, 0.25 0.75, 0.25 0.25, 0.75 0.25))");
        testSerialization("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0.25 0.25, 0.75 0.75, 0.25 0.75, 0.75 0.25, 0.25 0.25))");
        testSerialization("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0.25 0.25, 0.75 0.75, 0.25 0.75, 0.75 0.25, 0.25 0.25), (0.25 0.25, 0.75 0.75, 0.25 0.75, 0.75 0.25, 0.25 0.25))");
        testSerialization("POLYGON EMPTY");
        testSerialization("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0.25 0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25, 0.25 0.25))");
        testSerialization("POLYGON ((489960.9893476785 6104581.829743207, 490020.7763635455 6104522.042727341, 490020.77636354364 6104522.042727342, 489960.9893476785 6104581.829743207))");
    }

    @Test
    public void testMultiPolygon()
    {
        testSerialization("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)))");
        testSerialization("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((30 20, 45 40, 10 40, 30 20)))");
        testSerialization("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 15 5)))");
        testSerialization("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((0 0, 0 1, 1 1, 1 0, 0 0), (0.75 0.25, 0.75 0.75, 0.25 0.75, 0.25 0.25, 0.75 0.25)), ((15 5, 40 10, 10 20, 5 10, 15 5)))");
        testSerialization("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((0 0, 0 1, 1 1, 1 0, 0 0), (0.25 0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25, 0.25 0.25)))");
        testSerialization("MULTIPOLYGON (" +
                "((30 20, 45 40, 10 40, 30 20)), " +
                // clockwise, counter clockwise
                "((0 0, 0 1, 1 1, 1 0, 0 0), (0.75 0.25, 0.75 0.75, 0.25 0.75, 0.25 0.25, 0.75 0.25)), " +
                // clockwise, clockwise
                "((0 0, 0 1, 1 1, 1 0, 0 0), (0.25 0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25, 0.25 0.25)), " +
                // counter clockwise, clockwise
                "((0 0, 1 0, 1 1, 0 1, 0 0), (0.25 0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25, 0.25 0.25)), " +
                // counter clockwise, counter clockwise
                "((0 0, 1 0, 1 1, 0 1, 0 0), (0.75 0.25, 0.75 0.75, 0.25 0.75, 0.25 0.25, 0.75 0.25)), " +
                // counter clockwise, counter clockwise, clockwise
                "((0 0, 1 0, 1 1, 0 1, 0 0), (0.75 0.25, 0.75 0.75, 0.25 0.75, 0.25 0.25, 0.75 0.25), (0.25 0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25, 0.25 0.25)))");
        testSerialization("MULTIPOLYGON EMPTY");
    }

    @Test
    public void testGeometryCollection()
    {
        testSerialization("GEOMETRYCOLLECTION (POINT (1 2))");
        testSerialization("GEOMETRYCOLLECTION (POINT (1 2), POINT (2 1), POINT EMPTY)");
        testSerialization("GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 2, 3 4), POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0)))");
        testSerialization("GEOMETRYCOLLECTION (MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20))))");
        testSerialization("GEOMETRYCOLLECTION (MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5))))");
        testSerialization("GEOMETRYCOLLECTION (MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5))), POINT (1 2))");
        testSerialization("GEOMETRYCOLLECTION (POINT EMPTY)");
        testSerialization("GEOMETRYCOLLECTION EMPTY");
        testSerialization("GEOMETRYCOLLECTION (MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20))), GEOMETRYCOLLECTION (MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)))))");
    }

    @Test
    public void testPointSridRoundTrip()
    {
        testSerializationWithSrid("POINT (1 2)", 4326);
    }

    @Test
    public void testGeometryCollectionSridRoundTrip()
    {
        testSerializationWithSrid("GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 2, 3 4))", 3857);
    }

    @Test
    public void testPointWithZRoundTrip()
    {
        testSerializationWithZ("POINT Z (1 2 3)", 3.0);
    }

    @Test
    public void testLineStringWithZRoundTrip()
    {
        testSerializationWithZ("LINESTRING Z (0 1 2, 3 4 5)", 5.0);
    }

    @Test
    public void testPolygonWithZRoundTrip()
    {
        testSerializationWithZ("POLYGON Z ((0 0 1, 0 1 2, 1 1 3, 0 0 1))", 3.0);
    }

    @Test
    public void testGeometryCollectionWithZRoundTrip()
    {
        testSerializationWithZ("GEOMETRYCOLLECTION (POINT Z (1 2 3), LINESTRING (4 5, 6 7))", 3.0);
    }

    @Test
    public void testCrsToSridRejectsNonPositiveEpsgCodes()
    {
        assertThat(crsToSrid("EPSG:3857")).isEqualTo(3857);
        assertThatThrownBy(() -> crsToSrid("EPSG:0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid EPSG code: EPSG:0");
        assertThatThrownBy(() -> crsToSrid("EPSG:-3857"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid EPSG code: EPSG:-3857");
    }

    @Test
    public void testWkbToEwkbRejectsEwkbInput()
    {
        Geometry geometry = createJtsGeometry("POINT (1 2)");
        geometry.setSRID(3857);
        Slice ewkb = serialize(geometry);

        assertThat(ewkbToWkb(ewkb)).isNotEqualTo(ewkb);
        assertThatThrownBy(() -> wkbToEwkb(ewkb, 3857))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Input already has SRID flag set (expected WKB, got EWKB)");
    }

    @Test
    public void testSridHelpersPreserveZ()
    {
        Geometry geometry = createJtsGeometry("POINT Z (1 2 3)");

        Slice wkb = Slices.wrappedBuffer(new WKBWriter(3, false).write(geometry));
        Slice ewkb = wkbToEwkb(wkb, 4326);
        assertThat(extractSrid(ewkb)).isEqualTo(4326);
        assertThat(deserialize(ewkb).getCoordinate().getZ()).isEqualTo(3.0);

        Slice stripped = ewkbToWkb(ewkb);
        assertThat(extractSrid(stripped)).isEqualTo(0);
        assertThat(deserialize(stripped).getCoordinate().getZ()).isEqualTo(3.0);
    }

    @Test
    public void testSridHelpersRejectMeasureCoordinates()
    {
        Slice mWkb = Slices.wrappedBuffer(new byte[] {1, 1, 0, 0, 0x40});
        assertThatThrownBy(() -> wkbToEwkb(mWkb, 4326))
                .hasMessage("Geospatial values with measure coordinates are not supported");

        Slice mEwkb = Slices.wrappedBuffer(new byte[] {1, 1, 0, 0, 0x40, 0, 0, 0, 0});
        assertThatThrownBy(() -> ewkbToWkb(mEwkb))
                .hasMessage("Geospatial values with measure coordinates are not supported");
    }

    @Test
    public void testSerializeRejectsMeasureCoordinates()
    {
        Geometry pointWithM = GEOMETRY_FACTORY.createPoint(new CoordinateXYM(1, 2, 3));
        assertThatThrownBy(() -> serialize(pointWithM))
                .hasMessage("Geospatial values with measure coordinates are not supported");
        assertThatThrownBy(() -> serializeWithoutSrid(pointWithM))
                .hasMessage("Geospatial values with measure coordinates are not supported");

        Geometry pointWithZAndM = GEOMETRY_FACTORY.createPoint(new CoordinateXYZM(1, 2, 3, 4));
        assertThatThrownBy(() -> serialize(pointWithZAndM))
                .hasMessage("Geospatial values with measure coordinates are not supported");
        assertThatThrownBy(() -> serializeWithoutSrid(pointWithZAndM))
                .hasMessage("Geospatial values with measure coordinates are not supported");
    }

    @Test
    public void testSerializeWithoutSridPreservesZ()
    {
        Geometry geometry = createJtsGeometry("POINT Z (1 2 3)");
        geometry.setSRID(4326);

        Slice wkb = serializeWithoutSrid(geometry);
        Geometry deserialized = deserialize(wkb);

        assertThat(deserialized.getSRID()).isEqualTo(0);
        assertThat(deserialized.getCoordinate().getZ()).isEqualTo(3.0);
    }

    @Test
    public void testSerializeWithoutSridUsesIsoZTypeCodes()
    {
        assertIsoZType("POINT Z (1 2 3)", 1001);
        assertIsoZType("LINESTRING Z (0 0 1, 1 1 2)", 1002);
        assertIsoZType("POLYGON Z ((0 0 1, 0 1 2, 1 0 3, 0 0 1))", 1003);
        assertIsoZType("MULTIPOINT Z ((0 0 1), (1 1 2))", 1004);
        assertIsoZType("MULTILINESTRING Z ((0 0 1, 1 1 2))", 1005);
        assertIsoZType("MULTIPOLYGON Z (((0 0 1, 0 1 2, 1 0 3, 0 0 1)))", 1006);
        assertIsoZType("GEOMETRYCOLLECTION Z (POINT Z (0 0 1))", 1007);

        Slice collection = serializeWithoutSrid(createJtsGeometry("GEOMETRYCOLLECTION Z (POINT Z (0 0 1), LINESTRING Z (0 0 2, 1 1 3))"));
        assertThat(readWkbType(collection, 0)).isEqualTo(1007);
        assertThat(readWkbType(collection, 9)).isEqualTo(1001);
        assertThat(readWkbType(collection, 38)).isEqualTo(1002);
    }

    private static void assertIsoZType(String wkt, int expectedType)
    {
        Slice wkb = serializeWithoutSrid(createJtsGeometry(wkt));
        assertThat(readWkbType(wkb, 0)).isEqualTo(expectedType);
    }

    private static int readWkbType(Slice slice, int geometryOffset)
    {
        int offset = geometryOffset + 1;
        if (slice.getByte(geometryOffset) == 0) {
            return ((slice.getByte(offset) & 0xFF) << 24) |
                    ((slice.getByte(offset + 1) & 0xFF) << 16) |
                    ((slice.getByte(offset + 2) & 0xFF) << 8) |
                    (slice.getByte(offset + 3) & 0xFF);
        }
        return (slice.getByte(offset) & 0xFF) |
                ((slice.getByte(offset + 1) & 0xFF) << 8) |
                ((slice.getByte(offset + 2) & 0xFF) << 16) |
                ((slice.getByte(offset + 3) & 0xFF) << 24);
    }

    @Test
    public void testEnvelope()
    {
        testEnvelopeSerialization(new Envelope(0, 1, 0, 1));
        testEnvelopeSerialization(new Envelope(1, 3, 2, 4));
        testEnvelopeSerialization(new Envelope(-3e5, 10101, -2.05, 0));
    }

    private void testEnvelopeSerialization(Envelope envelope)
    {
        Slice serialized = serialize(envelope);
        Geometry deserialized = deserialize(serialized);

        assertThat(deserialized.getGeometryType()).isEqualTo("Polygon");
        assertThat(deserialized.getEnvelopeInternal()).isEqualTo(envelope);
        assertThat(deserializeType(serialized)).isEqualTo(POLYGON);
        assertThat(deserializeEnvelope(serialized)).isEqualTo(envelope);
    }

    @Test
    public void testDeserializeEnvelope()
    {
        assertDeserializeEnvelope("MULTIPOINT ((20 20), (25 25))", new Envelope(20, 25, 20, 25));
        assertDeserializeEnvelope("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", new Envelope(1, 5, 1, 4));
        assertDeserializeEnvelope("POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))", new Envelope(0, 4, 0, 4));
        assertDeserializeEnvelope("MULTIPOLYGON (((0 0, 0 2, 2 2, 2 0, 0 0)), ((2 2, 2 4, 4 4, 4 2, 2 2)))", new Envelope(0, 4, 0, 4));
        assertDeserializeEnvelope("GEOMETRYCOLLECTION (POINT (3 7), LINESTRING (4 6, 7 10))", new Envelope(3, 7, 6, 10));
        assertDeserializeEnvelope("POLYGON EMPTY", new Envelope());
        assertDeserializeEnvelope("POINT (1 2)", new Envelope(1, 1, 2, 2));
        assertDeserializeEnvelope("POINT EMPTY", new Envelope());
        assertDeserializeEnvelope("GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (2 7), LINESTRING (4 6, 7 10)), POINT (3 7), LINESTRING (4 6, 7 10))", new Envelope(2, 7, 6, 10));
    }

    @Test
    public void testDeserializeType()
    {
        assertDeserializeType("POINT (1 2)", POINT);
        assertDeserializeType("POINT EMPTY", POINT);
        assertDeserializeType("MULTIPOINT ((20 20), (25 25))", MULTI_POINT);
        assertDeserializeType("MULTIPOINT EMPTY", MULTI_POINT);
        assertDeserializeType("LINESTRING (1 1, 5 1, 6 2)", LINE_STRING);
        assertDeserializeType("LINESTRING EMPTY", LINE_STRING);
        assertDeserializeType("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", MULTI_LINE_STRING);
        assertDeserializeType("MULTILINESTRING EMPTY", MULTI_LINE_STRING);
        assertDeserializeType("POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))", POLYGON);
        assertDeserializeType("POLYGON EMPTY", POLYGON);
        assertDeserializeType("MULTIPOLYGON (((0 0, 0 2, 2 2, 2 0, 0 0)), ((2 2, 2 4, 4 4, 4 2, 2 2)))", MULTI_POLYGON);
        assertDeserializeType("MULTIPOLYGON EMPTY", MULTI_POLYGON);
        assertDeserializeType("GEOMETRYCOLLECTION (POINT (3 7), LINESTRING (4 6, 7 10))", GEOMETRY_COLLECTION);
        assertDeserializeType("GEOMETRYCOLLECTION EMPTY", GEOMETRY_COLLECTION);

        assertThat(deserializeType(serialize(new Envelope(1, 3, 2, 4)))).isEqualTo(POLYGON);
    }

    @Test
    public void testDeserializeTypeRejectsInvalidByteOrder()
    {
        assertThatThrownBy(() -> deserializeType(Slices.wrappedBuffer(new byte[] {2, 1, 0, 0, 0})))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid WKB endianness: 2");
    }

    @Test
    public void testSridHelpersRejectInvalidByteOrder()
    {
        Slice invalidEwkb = Slices.wrappedBuffer(new byte[] {2, 1, 0, 0, 0, 0, 0, 0, 0});
        assertThatThrownBy(() -> extractSrid(invalidEwkb))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid WKB endianness: 2");
        assertThatThrownBy(() -> ewkbToWkb(invalidEwkb))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid WKB endianness: 2");

        Slice invalidWkb = Slices.wrappedBuffer(new byte[] {2, 1, 0, 0, 0});
        assertThatThrownBy(() -> wkbToEwkb(invalidWkb, 4326))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid WKB endianness: 2");
    }

    private static void testSerialization(String wkt)
    {
        Geometry geometry = createJtsGeometry(wkt);
        Slice serialized = serialize(geometry);
        Geometry deserialized = deserialize(serialized);

        assertThat(deserialized.norm()).isEqualTo(geometry.norm());
    }

    private static void testSerializationWithSrid(String wkt, int srid)
    {
        Geometry geometry = createJtsGeometry(wkt);
        geometry.setSRID(srid);

        Slice serialized = serialize(geometry);
        Geometry deserialized = deserialize(serialized);

        assertThat(deserialized.norm()).isEqualTo(geometry.norm());
        assertThat(deserialized.getSRID()).isEqualTo(srid);
    }

    private static void testSerializationWithZ(String wkt, double expectedZ)
    {
        Geometry geometry = createJtsGeometry(wkt);
        Slice serialized = serialize(geometry);
        Geometry deserialized = deserialize(serialized);

        assertThat(deserialized.norm()).isEqualTo(geometry.norm());
        assertThat(deserialized.getCoordinates())
                .extracting(coordinate -> coordinate.getZ())
                .contains(expectedZ);
    }

    private static Slice geometryFromText(String wkt)
    {
        return serialize(createJtsGeometry(wkt));
    }

    private static Geometry createJtsGeometry(String wkt)
    {
        try {
            return new WKTReader().read(wkt);
        }
        catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private static void assertDeserializeEnvelope(String wkt, Envelope expectedEnvelope)
    {
        assertThat(deserializeEnvelope(geometryFromText(wkt))).isEqualTo(expectedEnvelope);
    }

    private static void assertDeserializeType(String wkt, GeometryType expectedType)
    {
        assertThat(deserializeType(geometryFromText(wkt))).isEqualTo(expectedType);
    }
}
