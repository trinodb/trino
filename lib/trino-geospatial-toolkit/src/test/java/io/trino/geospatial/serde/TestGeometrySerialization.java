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
import io.trino.geospatial.GeometryType;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import static io.trino.geospatial.GeometryType.GEOMETRY_COLLECTION;
import static io.trino.geospatial.GeometryType.LINE_STRING;
import static io.trino.geospatial.GeometryType.MULTI_LINE_STRING;
import static io.trino.geospatial.GeometryType.MULTI_POINT;
import static io.trino.geospatial.GeometryType.MULTI_POLYGON;
import static io.trino.geospatial.GeometryType.POINT;
import static io.trino.geospatial.GeometryType.POLYGON;
import static io.trino.geospatial.serde.JtsGeometrySerde.deserialize;
import static io.trino.geospatial.serde.JtsGeometrySerde.deserializeEnvelope;
import static io.trino.geospatial.serde.JtsGeometrySerde.deserializeType;
import static io.trino.geospatial.serde.JtsGeometrySerde.serialize;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGeometrySerialization
{
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

    private static void testSerialization(String wkt)
    {
        Geometry geometry = createJtsGeometry(wkt);
        Slice serialized = serialize(geometry);
        Geometry deserialized = deserialize(serialized);

        assertThat(deserialized.norm()).isEqualTo(geometry.norm());
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
