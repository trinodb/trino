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

import com.esri.core.geometry.Point;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;
import com.google.common.collect.ImmutableList;
import io.trino.geospatial.KdbTreeUtils;
import io.trino.geospatial.Rectangle;
import io.trino.geospatial.serde.GeometrySerde;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.trino.geospatial.KdbTree.buildKdbTree;
import static io.trino.plugin.geospatial.GeoFunctions.stCentroid;
import static io.trino.plugin.geospatial.GeoTestUtils.assertSpatialArrayEquals;
import static io.trino.plugin.geospatial.GeoTestUtils.assertSpatialEquals;
import static io.trino.plugin.geospatial.GeometryType.GEOMETRY;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestGeoFunctions
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addPlugin(new GeoPlugin());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testSpatialPartitions()
    {
        String kdbTreeJson = makeKdbTreeJson();

        assertSpatialPartitions(kdbTreeJson, "POINT EMPTY", null);
        // points inside partitions
        assertSpatialPartitions(kdbTreeJson, "POINT (0 0)", ImmutableList.of(0));
        assertSpatialPartitions(kdbTreeJson, "POINT (3 1)", ImmutableList.of(2));
        // point on the border between two partitions
        assertSpatialPartitions(kdbTreeJson, "POINT (1 2.5)", ImmutableList.of(1));
        // point at the corner of three partitions
        assertSpatialPartitions(kdbTreeJson, "POINT (4.5 2.5)", ImmutableList.of(4));
        // points outside
        assertSpatialPartitions(kdbTreeJson, "POINT (2 6)", ImmutableList.of());
        assertSpatialPartitions(kdbTreeJson, "POINT (3 -1)", ImmutableList.of());
        assertSpatialPartitions(kdbTreeJson, "POINT (10 3)", ImmutableList.of());

        // geometry within a partition
        assertSpatialPartitions(kdbTreeJson, "MULTIPOINT (5 0.1, 6 2)", ImmutableList.of(3));
        // geometries spanning multiple partitions
        assertSpatialPartitions(kdbTreeJson, "MULTIPOINT (5 0.1, 5.5 3, 6 2)", ImmutableList.of(3, 4));
        assertSpatialPartitions(kdbTreeJson, "MULTIPOINT (3 2, 8 3)", ImmutableList.of(2, 3, 4, 5));
        // geometry outside
        assertSpatialPartitions(kdbTreeJson, "MULTIPOINT (2 6, 3 7)", ImmutableList.of());

        // with distance
        assertSpatialPartitions(kdbTreeJson, "POINT EMPTY", 1.2, null);
        assertSpatialPartitions(kdbTreeJson, "POINT (1 1)", 1.2, ImmutableList.of(0));
        assertSpatialPartitions(kdbTreeJson, "POINT (1 1)", 2.3, ImmutableList.of(0, 1, 2));
        assertSpatialPartitions(kdbTreeJson, "MULTIPOINT (5 0.1, 6 2)", 0.2, ImmutableList.of(3));
        assertSpatialPartitions(kdbTreeJson, "MULTIPOINT (5 0.1, 6 2)", 1.2, ImmutableList.of(2, 3, 4));
        assertSpatialPartitions(kdbTreeJson, "MULTIPOINT (2 6, 3 7)", 1.2, ImmutableList.of());
    }

    private static String makeKdbTreeJson()
    {
        ImmutableList.Builder<Rectangle> rectangles = ImmutableList.builder();
        for (double x = 0; x < 10; x += 1) {
            for (double y = 0; y < 5; y += 1) {
                rectangles.add(new Rectangle(x, y, x + 1, y + 2));
            }
        }
        return KdbTreeUtils.toJson(buildKdbTree(10, new Rectangle(0, 0, 9, 4), rectangles.build())).toStringUtf8();
    }

    private void assertSpatialPartitions(String kdbTreeJson, String wkt, List<Integer> expectedPartitions)
    {
        assertThat(assertions.function("spatial_partitions", "cast('%s' as KdbTree)".formatted(kdbTreeJson), "ST_GeometryFromText('%s')".formatted(wkt)))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(expectedPartitions);
    }

    private void assertSpatialPartitions(String kdbTreeJson, String wkt, double distance, List<Integer> expectedPartitions)
    {
        assertThat(assertions.function("spatial_partitions", "cast('%s' as KdbTree)".formatted(kdbTreeJson), "ST_GeometryFromText('%s')".formatted(wkt), Double.toString(distance)))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(expectedPartitions);
    }

    @Test
    public void testGeometryGetObjectValue()
    {
        BlockBuilder builder = GEOMETRY.createBlockBuilder(null, 1);
        GEOMETRY.writeSlice(builder, GeoFunctions.stPoint(1.2, 3.4));
        Block block = builder.build();

        assertThat("POINT (1.2 3.4)").isEqualTo(GEOMETRY.getObjectValue(block, 0));
    }

    @Test
    public void testSTPoint()
    {
        assertThat(assertions.function("ST_AsText", "ST_Point(1, 4)"))
                .hasType(VARCHAR)
                .isEqualTo("POINT (1 4)");

        assertThat(assertions.function("ST_AsText", "ST_Point(122.3, 10.55)"))
                .hasType(VARCHAR)
                .isEqualTo("POINT (122.3 10.55)");
    }

    @Test
    public void testSTLineFromText()
    {
        assertThat(assertions.function("ST_AsText", "ST_LineFromText('LINESTRING EMPTY')"))
                .hasType(VARCHAR)
                .isEqualTo("LINESTRING EMPTY");

        assertThat(assertions.function("ST_AsText", "ST_LineFromText('LINESTRING (1 1, 2 2, 1 3)')"))
                .hasType(VARCHAR)
                .isEqualTo("LINESTRING (1 1, 2 2, 1 3)");

        assertTrinoExceptionThrownBy(assertions.function("ST_AsText", "ST_LineFromText('MULTILINESTRING EMPTY')")::evaluate)
                .hasMessage("ST_LineFromText only applies to LINE_STRING. Input type is: MULTI_LINE_STRING");

        assertTrinoExceptionThrownBy(assertions.function("ST_AsText", "ST_LineFromText('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))')")::evaluate)
                .hasMessage("ST_LineFromText only applies to LINE_STRING. Input type is: POLYGON");
    }

    @Test
    public void testSTPolygon()
    {
        assertSpatialEquals(assertions,
                "ST_Polygon('POLYGON EMPTY')",
                "POLYGON EMPTY");

        assertSpatialEquals(assertions,
                "ST_Polygon('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))')",
                "POLYGON ((1 1, 4 1, 4 4, 1 4, 1 1))");

        assertTrinoExceptionThrownBy(assertions.function("ST_AsText", "ST_Polygon('LINESTRING (1 1, 2 2, 1 3)')")::evaluate)
                .hasMessage("ST_Polygon only applies to POLYGON. Input type is: LINE_STRING");
    }

    @Test
    public void testSTArea()
    {
        assertArea("POLYGON ((2 2, 2 6, 6 6, 6 2, 2 2))", 16.0);
        assertArea("POLYGON EMPTY", 0.0);
        assertArea("LINESTRING (1 4, 2 5)", 0.0);
        assertArea("LINESTRING EMPTY", 0.0);
        assertArea("POINT (1 4)", 0.0);
        assertArea("POINT EMPTY", 0.0);
        assertArea("GEOMETRYCOLLECTION EMPTY", 0.0);

        // Test basic geometry collection. Area is the area of the polygon.
        assertArea("GEOMETRYCOLLECTION (POINT (8 8), LINESTRING (5 5, 6 6), POLYGON ((1 1, 3 1, 3 4, 1 4, 1 1)))", 6.0);

        // Test overlapping geometries. Area is the sum of the individual elements
        assertArea("GEOMETRYCOLLECTION (POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)))", 8.0);

        // Test nested geometry collection
        assertArea("GEOMETRYCOLLECTION (POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)), GEOMETRYCOLLECTION (POINT (8 8), LINESTRING (5 5, 6 6), POLYGON ((1 1, 3 1, 3 4, 1 4, 1 1))))", 14.0);
    }

    private void assertArea(String wkt, double expectedArea)
    {
        assertThat(assertions.expression("ST_Area(geometry)")
                .binding("geometry", "ST_GeometryFromText('%s')".formatted(wkt)))
                .isEqualTo(expectedArea);
    }

    @Test
    public void testSTBuffer()
    {
        // ST_Buffer involves trigonometric calculations that produce slightly different
        // floating-point results across CPU architectures (ARM vs x86). Instead of checking
        // exact coordinates, we verify the area (which is stable across architectures).

        // Point buffer: area should be approximately pi * r^2 = pi * 0.25 ≈ 0.785
        assertThat((Double) assertions.expression("ST_Area(ST_Buffer(ST_Point(0, 0), 0.5))")
                .evaluate().value())
                .isCloseTo(0.785, within(0.01));

        // LineString buffer: verify approximate area
        assertThat((Double) assertions.expression("ST_Area(ST_Buffer(ST_LineFromText('LINESTRING (0 0, 1 1, 2 0.5)'), 0.2))")
                .evaluate().value())
                .isCloseTo(1.13, within(0.05));

        // Polygon buffer: area should be approximately (5+2*1.2)^2 with rounded corners ≈ 53.5
        assertThat((Double) assertions.expression("ST_Area(ST_Buffer(ST_GeometryFromText('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))'), 1.2))")
                .evaluate().value())
                .isCloseTo(53.5, within(0.5));

        // zero distance
        assertSpatialEquals(assertions, "ST_Buffer(ST_Point(0, 0), 0)", "POINT (0 0)");
        assertSpatialEquals(assertions, "ST_Buffer(ST_LineFromText('LINESTRING (0 0, 1 1, 2 0.5)'), 0)", "LINESTRING (0 0, 1 1, 2 0.5)");
        assertSpatialEquals(assertions, "ST_Buffer(ST_GeometryFromText('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))'), 0)", "POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))");

        // geometry collection buffer: verify area is positive (intersection produces points at (5, 1) and (4, 4))
        assertThat((Double) assertions.expression("ST_Area(ST_Buffer(ST_Intersection(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'), ST_GeometryFromText('MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))')), 0.2))")
                .evaluate().value())
                .isGreaterThan(0.0);

        // empty geometry
        assertThat(assertions.function("ST_Buffer", "ST_GeometryFromText('POINT EMPTY')", "1"))
                .isNull(GEOMETRY);

        // negative distance
        assertTrinoExceptionThrownBy(assertions.function("ST_Buffer", "ST_Point(0, 0)", "-1.2")::evaluate)
                .hasMessage("distance is negative");

        assertTrinoExceptionThrownBy(assertions.function("ST_Buffer", "ST_Point(0, 0)", "-infinity()")::evaluate)
                .hasMessage("distance is negative");

        // infinity() and nan() distance
        assertSpatialEquals(assertions, "ST_Buffer(ST_Point(0, 0), infinity())", "MULTIPOLYGON EMPTY");

        assertTrinoExceptionThrownBy(assertions.function("ST_Buffer", "ST_Point(0, 0)", "nan()")::evaluate)
                .hasMessage("distance is NaN");

        // For small polygons, there was a bug in ESRI that throw an NPE.  This
        // was fixed (https://github.com/Esri/geometry-api-java/pull/243) to
        // return an empty geometry instead. Ideally, these would return
        // something approximately like `ST_Buffer(ST_Centroid(geometry))`.
        assertThat(assertions.function("ST_IsEmpty", "ST_Buffer(ST_Buffer(ST_Point(177.50102959662, 64.726807421691), 0.0000000001), 0.00005)"))
                .hasType(BOOLEAN)
                .isEqualTo(true);

        assertThat(assertions.function("ST_IsEmpty", "ST_Buffer(ST_GeometryFromText(" +
                "'POLYGON ((177.0 64.0, 177.0000000001 64.0, 177.0000000001 64.0000000001, 177.0 64.0000000001, 177.0 64.0))'), 0.01)"))
                .hasType(BOOLEAN)
                .isEqualTo(true);
    }

    @Test
    public void testSTCentroid()
    {
        assertSpatialEquals(assertions,
                "ST_Centroid(ST_GeometryFromText('LINESTRING EMPTY'))",
                "POINT EMPTY");

        assertSpatialEquals(assertions,
                "ST_Centroid(ST_GeometryFromText('POINT (3 5)'))",
                "POINT (3 5)");

        assertSpatialEquals(assertions,
                "ST_Centroid(ST_GeometryFromText('MULTIPOINT (1 2, 2 4, 3 6, 4 8)'))",
                "POINT (2.5 5)");

        assertSpatialEquals(assertions,
                "ST_Centroid(ST_GeometryFromText('LINESTRING (1 1, 2 2, 3 3)'))",
                "POINT (2 2)");

        assertSpatialEquals(assertions,
                "ST_Centroid(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'))",
                "POINT (3 2)");

        assertSpatialEquals(assertions,
                "ST_Centroid(ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))'))",
                "POINT (2.5 2.5)");

        assertSpatialEquals(assertions,
                "ST_Centroid(ST_GeometryFromText('POLYGON ((1 1, 5 1, 3 4, 1 1))'))",
                "POINT (3 2)");

        assertSpatialEquals(assertions,
                "ST_Centroid(ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 6 4, 2 4)))'))",
                "POINT (3.3333333333333335 4)");

        assertSpatialEquals(assertions,
                "ST_Centroid(ST_GeometryFromText('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))'))",
                "POINT (2.5416666666666665 2.5416666666666665)");

        assertApproximateCentroid("MULTIPOLYGON (((4.903234300000006 52.08474289999999, 4.903234265193165 52.084742934806826, 4.903234299999999 52.08474289999999, 4.903234300000006 52.08474289999999)))", new Point(4.9032343, 52.0847429), 1e-7);

        // Numerical stability tests
        assertApproximateCentroid(
                "MULTIPOLYGON (((153.492818 -28.13729, 153.492821 -28.137291, 153.492816 -28.137289, 153.492818 -28.13729)))",
                new Point(153.49282, -28.13729), 1e-5);
        assertApproximateCentroid(
                "MULTIPOLYGON (((153.112475 -28.360526, 153.1124759 -28.360527, 153.1124759 -28.360526, 153.112475 -28.360526)))",
                new Point(153.112475, -28.360526), 1e-5);
        assertApproximateCentroid(
                "POLYGON ((4.903234300000006 52.08474289999999, 4.903234265193165 52.084742934806826, 4.903234299999999 52.08474289999999, 4.903234300000006 52.08474289999999))",
                new Point(4.9032343, 52.0847429), 1e-6);
        assertApproximateCentroid(
                "MULTIPOLYGON (((4.903234300000006 52.08474289999999, 4.903234265193165 52.084742934806826, 4.903234299999999 52.08474289999999, 4.903234300000006 52.08474289999999)))",
                new Point(4.9032343, 52.0847429), 1e-6);
        assertApproximateCentroid(
                "POLYGON ((-81.0387349 29.20822, -81.039974 29.210597, -81.0410331 29.2101579, -81.0404758 29.2090879, -81.0404618 29.2090609, -81.040433 29.209005, -81.0404269 29.208993, -81.0404161 29.2089729, -81.0398001 29.20779, -81.0387349 29.20822), (-81.0404229 29.208986, -81.04042 29.2089809, -81.0404269 29.208993, -81.0404229 29.208986))",
                new Point(-81.039885, 29.209191), 1e-6);
    }

    private void assertApproximateCentroid(String wkt, Point expectedCentroid, double epsilon)
    {
        OGCPoint actualCentroid = (OGCPoint) GeometrySerde.deserialize(
                stCentroid(GeometrySerde.serialize(OGCGeometry.fromText(wkt))));
        assertThat(expectedCentroid.getX()).isCloseTo(actualCentroid.X(), within(epsilon));
        assertThat(expectedCentroid.getY()).isCloseTo(actualCentroid.Y(), within(epsilon));
    }

    @Test
    public void testSTConvexHull()
    {
        // test empty geometry
        assertConvexHull("POINT EMPTY", "POINT EMPTY");
        assertConvexHull("MULTIPOINT EMPTY", "MULTIPOINT EMPTY");
        assertConvexHull("LINESTRING EMPTY", "LINESTRING EMPTY");
        assertConvexHull("MULTILINESTRING EMPTY", "MULTILINESTRING EMPTY");
        assertConvexHull("POLYGON EMPTY", "POLYGON EMPTY");
        assertConvexHull("MULTIPOLYGON EMPTY", "MULTIPOLYGON EMPTY");
        assertConvexHull("GEOMETRYCOLLECTION EMPTY", "GEOMETRYCOLLECTION EMPTY");
        assertConvexHull("GEOMETRYCOLLECTION (POINT (1 1), POINT EMPTY)", "POINT (1 1)");
        assertConvexHull("GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (1 1), GEOMETRYCOLLECTION (POINT (1 5), POINT (4 5), GEOMETRYCOLLECTION (POINT (3 4), POINT EMPTY))))", "POLYGON ((1 1, 4 5, 1 5, 1 1))");

        // test single geometry
        assertConvexHull("POINT (1 1)", "POINT (1 1)");
        assertConvexHull("LINESTRING (1 1, 1 9, 2 2)", "POLYGON ((1 1, 2 2, 1 9, 1 1))");

        // convex single geometry
        assertConvexHull("LINESTRING (1 1, 1 9, 2 2, 1 1)", "POLYGON ((1 1, 2 2, 1 9, 1 1))");
        assertConvexHull("POLYGON ((0 0, 0 3, 2 4, 4 2, 3 0, 0 0))", "POLYGON ((0 0, 3 0, 4 2, 2 4, 0 3, 0 0))");

        // non-convex geometry
        assertConvexHull("LINESTRING (1 1, 1 9, 2 2, 1 1, 4 0)", "POLYGON ((1 1, 4 0, 1 9, 1 1))");
        assertConvexHull("POLYGON ((0 0, 0 3, 4 4, 1 1, 3 0, 0 0))", "POLYGON ((0 0, 3 0, 4 4, 0 3, 0 0))");

        // all points are on the same line
        assertConvexHull("LINESTRING (20 20, 30 30)", "LINESTRING (20 20, 30 30)");
        assertConvexHull("MULTILINESTRING ((0 0, 3 3), (1 1, 2 2), (2 2, 4 4), (5 5, 8 8))", "LINESTRING (0 0, 8 8)");
        assertConvexHull("MULTIPOINT (0 1, 1 2, 2 3, 3 4, 4 5, 5 6)", "LINESTRING (0 1, 5 6)");
        assertConvexHull("GEOMETRYCOLLECTION (POINT (0 0), LINESTRING (1 1, 4 4, 2 2), POINT (10 10), LINESTRING (5 5, 7 7), POINT (2 2), LINESTRING (6 6, 9 9), POINT (1 1))", "LINESTRING (0 0, 10 10)");
        assertConvexHull("GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (2 2), POINT (1 1)), POINT (3 3))", "LINESTRING (3 3, 1 1)");

        // not all points are on the same line
        assertConvexHull("MULTILINESTRING ((1 1, 5 1, 6 6), (2 4, 4 0), (2 -4, 4 4), (3 -2, 4 -3))", "POLYGON ((1 1, 2 -4, 4 -3, 5 1, 6 6, 2 4, 1 1))");
        assertConvexHull("MULTIPOINT (0 2, 1 0, 3 0, 4 0, 4 2, 2 2, 2 4)", "POLYGON ((0 2, 1 0, 4 0, 4 2, 2 4, 0 2))");
        assertConvexHull("MULTIPOLYGON (((0 3, 2 0, 3 6, 0 3), (2 1, 2 3, 5 3, 5 1, 2 1), (1 7, 2 4, 4 2, 5 6, 3 8, 1 7)))", "POLYGON ((0 3, 2 0, 5 1, 5 6, 3 8, 1 7, 0 3))");
        assertConvexHull("GEOMETRYCOLLECTION (POINT (2 3), LINESTRING (2 8, 7 10), POINT (8 10), POLYGON ((4 4, 4 8, 9 8, 6 6, 6 4, 8 3, 6 1, 4 4)), POINT (4 2), LINESTRING (3 6, 5 5), POLYGON ((7 5, 7 6, 8 6, 8 5, 7 5)))", "POLYGON ((2 3, 6 1, 8 3, 9 8, 8 10, 7 10, 2 8, 2 3))");
        assertConvexHull("GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (2 3), LINESTRING (2 8, 7 10), GEOMETRYCOLLECTION (POINT (8 10))), POLYGON ((4 4, 4 8, 9 8, 6 6, 6 4, 8 3, 6 1, 4 4)), POINT (4 2), LINESTRING (3 6, 5 5), POLYGON ((7 5, 7 6, 8 6, 8 5, 7 5)))", "POLYGON ((2 3, 6 1, 8 3, 9 8, 8 10, 7 10, 2 8, 2 3))");

        // single-element multi-geometries and geometry collections
        assertConvexHull("MULTILINESTRING ((1 1, 5 1, 6 6))", "POLYGON ((1 1, 5 1, 6 6, 1 1))");
        assertConvexHull("MULTILINESTRING ((1 1, 5 1, 1 4, 5 4))", "POLYGON ((1 1, 5 1, 5 4, 1 4, 1 1))");
        assertConvexHull("MULTIPOINT (0 2)", "POINT (0 2)");
        assertConvexHull("MULTIPOLYGON (((0 3, 2 0, 3 6, 0 3)))", "POLYGON ((0 3, 2 0, 3 6, 0 3))");
        assertConvexHull("MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 2 2, 0 0)))", "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))");
        assertConvexHull("GEOMETRYCOLLECTION (POINT (2 3))", "POINT (2 3)");
        assertConvexHull("GEOMETRYCOLLECTION (LINESTRING (1 1, 5 1, 6 6))", "POLYGON ((1 1, 5 1, 6 6, 1 1))");
        assertConvexHull("GEOMETRYCOLLECTION (LINESTRING (1 1, 5 1, 1 4, 5 4))", "POLYGON ((1 1, 5 1, 5 4, 1 4, 1 1))");
        assertConvexHull("GEOMETRYCOLLECTION (POLYGON ((0 3, 2 0, 3 6, 0 3)))", "POLYGON ((0 3, 2 0, 3 6, 0 3))");
        assertConvexHull("GEOMETRYCOLLECTION (POLYGON ((0 0, 4 0, 4 4, 0 4, 2 2, 0 0)))", "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))");
    }

    private void assertConvexHull(String inputWKT, String expectWKT)
    {
        assertSpatialEquals(
                assertions,
                "ST_ConvexHull(ST_GeometryFromText('%s'))".formatted(inputWKT),
                expectWKT);
    }

    @Test
    public void testSTCoordDim()
    {
        assertThat(assertions.function("ST_CoordDim", "ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))')"))
                .isEqualTo((byte) 2);

        assertThat(assertions.function("ST_CoordDim", "ST_GeometryFromText('POLYGON EMPTY')"))
                .isEqualTo((byte) 2);

        assertThat(assertions.function("ST_CoordDim", "ST_GeometryFromText('LINESTRING EMPTY')"))
                .isEqualTo((byte) 2);

        assertThat(assertions.function("ST_CoordDim", "ST_GeometryFromText('POINT (1 4)')"))
                .isEqualTo((byte) 2);
    }

    @Test
    public void testSTDimension()
    {
        assertThat(assertions.function("ST_Dimension", "ST_GeometryFromText('POLYGON EMPTY')"))
                .isEqualTo((byte) 2);

        assertThat(assertions.function("ST_Dimension", "ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))')"))
                .isEqualTo((byte) 2);

        assertThat(assertions.function("ST_Dimension", "ST_GeometryFromText('LINESTRING EMPTY')"))
                .isEqualTo((byte) 1);

        assertThat(assertions.function("ST_Dimension", "ST_GeometryFromText('POINT (1 4)')"))
                .isEqualTo((byte) 0);
    }

    @Test
    public void testSTIsClosed()
    {
        assertThat(assertions.function("ST_IsClosed", "ST_GeometryFromText('LINESTRING (1 1, 2 2, 1 3, 1 1)')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_IsClosed", "ST_GeometryFromText('LINESTRING (1 1, 2 2, 1 3)')"))
                .isEqualTo(false);

        assertTrinoExceptionThrownBy(assertions.function("ST_IsClosed", "ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))')")::evaluate)
                .hasMessage("ST_IsClosed only applies to LINE_STRING or MULTI_LINE_STRING. Input type is: POLYGON");
    }

    @Test
    public void testSTIsEmpty()
    {
        assertThat(assertions.function("ST_IsEmpty", "ST_GeometryFromText('POINT (1.5 2.5)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_IsEmpty", "ST_GeometryFromText('POLYGON EMPTY')"))
                .isEqualTo(true);
    }

    private void assertSimpleGeometry(String text)
    {
        assertThat(assertions.function("ST_IsSimple", "ST_GeometryFromText('%s')".formatted(text)))
                .isEqualTo(true);
    }

    private void assertNotSimpleGeometry(String text)
    {
        assertThat(assertions.function("ST_IsSimple", "ST_GeometryFromText('%s')".formatted(text)))
                .isEqualTo(false);
    }

    @Test
    public void testSTIsSimple()
    {
        assertSimpleGeometry("POINT (1.5 2.5)");
        assertSimpleGeometry("MULTIPOINT (1 2, 2 4, 3 6, 4 8)");
        assertNotSimpleGeometry("MULTIPOINT (1 2, 2 4, 3 6, 1 2)");
        assertSimpleGeometry("LINESTRING (8 4, 5 7)");
        assertSimpleGeometry("LINESTRING (1 1, 2 2, 1 3, 1 1)");
        assertNotSimpleGeometry("LINESTRING (0 0, 1 1, 1 0, 0 1)");
        assertSimpleGeometry("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))");
        assertNotSimpleGeometry("MULTILINESTRING ((1 1, 5 1), (2 4, 4 0))");
        assertSimpleGeometry("POLYGON EMPTY");
        assertSimpleGeometry("POLYGON ((2 0, 2 1, 3 1, 2 0))");
        assertSimpleGeometry("MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 6 4, 2 4)))");
    }

    @Test
    public void testSimplifyGeometry()
    {
        // Eliminate unnecessary points on the same line.
        assertThat(assertions.function("ST_AsText", "simplify_geometry(ST_GeometryFromText('POLYGON ((1 0, 2 1, 3 1, 3 1, 4 1, 1 0))'), 1.5)"))
                .hasType(VARCHAR)
                .isEqualTo("POLYGON ((1 0, 4 1, 2 1, 1 0))");

        // Use distanceTolerance to control fidelity.
        assertThat(assertions.function("ST_AsText", "simplify_geometry(ST_GeometryFromText('POLYGON ((1 0, 1 1, 2 1, 2 3, 3 3, 3 1, 4 1, 4 0, 1 0))'), 1.0)"))
                .hasType(VARCHAR)
                .isEqualTo("POLYGON ((1 0, 4 0, 3 3, 2 3, 1 0))");

        assertThat(assertions.function("ST_AsText", "simplify_geometry(ST_GeometryFromText('POLYGON ((1 0, 1 1, 2 1, 2 3, 3 3, 3 1, 4 1, 4 0, 1 0))'), 0.5)"))
                .hasType(VARCHAR)
                .isEqualTo("POLYGON ((1 0, 4 0, 4 1, 3 1, 3 3, 2 3, 2 1, 1 1, 1 0))");

        // Negative distance tolerance is invalid.
        assertTrinoExceptionThrownBy(assertions.function("ST_AsText", "simplify_geometry(ST_GeometryFromText('POLYGON ((1 0, 1 1, 2 1, 2 3, 3 3, 3 1, 4 1, 4 0, 1 0))'), -0.5)")::evaluate)
                .hasMessage("distanceTolerance is negative");
    }

    @Test
    public void testSTIsValid()
    {
        // empty geometries are valid
        assertValidGeometry("POINT EMPTY");
        assertValidGeometry("MULTIPOINT EMPTY");
        assertValidGeometry("LINESTRING EMPTY");
        assertValidGeometry("MULTILINESTRING EMPTY");
        assertValidGeometry("POLYGON EMPTY");
        assertValidGeometry("MULTIPOLYGON EMPTY");
        assertValidGeometry("GEOMETRYCOLLECTION EMPTY");

        // valid geometries
        assertValidGeometry("POINT (1 2)");
        assertValidGeometry("MULTIPOINT (1 2, 3 4)");
        assertValidGeometry("LINESTRING (0 0, 1 2, 3 4)");
        assertValidGeometry("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))");
        assertValidGeometry("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))");
        assertValidGeometry("MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 6 4, 2 4)))");
        assertValidGeometry("GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 2, 3 4), POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0)))");

        // invalid geometries
        assertInvalidGeometry("MULTIPOINT ((0 0), (0 1), (1 1), (0 1))", "Repeated points at or near (0.0 1.0) and (0.0 1.0)");
        assertInvalidGeometry("LINESTRING (0 0, 0 1, 0 1, 1 1, 1 0, 0 0)", "Degenerate segments at or near (0.0 1.0)");
        assertInvalidGeometry("LINESTRING (0 0, -1 0.5, 0 1, 1 1, 1 0, 0 1, 0 0)", "Self-tangency at or near (0.0 1.0) and (0.0 1.0)");
        assertInvalidGeometry("POLYGON ((0 0, 1 1, 0 1, 1 0, 0 0))", "Intersecting or overlapping segments at or near (1.0 0.0) and (1.0 1.0)");
        assertInvalidGeometry("POLYGON ((0 0, 0 1, 0 1, 1 1, 1 0, 0 0), (2 2, 2 3, 3 3, 3 2, 2 2))", "Degenerate segments at or near (0.0 1.0)");
        assertInvalidGeometry("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (2 2, 2 3, 3 3, 3 2, 2 2))", "RingOrientation");
        assertInvalidGeometry("POLYGON ((0 0, 0 1, 2 1, 1 1, 1 0, 0 0))", "Intersecting or overlapping segments at or near (0.0 1.0) and (2.0 1.0)");
        assertInvalidGeometry("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0 1, 1 1, 0.5 0.5, 0 1))", "Self-intersection at or near (0.0 1.0) and (1.0 1.0)");
        assertInvalidGeometry("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0 0, 0.5 0.7, 1 1, 0.5 0.4, 0 0))", "Disconnected interior at or near (0.0 1.0)");
        assertInvalidGeometry("POLYGON ((0 0, -1 0.5, 0 1, 1 1, 1 0, 0 1, 0 0))", "Self-tangency at or near (0.0 1.0) and (0.0 1.0)");
        assertInvalidGeometry("MULTIPOLYGON (((0 0, 0 1, 1 1, 1 0, 0 0)), ((0.5 0.5, 0.5 2, 2 2, 2 0.5, 0.5 0.5)))", "Intersecting or overlapping segments at or near (0.0 1.0) and (0.5 0.5)");
        assertInvalidGeometry("GEOMETRYCOLLECTION (POINT (1 2), POLYGON ((0 0, 0 1, 2 1, 1 1, 1 0, 0 0)))", "Intersecting or overlapping segments at or near (0.0 1.0) and (2.0 1.0)");

        // corner cases
        assertThat(assertions.function("ST_IsValid", "ST_GeometryFromText(null)"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("geometry_invalid_reason", "ST_GeometryFromText(null)"))
                .isNull(VARCHAR);
    }

    private void assertValidGeometry(String wkt)
    {
        assertThat(assertions.function("ST_IsValid", "ST_GeometryFromText('%s')".formatted(wkt)))
                .isEqualTo(true);

        assertThat(assertions.function("geometry_invalid_reason", "ST_GeometryFromText('%s')".formatted(wkt)))
                .isNull(VARCHAR);
    }

    private void assertInvalidGeometry(String wkt, String reason)
    {
        assertThat(assertions.function("ST_IsValid", "ST_GeometryFromText('%s')".formatted(wkt)))
                .isEqualTo(false);

        assertThat(assertions.function("geometry_invalid_reason", "ST_GeometryFromText('%s')".formatted(wkt)))
                .hasType(VARCHAR)
                .isEqualTo(reason);
    }

    @Test
    public void testSTLength()
    {
        assertThat(assertions.function("ST_Length", "ST_GeometryFromText('LINESTRING EMPTY')"))
                .isEqualTo(0.0);

        assertThat(assertions.function("ST_Length", "ST_GeometryFromText('LINESTRING (0 0, 2 2)')"))
                .isEqualTo(2.8284271247461903);

        assertThat(assertions.function("ST_Length", "ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))')"))
                .isEqualTo(6.0);

        assertTrinoExceptionThrownBy(assertions.function("ST_Length", "ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))')")::evaluate)
                .hasMessage("ST_Length only applies to LINE_STRING or MULTI_LINE_STRING. Input type is: POLYGON");
    }

    @Test
    public void testSTLengthSphericalGeography()
    {
        // Empty linestring returns null
        assertSTLengthSphericalGeography("LINESTRING EMPTY", null);

        // Linestring with only one distinct point has length 0
        assertSTLengthSphericalGeography("LINESTRING (0 0, 0 0)", 0.0);
        assertSTLengthSphericalGeography("LINESTRING (0 0, 0 0, 0 0)", 0.0);

        double length = 4350866.6362;

        // ST_Length is equivalent to sums of ST_DISTANCE between points in the LineString
        assertSTLengthSphericalGeography("LINESTRING (-71.05 42.36, -87.62 41.87, -122.41 37.77)", length);

        // Linestring has same length as its reverse
        assertSTLengthSphericalGeography("LINESTRING (-122.41 37.77, -87.62 41.87, -71.05 42.36)", length);

        // Path north pole -> south pole -> north pole should be roughly the circumference of the Earth
        assertSTLengthSphericalGeography("LINESTRING (0.0 90.0, 0.0 -90.0, 0.0 90.0)", 4.003e7);

        // Empty multi-linestring returns null
        assertSTLengthSphericalGeography("MULTILINESTRING EMPTY", null);

        // Multi-linestring with one path is equivalent to a single linestring
        assertSTLengthSphericalGeography("MULTILINESTRING ((-71.05 42.36, -87.62 41.87, -122.41 37.77))", length);

        // Multi-linestring with two disjoint paths has length equal to sum of lengths of lines
        assertSTLengthSphericalGeography("MULTILINESTRING ((-71.05 42.36, -87.62 41.87, -122.41 37.77), (-73.05 42.36, -89.62 41.87, -124.41 37.77))", 2 * length);

        // Multi-linestring with adjacent paths is equivalent to a single linestring
        assertSTLengthSphericalGeography("MULTILINESTRING ((-71.05 42.36, -87.62 41.87), (-87.62 41.87, -122.41 37.77))", length);
    }

    private void assertSTLengthSphericalGeography(String lineString, Double expectedLength)
    {
        if (expectedLength == null || expectedLength == 0.0) {
            assertThat(assertions.function("ST_Length", "to_spherical_geography(ST_GeometryFromText('%s'))".formatted(lineString)))
                    .isEqualTo(expectedLength);
        }
        else {
            assertThat(assertions.expression("ROUND(ABS((ST_Length(to_spherical_geography(ST_GeometryFromText('%s'))) / %f) - 1.0) / %f, 0)".formatted(lineString, expectedLength, 1e-4)))
                    .isEqualTo(0.0);
        }
    }

    @Test
    public void testLineLocatePoint()
    {
        assertThat(assertions.function("line_locate_point", "ST_GeometryFromText('LINESTRING (0 0, 0 1)')", "ST_Point(0, 0.2)"))
                .isEqualTo(0.2);

        assertThat(assertions.function("line_locate_point", "ST_GeometryFromText('LINESTRING (0 0, 0 1)')", "ST_Point(0, 0)"))
                .isEqualTo(0.0);

        assertThat(assertions.function("line_locate_point", "ST_GeometryFromText('LINESTRING (0 0, 0 1)')", "ST_Point(0, -1)"))
                .isEqualTo(0.0);

        assertThat(assertions.function("line_locate_point", "ST_GeometryFromText('LINESTRING (0 0, 0 1)')", "ST_Point(0, 1)"))
                .isEqualTo(1.0);

        assertThat(assertions.function("line_locate_point", "ST_GeometryFromText('LINESTRING (0 0, 0 1)')", "ST_Point(0, 2)"))
                .isEqualTo(1.0);

        assertThat(assertions.function("line_locate_point", "ST_GeometryFromText('LINESTRING (0 0, 0 1, 2 1)')", "ST_Point(0, 0.2)"))
                .isEqualTo(0.06666666666666667);

        assertThat(assertions.function("line_locate_point", "ST_GeometryFromText('LINESTRING (0 0, 0 1, 2 1)')", "ST_Point(0.9, 1)"))
                .isEqualTo(0.6333333333333333);

        assertThat(assertions.function("line_locate_point", "ST_GeometryFromText('LINESTRING (1 3, 5 4)')", "ST_Point(1, 3)"))
                .isEqualTo(0.0);

        assertThat(assertions.function("line_locate_point", "ST_GeometryFromText('LINESTRING (1 3, 5 4)')", "ST_Point(2, 3)"))
                .isEqualTo(0.23529411764705882);

        assertThat(assertions.function("line_locate_point", "ST_GeometryFromText('LINESTRING (1 3, 5 4)')", "ST_Point(5, 4)"))
                .isEqualTo(1.0);

        assertThat(assertions.function("line_locate_point", "ST_GeometryFromText('MULTILINESTRING ((0 0, 0 1), (2 2, 4 2))')", "ST_Point(3, 1)"))
                .isEqualTo(0.6666666666666666);

        assertThat(assertions.function("line_locate_point", "ST_GeometryFromText('LINESTRING EMPTY')", "ST_Point(0, 1)"))
                .isNull(DOUBLE);

        assertThat(assertions.function("line_locate_point", "ST_GeometryFromText('LINESTRING (0 0, 0 1, 2 1)')", "ST_GeometryFromText('POINT EMPTY')"))
                .isNull(DOUBLE);

        assertTrinoExceptionThrownBy(assertions.function("line_locate_point", "ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))')", "ST_Point(0.4, 1)")::evaluate)
                .hasMessage("First argument to line_locate_point must be a LineString or a MultiLineString. Got: Polygon");

        assertTrinoExceptionThrownBy(assertions.function("line_locate_point", "ST_GeometryFromText('LINESTRING (0 0, 0 1, 2 1)')", "ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))')")::evaluate)
                .hasMessage("Second argument to line_locate_point must be a Point. Got: Polygon");
    }

    @Test
    public void testLineInterpolatePoint()
    {
        assertThat(assertions.function("ST_AsText", "line_interpolate_point(ST_GeometryFromText('LINESTRING EMPTY'), 0.5)"))
                .isNull(VARCHAR);

        assertLineInterpolatePoint("LINESTRING (0 0, 1 1, 10 10)", 0.0, "POINT (0 0)");
        assertLineInterpolatePoint("LINESTRING (0 0, 1 1, 10 10)", 0.1, "POINT (1 1)");
        assertLineInterpolatePoint("LINESTRING (0 0, 1 1, 10 10)", 0.05, "POINT (0.5 0.5)");
        assertLineInterpolatePoint("LINESTRING (0 0, 1 1, 10 10)", 0.4, "POINT (4.000000000000001 4.000000000000001)");
        assertLineInterpolatePoint("LINESTRING (0 0, 1 1, 10 10)", 1.0, "POINT (10 10)");

        assertLineInterpolatePoint("LINESTRING (0 0, 1 1)", 0.0, "POINT (0 0)");
        assertLineInterpolatePoint("LINESTRING (0 0, 1 1)", 0.1, "POINT (0.1 0.1)");
        assertLineInterpolatePoint("LINESTRING (0 0, 1 1)", 0.05, "POINT (0.05 0.05)");
        assertLineInterpolatePoint("LINESTRING (0 0, 1 1)", 0.4, "POINT (0.4 0.4)");
        assertLineInterpolatePoint("LINESTRING (0 0, 1 1)", 1.0, "POINT (1 1)");

        assertLineInterpolatePoint("LINESTRING (0 0, 1 0, 1 9)", 0.0, "POINT (0 0)");
        assertLineInterpolatePoint("LINESTRING (0 0, 1 0, 1 9)", 0.05, "POINT (0.5 0)");
        assertLineInterpolatePoint("LINESTRING (0 0, 1 0, 1 9)", 0.1, "POINT (1 0)");
        assertLineInterpolatePoint("LINESTRING (0 0, 1 0, 1 9)", 0.5, "POINT (1 4)");
        assertLineInterpolatePoint("LINESTRING (0 0, 1 0, 1 9)", 1.0, "POINT (1 9)");

        assertTrinoExceptionThrownBy(assertions.function("line_interpolate_point", "ST_GeometryFromText('LINESTRING (0 0, 1 0, 1 9)')", "-0.5")::evaluate)
                .hasMessage("fraction must be between 0 and 1");

        assertTrinoExceptionThrownBy(assertions.function("line_interpolate_point", "ST_GeometryFromText('LINESTRING (0 0, 1 0, 1 9)')", "2.0")::evaluate)
                .hasMessage("fraction must be between 0 and 1");

        assertTrinoExceptionThrownBy(assertions.function("line_interpolate_point", "ST_GeometryFromText('POLYGON ((0 0, 1 1, 0 1, 1 0, 0 0))')", "0.2")::evaluate)
                .hasMessage("line_interpolate_point only applies to LINE_STRING. Input type is: POLYGON");
    }

    @Test
    public void testLineInterpolatePoints()
    {
        assertThat(assertions.function("line_interpolate_points", "ST_GeometryFromText('LINESTRING EMPTY')", "0.5"))
                .isNull(new ArrayType(GEOMETRY));

        assertLineInterpolatePoints("LINESTRING (0 0, 1 1, 10 10)", 0.0, "0 0");
        assertLineInterpolatePoints("LINESTRING (0 0, 1 1, 10 10)", 0.4, "4.000000000000001 4.000000000000001", "8 8");
        assertLineInterpolatePoints("LINESTRING (0 0, 1 1, 10 10)", 0.3, "3 3", "6 6", "9 9");
        assertLineInterpolatePoints("LINESTRING (0 0, 1 1, 10 10)", 0.5, "5.000000000000001 5.000000000000001", "10 10");
        assertLineInterpolatePoints("LINESTRING (0 0, 1 1, 10 10)", 1, "10 10");

        assertTrinoExceptionThrownBy(assertions.function("line_interpolate_points", "ST_GeometryFromText('LINESTRING (0 0, 1 0, 1 9)')", "-0.5")::evaluate)
                .hasMessage("fraction must be between 0 and 1");

        assertTrinoExceptionThrownBy(assertions.function("line_interpolate_points", "ST_GeometryFromText('LINESTRING (0 0, 1 0, 1 9)')", "2.0")::evaluate)
                .hasMessage("fraction must be between 0 and 1");

        assertTrinoExceptionThrownBy(assertions.function("line_interpolate_points", "ST_GeometryFromText('POLYGON ((0 0, 1 1, 0 1, 1 0, 0 0))')", "0.2")::evaluate)
                .hasMessage("line_interpolate_point only applies to LINE_STRING. Input type is: POLYGON");
    }

    private void assertLineInterpolatePoint(String wkt, double fraction, String expectedPoint)
    {
        assertSpatialEquals(assertions,
                "line_interpolate_point(ST_GeometryFromText('%s'), %s)".formatted(wkt, fraction),
                expectedPoint);
    }

    private void assertLineInterpolatePoints(String wkt, double fraction, String... expectedCoords)
    {
        assertSpatialArrayEquals(assertions,
                "line_interpolate_points(ST_GeometryFromText('%s'), %s)".formatted(wkt, fraction),
                Arrays.stream(expectedCoords)
                        .map(s -> "POINT (" + s + ")")
                        .toArray(String[]::new));
    }

    @Test
    public void testSTMax()
    {
        assertThat(assertions.function("ST_XMax", "ST_GeometryFromText('POINT (1.5 2.5)')"))
                .isEqualTo(1.5);

        assertThat(assertions.function("ST_YMax", "ST_GeometryFromText('POINT (1.5 2.5)')"))
                .isEqualTo(2.5);

        assertThat(assertions.function("ST_XMax", "ST_GeometryFromText('MULTIPOINT (1 2, 2 4, 3 6, 4 8)')"))
                .isEqualTo(4.0);

        assertThat(assertions.function("ST_YMax", "ST_GeometryFromText('MULTIPOINT (1 2, 2 4, 3 6, 4 8)')"))
                .isEqualTo(8.0);

        assertThat(assertions.function("ST_XMax", "ST_GeometryFromText('LINESTRING (8 4, 5 7)')"))
                .isEqualTo(8.0);

        assertThat(assertions.function("ST_YMax", "ST_GeometryFromText('LINESTRING (8 4, 5 7)')"))
                .isEqualTo(7.0);

        assertThat(assertions.function("ST_XMax", "ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))')"))
                .isEqualTo(5.0);

        assertThat(assertions.function("ST_YMax", "ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))')"))
                .isEqualTo(4.0);

        assertThat(assertions.function("ST_XMax", "ST_GeometryFromText('POLYGON ((2 0, 2 1, 3 1, 2 0))')"))
                .isEqualTo(3.0);

        assertThat(assertions.function("ST_YMax", "ST_GeometryFromText('POLYGON ((2 0, 2 1, 3 1, 2 0))')"))
                .isEqualTo(1.0);

        assertThat(assertions.function("ST_XMax", "ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 6 4, 2 4)))')"))
                .isEqualTo(6.0);

        assertThat(assertions.function("ST_YMax", "ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 10, 6 4, 2 4)))')"))
                .isEqualTo(10.0);

        assertThat(assertions.function("ST_XMax", "ST_GeometryFromText('POLYGON EMPTY')"))
                .isNull(DOUBLE);

        assertThat(assertions.function("ST_YMax", "ST_GeometryFromText('POLYGON EMPTY')"))
                .isNull(DOUBLE);

        assertThat(assertions.function("ST_XMax", "ST_GeometryFromText('GEOMETRYCOLLECTION (POINT (5 1), LINESTRING (3 4, 4 4))')"))
                .isEqualTo(5.0);

        assertThat(assertions.function("ST_YMax", "ST_GeometryFromText('GEOMETRYCOLLECTION (POINT (5 1), LINESTRING (3 4, 4 4))')"))
                .isEqualTo(4.0);

        assertThat(assertions.function("ST_XMax", "null"))
                .isNull(DOUBLE);

        assertThat(assertions.function("ST_YMax", "null"))
                .isNull(DOUBLE);
    }

    @Test
    public void testSTMin()
    {
        assertThat(assertions.function("ST_XMin", "ST_GeometryFromText('POINT (1.5 2.5)')"))
                .isEqualTo(1.5);

        assertThat(assertions.function("ST_YMin", "ST_GeometryFromText('POINT (1.5 2.5)')"))
                .isEqualTo(2.5);

        assertThat(assertions.function("ST_XMin", "ST_GeometryFromText('MULTIPOINT (1 2, 2 4, 3 6, 4 8)')"))
                .isEqualTo(1.0);

        assertThat(assertions.function("ST_YMin", "ST_GeometryFromText('MULTIPOINT (1 2, 2 4, 3 6, 4 8)')"))
                .isEqualTo(2.0);

        assertThat(assertions.function("ST_XMin", "ST_GeometryFromText('LINESTRING (8 4, 5 7)')"))
                .isEqualTo(5.0);

        assertThat(assertions.function("ST_YMin", "ST_GeometryFromText('LINESTRING (8 4, 5 7)')"))
                .isEqualTo(4.0);

        assertThat(assertions.function("ST_XMin", "ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))')"))
                .isEqualTo(1.0);

        assertThat(assertions.function("ST_YMin", "ST_GeometryFromText('MULTILINESTRING ((1 2, 5 3), (2 4, 4 4))')"))
                .isEqualTo(2.0);

        assertThat(assertions.function("ST_XMin", "ST_GeometryFromText('POLYGON ((2 0, 2 1, 3 1, 2 0))')"))
                .isEqualTo(2.0);

        assertThat(assertions.function("ST_YMin", "ST_GeometryFromText('POLYGON ((2 0, 2 1, 3 1, 2 0))')"))
                .isEqualTo(0.0);

        assertThat(assertions.function("ST_XMin", "ST_GeometryFromText('MULTIPOLYGON (((1 10, 1 3, 3 3, 3 10, 1 10)), ((2 4, 2 6, 6 6, 6 4, 2 4)))')"))
                .isEqualTo(1.0);

        assertThat(assertions.function("ST_YMin", "ST_GeometryFromText('MULTIPOLYGON (((1 10, 1 3, 3 3, 3 10, 1 10)), ((2 4, 2 6, 6 10, 6 4, 2 4)))')"))
                .isEqualTo(3.0);

        assertThat(assertions.function("ST_XMin", "ST_GeometryFromText('POLYGON EMPTY')"))
                .isNull(DOUBLE);

        assertThat(assertions.function("ST_YMin", "ST_GeometryFromText('POLYGON EMPTY')"))
                .isNull(DOUBLE);

        assertThat(assertions.function("ST_XMin", "ST_GeometryFromText('GEOMETRYCOLLECTION (POINT (5 1), LINESTRING (3 4, 4 4))')"))
                .isEqualTo(3.0);

        assertThat(assertions.function("ST_YMin", "ST_GeometryFromText('GEOMETRYCOLLECTION (POINT (5 1), LINESTRING (3 4, 4 4))')"))
                .isEqualTo(1.0);

        assertThat(assertions.function("ST_XMin", "null"))
                .isNull(DOUBLE);

        assertThat(assertions.function("ST_YMin", "null"))
                .isNull(DOUBLE);
    }

    @Test
    public void testSTNumInteriorRing()
    {
        assertThat(assertions.function("ST_NumInteriorRing", "ST_GeometryFromText('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))')"))
                .isEqualTo(0L);

        assertThat(assertions.function("ST_NumInteriorRing", "ST_GeometryFromText('POLYGON ((0 0, 8 0, 0 8, 0 0), (1 1, 1 5, 5 1, 1 1))')"))
                .isEqualTo(1L);

        assertTrinoExceptionThrownBy(assertions.function("ST_NumInteriorRing", "ST_GeometryFromText('LINESTRING (8 4, 5 7)')")::evaluate)
                .hasMessage("ST_NumInteriorRing only applies to POLYGON. Input type is: LINE_STRING");
    }

    @Test
    public void testSTNumPoints()
    {
        assertNumPoints("POINT EMPTY", 0);
        assertNumPoints("MULTIPOINT EMPTY", 0);
        assertNumPoints("LINESTRING EMPTY", 0);
        assertNumPoints("MULTILINESTRING EMPTY", 0);
        assertNumPoints("POLYGON EMPTY", 0);
        assertNumPoints("MULTIPOLYGON EMPTY", 0);
        assertNumPoints("GEOMETRYCOLLECTION EMPTY", 0);

        assertNumPoints("POINT (1 2)", 1);
        assertNumPoints("MULTIPOINT (1 2, 2 4, 3 6, 4 8)", 4);
        assertNumPoints("LINESTRING (8 4, 5 7)", 2);
        assertNumPoints("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", 4);
        assertNumPoints("POLYGON ((0 0, 8 0, 0 8, 0 0), (1 1, 1 5, 5 1, 1 1))", 8);
        assertNumPoints("MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 6 4, 2 4)))", 10);
        assertNumPoints("GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (8 4, 5 7), POLYGON EMPTY)", 3);
    }

    private void assertNumPoints(String wkt, int expectedPoints)
    {
        assertThat(assertions.function("ST_NumPoints", "ST_GeometryFromText('%s')".formatted(wkt)))
                .isEqualTo((long) expectedPoints);
    }

    @Test
    public void testSTIsRing()
    {
        assertThat(assertions.function("ST_IsRing", "ST_GeometryFromText('LINESTRING (8 4, 4 8)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_IsRing", "ST_GeometryFromText('LINESTRING (0 0, 1 1, 0 2, 0 0)')"))
                .isEqualTo(true);

        assertTrinoExceptionThrownBy(assertions.function("ST_IsRing", "ST_GeometryFromText('POLYGON ((2 0, 2 1, 3 1, 2 0))')")::evaluate)
                .hasMessage("ST_IsRing only applies to LINE_STRING. Input type is: POLYGON");
    }

    @Test
    public void testSTStartEndPoint()
    {
        assertSpatialEquals(assertions,
                "ST_StartPoint(ST_GeometryFromText('LINESTRING (8 4, 4 8, 5 6)'))",
                "POINT (8 4)");

        assertSpatialEquals(assertions,
                "ST_EndPoint(ST_GeometryFromText('LINESTRING (8 4, 4 8, 5 6)'))",
                "POINT (5 6)");

        assertTrinoExceptionThrownBy(assertions.function("ST_AsText", "ST_StartPoint(ST_GeometryFromText('POLYGON ((2 0, 2 1, 3 1, 2 0))'))")::evaluate)
                .hasMessage("ST_StartPoint only applies to LINE_STRING. Input type is: POLYGON");

        assertTrinoExceptionThrownBy(assertions.function("ST_AsText", "ST_EndPoint(ST_GeometryFromText('POLYGON ((2 0, 2 1, 3 1, 2 0))'))")::evaluate)
                .hasMessage("ST_EndPoint only applies to LINE_STRING. Input type is: POLYGON");
    }

    @Test
    public void testSTPoints()
    {
        assertThat(assertions.function("ST_Points", "ST_GeometryFromText('LINESTRING EMPTY')"))
                .isNull(new ArrayType(GEOMETRY));

        assertSTPoints("LINESTRING (0 0, 0 0)", "0 0", "0 0");
        assertSTPoints("LINESTRING (8 4, 3 9, 8 4)", "8 4", "3 9", "8 4");
        assertSTPoints("LINESTRING (8 4, 3 9, 5 6)", "8 4", "3 9", "5 6");
        assertSTPoints("LINESTRING (8 4, 3 9, 5 6, 3 9, 8 4)", "8 4", "3 9", "5 6", "3 9", "8 4");

        assertThat(assertions.function("ST_Points", "ST_GeometryFromText('POLYGON EMPTY')"))
                .isNull(new ArrayType(GEOMETRY));

        assertSTPoints("POLYGON ((8 4, 3 9, 5 6, 8 4))", "8 4", "5 6", "3 9", "8 4");
        assertSTPoints("POLYGON ((8 4, 3 9, 5 6, 7 2, 8 4))", "8 4", "7 2", "5 6", "3 9", "8 4");

        assertThat(assertions.function("ST_Points", "ST_GeometryFromText('POINT EMPTY')"))
                .isNull(new ArrayType(GEOMETRY));

        assertSTPoints("POINT (0 0)", "0 0");
        assertSTPoints("POINT (0 1)", "0 1");

        assertThat(assertions.function("ST_Points", "ST_GeometryFromText('MULTIPOINT EMPTY')"))
                .isNull(new ArrayType(GEOMETRY));

        assertSTPoints("MULTIPOINT (0 0)", "0 0");
        assertSTPoints("MULTIPOINT (0 0, 1 2)", "0 0", "1 2");

        assertThat(assertions.function("ST_Points", "ST_GeometryFromText('MULTILINESTRING EMPTY')"))
                .isNull(new ArrayType(GEOMETRY));

        assertSTPoints("MULTILINESTRING ((0 0, 1 1), (2 3, 3 2))", "0 0", "1 1", "2 3", "3 2");
        assertSTPoints("MULTILINESTRING ((0 0, 1 1, 1 2), (2 3, 3 2, 5 4))", "0 0", "1 1", "1 2", "2 3", "3 2", "5 4");
        assertSTPoints("MULTILINESTRING ((0 0, 1 1, 1 2), (1 2, 3 2, 5 4))", "0 0", "1 1", "1 2", "1 2", "3 2", "5 4");

        assertThat(assertions.function("ST_Points", "ST_GeometryFromText('MULTIPOLYGON EMPTY')"))
                .isNull(new ArrayType(GEOMETRY));

        assertSTPoints("MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1)), ((-1 -1, -1 -2, -2 -2, -2 -1, -1 -1)))",
                "0 0", "0 4", "4 4", "4 0", "0 0",
                "1 1", "2 1", "2 2", "1 2", "1 1",
                "-1 -1", "-1 -2", "-2 -2", "-2 -1", "-1 -1");

        assertThat(assertions.function("ST_Points", "ST_GeometryFromText('GEOMETRYCOLLECTION EMPTY')"))
                .isNull(new ArrayType(GEOMETRY));

        String newLine = System.getProperty("line.separator");
        String geometryCollection = String.join(newLine,
                "GEOMETRYCOLLECTION(",
                "          POINT ( 0 1 ),",
                "          LINESTRING ( 0 3, 3 4 ),",
                "          POLYGON (( 2 0, 2 3, 0 2, 2 0 )),",
                "          POLYGON (( 3 0, 3 3, 6 3, 6 0, 3 0 ),",
                "                   ( 5 1, 4 2, 5 2, 5 1 )),",
                "          MULTIPOLYGON (",
                "                  (( 0 5, 0 8, 4 8, 4 5, 0 5 ),",
                "                   ( 1 6, 3 6, 2 7, 1 6 )),",
                "                  (( 5 4, 5 8, 6 7, 5 4 ))",
                "           )",
                ")");
        assertSTPoints(geometryCollection, "0 1", "0 3", "3 4", "2 0", "0 2", "2 3", "2 0", "3 0", "3 3", "6 3", "6 0", "3 0",
                "5 1", "5 2", "4 2", "5 1", "0 5", "0 8", "4 8", "4 5", "0 5", "1 6", "3 6", "2 7", "1 6", "5 4", "5 8", "6 7", "5 4");
    }

    private void assertSTPoints(String wkt, String... expected)
    {
        assertSpatialArrayEquals(assertions,
                "ST_Points(ST_GeometryFromText('%s'))".formatted(wkt),
                Arrays.stream(expected).map(s -> "POINT (" + s + ")").toArray(String[]::new));
    }

    @Test
    public void testSTXY()
    {
        assertThat(assertions.function("ST_Y", "ST_GeometryFromText('POINT EMPTY')"))
                .isNull(DOUBLE);

        assertThat(assertions.function("ST_X", "ST_GeometryFromText('POINT (1 2)')"))
                .isEqualTo(1.0);

        assertThat(assertions.function("ST_Y", "ST_GeometryFromText('POINT (1 2)')"))
                .isEqualTo(2.0);

        assertTrinoExceptionThrownBy(assertions.function("ST_Y", "ST_GeometryFromText('POLYGON ((2 0, 2 1, 3 1, 2 0))')")::evaluate)
                .hasMessage("ST_Y only applies to POINT. Input type is: POLYGON");
    }

    @Test
    public void testSTBoundary()
    {
        assertSpatialEquals(assertions,
                "ST_Boundary(ST_GeometryFromText('POINT (1 2)'))",
                "MULTIPOINT EMPTY");

        assertSpatialEquals(assertions,
                "ST_Boundary(ST_GeometryFromText('MULTIPOINT (1 2, 2 4, 3 6, 4 8)'))",
                "MULTIPOINT EMPTY");

        assertSpatialEquals(assertions,
                "ST_Boundary(ST_GeometryFromText('LINESTRING EMPTY'))",
                "MULTIPOINT EMPTY");

        assertSpatialEquals(assertions,
                "ST_Boundary(ST_GeometryFromText('LINESTRING (8 4, 5 7)'))",
                "MULTIPOINT ((8 4), (5 7))");

        assertSpatialEquals(assertions,
                "ST_Boundary(ST_GeometryFromText('LINESTRING (100 150,50 60, 70 80, 160 170)'))",
                "MULTIPOINT ((100 150), (160 170))");

        assertSpatialEquals(assertions,
                "ST_Boundary(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'))",
                "MULTIPOINT ((1 1), (5 1), (2 4), (4 4))");

        assertSpatialEquals(assertions,
                "ST_Boundary(ST_GeometryFromText('POLYGON ((1 1, 4 1, 1 4, 1 1))'))",
                "LINESTRING (1 1, 4 1, 1 4, 1 1)");

        assertSpatialEquals(assertions,
                "ST_Boundary(ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))'))",
                "MULTILINESTRING ((1 1, 3 1, 3 3, 1 3, 1 1), (0 0, 2 0, 2 2, 0 2, 0 0))");
    }

    @Test
    public void testSTEnvelope()
    {
        assertSpatialEquals(assertions,
                "ST_Envelope(ST_GeometryFromText('MULTIPOINT (1 2, 2 4, 3 6, 4 8)'))",
                "POLYGON ((1 2, 4 2, 4 8, 1 8, 1 2))");

        assertSpatialEquals(assertions,
                "ST_Envelope(ST_GeometryFromText('LINESTRING EMPTY'))",
                "POLYGON EMPTY");

        assertSpatialEquals(assertions,
                "ST_Envelope(ST_GeometryFromText('LINESTRING (1 1, 2 2, 1 3)'))",
                "POLYGON ((1 1, 2 1, 2 3, 1 3, 1 1))");

        assertSpatialEquals(assertions,
                "ST_Envelope(ST_GeometryFromText('LINESTRING (8 4, 5 7)'))",
                "POLYGON ((5 4, 8 4, 8 7, 5 7, 5 4))");

        assertSpatialEquals(assertions,
                "ST_Envelope(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'))",
                "POLYGON ((1 1, 5 1, 5 4, 1 4, 1 1))");

        assertSpatialEquals(assertions,
                "ST_Envelope(ST_GeometryFromText('POLYGON ((1 1, 4 1, 1 4, 1 1))'))",
                "POLYGON ((1 1, 4 1, 4 4, 1 4, 1 1))");

        assertSpatialEquals(assertions,
                "ST_Envelope(ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))'))",
                "POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0))");

        assertSpatialEquals(assertions,
                "ST_Envelope(ST_GeometryFromText('GEOMETRYCOLLECTION (POINT (5 1), LINESTRING (3 4, 4 4))'))",
                "POLYGON ((3 1, 5 1, 5 4, 3 4, 3 1))");
    }

    @Test
    public void testSTEnvelopeAsPts()
    {
        assertEnvelopeAsPts("MULTIPOINT (1 2, 2 4, 3 6, 4 8)", new Point(1, 2), new Point(4, 8));
        assertThat(assertions.function("ST_EnvelopeAsPts", "ST_GeometryFromText('LINESTRING EMPTY')"))
                .isNull(new ArrayType(GEOMETRY));

        assertEnvelopeAsPts("LINESTRING (1 1, 2 2, 1 3)", new Point(1, 1), new Point(2, 3));
        assertEnvelopeAsPts("LINESTRING (8 4, 5 7)", new Point(5, 4), new Point(8, 7));
        assertEnvelopeAsPts("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", new Point(1, 1), new Point(5, 4));
        assertEnvelopeAsPts("POLYGON ((1 1, 4 1, 1 4, 1 1))", new Point(1, 1), new Point(4, 4));
        assertEnvelopeAsPts("MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))", new Point(0, 0), new Point(3, 3));
        assertEnvelopeAsPts("GEOMETRYCOLLECTION (POINT (5 1), LINESTRING (3 4, 4 4))", new Point(3, 1), new Point(5, 4));
        assertEnvelopeAsPts("POINT (1 2)", new Point(1, 2), new Point(1, 2));
    }

    private void assertEnvelopeAsPts(String wkt, Point lowerLeftCorner, Point upperRightCorner)
    {
        assertSpatialArrayEquals(assertions,
                "ST_EnvelopeAsPts(ST_GeometryFromText('%s'))".formatted(wkt),
                new OGCPoint(lowerLeftCorner, null).asText(),
                new OGCPoint(upperRightCorner, null).asText());
    }

    @Test
    public void testSTDifference()
    {
        assertSpatialEquals(assertions,
                "ST_Difference(ST_GeometryFromText('POINT (50 100)'), ST_GeometryFromText('POINT (150 150)'))",
                "POINT (50 100)");

        assertSpatialEquals(assertions,
                "ST_Difference(ST_GeometryFromText('MULTIPOINT (50 100, 50 200)'), ST_GeometryFromText('POINT (50 100)'))",
                "POINT (50 200)");

        assertSpatialEquals(assertions,
                "ST_Difference(ST_GeometryFromText('LINESTRING (50 100, 50 200)'), ST_GeometryFromText('LINESTRING (50 50, 50 150)'))",
                "LINESTRING (50 150, 50 200)");

        assertSpatialEquals(assertions,
                "ST_Difference(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'), ST_GeometryFromText('MULTILINESTRING ((2 1, 4 1), (3 3, 7 3))'))",
                "MULTILINESTRING ((1 1, 2 1), (4 1, 5 1), (2 4, 4 4))");

        assertSpatialEquals(assertions,
                "ST_Difference(ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))'), ST_GeometryFromText('POLYGON ((2 2, 2 5, 5 5, 5 2, 2 2))'))",
                "POLYGON ((1 1, 4 1, 4 2, 2 2, 2 4, 1 4, 1 1))");

        assertSpatialEquals(
                assertions,
                """
                ST_Difference(
                    ST_Union(ST_GeometryFromText('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))'), ST_GeometryFromText('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))')),
                    ST_GeometryFromText('POLYGON ((0 1, 3 1, 3 3, 0 3, 0 1))')
                )""",
                "POLYGON ((1 1, 0 1, 0 0, 2 0, 2 1, 1 1))");
    }

    @Test
    public void testSTDistance()
    {
        assertThat(assertions.function("ST_Distance", "ST_Point(50, 100)", "ST_Point(150, 150)"))
                .isEqualTo(111.80339887498948);

        assertThat(assertions.function("ST_Distance", "ST_Point(50, 100)", "ST_GeometryFromText('POINT (150 150)')"))
                .isEqualTo(111.80339887498948);

        assertThat(assertions.function("ST_Distance", "ST_GeometryFromText('POINT (50 100)')", "ST_GeometryFromText('POINT (150 150)')"))
                .isEqualTo(111.80339887498948);

        assertThat(assertions.function("ST_Distance", "ST_GeometryFromText('MULTIPOINT (50 100, 50 200)')", "ST_GeometryFromText('Point (50 100)')"))
                .isEqualTo(0.0);

        assertThat(assertions.function("ST_Distance", "ST_GeometryFromText('LINESTRING (50 100, 50 200)')", "ST_GeometryFromText('LINESTRING (10 10, 20 20)')"))
                .isEqualTo(85.44003745317531);

        assertThat(assertions.function("ST_Distance", "ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))')", "ST_GeometryFromText('LINESTRING (10 20, 20 50)')"))
                .isEqualTo(17.08800749063506);

        assertThat(assertions.function("ST_Distance", "ST_GeometryFromText('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))')", "ST_GeometryFromText('POLYGON ((4 4, 4 5, 5 5, 5 4, 4 4))')"))
                .isEqualTo(1.4142135623730951);

        assertThat(assertions.function("ST_Distance", "ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))')", "ST_GeometryFromText('POLYGON ((10 100, 30 10, 10 100))')"))
                .isEqualTo(27.892651361962706);

        assertThat(assertions.function("ST_Distance", "ST_GeometryFromText('POINT EMPTY')", "ST_Point(150, 150)"))
                .isNull(DOUBLE);

        assertThat(assertions.function("ST_Distance", "ST_Point(50, 100)", "ST_GeometryFromText('POINT EMPTY')"))
                .isNull(DOUBLE);

        assertThat(assertions.function("ST_Distance", "ST_GeometryFromText('POINT EMPTY')", "ST_GeometryFromText('POINT EMPTY')"))
                .isNull(DOUBLE);

        assertThat(assertions.function("ST_Distance", "ST_GeometryFromText('MULTIPOINT EMPTY')", "ST_GeometryFromText('Point (50 100)')"))
                .isNull(DOUBLE);

        assertThat(assertions.function("ST_Distance", "ST_GeometryFromText('LINESTRING (50 100, 50 200)')", "ST_GeometryFromText('LINESTRING EMPTY')"))
                .isNull(DOUBLE);

        assertThat(assertions.function("ST_Distance", "ST_GeometryFromText('MULTILINESTRING EMPTY')", "ST_GeometryFromText('LINESTRING (10 20, 20 50)')"))
                .isNull(DOUBLE);

        assertThat(assertions.function("ST_Distance", "ST_GeometryFromText('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))')", "ST_GeometryFromText('POLYGON EMPTY')"))
                .isNull(DOUBLE);

        assertThat(assertions.function("ST_Distance", "ST_GeometryFromText('MULTIPOLYGON EMPTY')", "ST_GeometryFromText('POLYGON ((10 100, 30 10, 10 100))')"))
                .isNull(DOUBLE);
    }

    @Test
    public void testGeometryNearestPoints()
    {
        assertNearestPoints("POINT (50 100)", "POINT (150 150)", "POINT (50 100)", "POINT (150 150)");
        assertNearestPoints("MULTIPOINT (50 100, 50 200)", "POINT (50 100)", "POINT (50 100)", "POINT (50 100)");
        assertNearestPoints("LINESTRING (50 100, 50 200)", "LINESTRING (10 10, 20 20)", "POINT (50 100)", "POINT (20 20)");
        assertNearestPoints("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", "LINESTRING (10 20, 20 50)", "POINT (4 4)", "POINT (10 20)");
        assertNearestPoints("POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))", "POLYGON ((4 4, 4 5, 5 5, 5 4, 4 4))", "POINT (3 3)", "POINT (4 4)");
        assertNearestPoints("MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))", "POLYGON ((10 100, 30 10, 30 100, 10 100))", "POINT (3 3)", "POINT (30 10)");
        assertNearestPoints("GEOMETRYCOLLECTION (POINT (0 0), LINESTRING (0 20, 20 0))", "POLYGON ((5 5, 5 6, 6 6, 6 5, 5 5))", "POINT (10 10)", "POINT (6 6)");

        assertNoNearestPoints("POINT EMPTY", "POINT (150 150)");
        assertNoNearestPoints("POINT (50 100)", "POINT EMPTY");
        assertNoNearestPoints("POINT EMPTY", "POINT EMPTY");
        assertNoNearestPoints("MULTIPOINT EMPTY", "POINT (50 100)");
        assertNoNearestPoints("LINESTRING (50 100, 50 200)", "LINESTRING EMPTY");
        assertNoNearestPoints("MULTILINESTRING EMPTY", "LINESTRING (10 20, 20 50)");
        assertNoNearestPoints("POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))", "POLYGON EMPTY");
        assertNoNearestPoints("MULTIPOLYGON EMPTY", "POLYGON ((10 100, 30 10, 30 100, 10 100))");
    }

    private void assertNearestPoints(String leftInputWkt, String rightInputWkt, String leftPointWkt, String rightPointWkt)
    {
        assertThat(assertions.function("geometry_nearest_points", "ST_GeometryFromText('%s')".formatted(leftInputWkt), "ST_GeometryFromText('%s')".formatted(rightInputWkt)))
                .hasType(RowType.anonymous(ImmutableList.of(GEOMETRY, GEOMETRY)))
                .isEqualTo(ImmutableList.of(leftPointWkt, rightPointWkt));
    }

    private void assertNoNearestPoints(String leftInputWkt, String rightInputWkt)
    {
        assertThat(assertions.function("geometry_nearest_points", "ST_GeometryFromText('%s')".formatted(leftInputWkt), "ST_GeometryFromText('%s')".formatted(rightInputWkt)))
                .isNull(RowType.anonymous(ImmutableList.of(GEOMETRY, GEOMETRY)));
    }

    @Test
    public void testSTExteriorRing()
    {
        assertThat(assertions.function("ST_AsText", "ST_ExteriorRing(ST_GeometryFromText('POLYGON EMPTY'))"))
                .isNull(VARCHAR);

        assertSpatialEquals(assertions,
                "ST_ExteriorRing(ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 1, 1 1))'))",
                "LINESTRING (1 1, 4 1, 1 4, 1 1)");

        assertSpatialEquals(assertions,
                "ST_ExteriorRing(ST_GeometryFromText('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))'))",
                "LINESTRING (0 0, 5 0, 5 5, 0 5, 0 0)");

        assertTrinoExceptionThrownBy(assertions.function("ST_AsText", "ST_ExteriorRing(ST_GeometryFromText('LINESTRING (1 1, 2 2, 1 3)'))")::evaluate)
                .hasMessage("ST_ExteriorRing only applies to POLYGON. Input type is: LINE_STRING");

        assertTrinoExceptionThrownBy(assertions.function("ST_AsText", "ST_ExteriorRing(ST_GeometryFromText('MULTIPOLYGON (((1 1, 2 2, 1 3, 1 1)), ((4 4, 5 5, 4 6, 4 4)))'))")::evaluate)
                .hasMessage("ST_ExteriorRing only applies to POLYGON. Input type is: MULTI_POLYGON");
    }

    @Test
    public void testSTIntersection()
    {
        assertSpatialEquals(assertions,
                "ST_Intersection(ST_GeometryFromText('POINT (50 100)'), ST_GeometryFromText('POINT (150 150)'))",
                "MULTIPOLYGON EMPTY");

        assertSpatialEquals(assertions,
                "ST_Intersection(ST_GeometryFromText('MULTIPOINT (50 100, 50 200)'), ST_GeometryFromText('Point (50 100)'))",
                "POINT (50 100)");

        assertSpatialEquals(assertions,
                "ST_Intersection(ST_GeometryFromText('LINESTRING (50 100, 50 200)'), ST_GeometryFromText('LINESTRING (20 150, 100 150)'))",
                "POINT (50 150)");

        assertSpatialEquals(assertions,
                "ST_Intersection(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'), ST_GeometryFromText('MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))'))",
                "GEOMETRYCOLLECTION (POINT (5 1), LINESTRING (3 4, 4 4))");

        assertSpatialEquals(assertions,
                "ST_Intersection(ST_GeometryFromText('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))'), ST_GeometryFromText('POLYGON ((4 4, 4 5, 5 5, 5 4, 4 4))'))",
                "MULTIPOLYGON EMPTY");

        assertSpatialEquals(assertions,
                "ST_Intersection(" +
                        "    ST_Union(" +
                        "        ST_GeometryFromText('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))'), " +
                        "        ST_GeometryFromText('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))')" +
                        "    ), " +
                        "    ST_GeometryFromText('POLYGON ((0 1, 3 1, 3 3, 0 3, 0 1))')" +
                        ")",
                "POLYGON ((0 2, 1 2, 1 3, 3 3, 3 1, 2 1, 0 1, 0 2))");

        assertSpatialEquals(assertions,
                "ST_Intersection(ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))'), ST_GeometryFromText('LINESTRING (2 0, 2 3)'))",
                "LINESTRING (2 1, 2 3)");

        assertSpatialEquals(assertions,
                "ST_Intersection(ST_GeometryFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'), ST_GeometryFromText('LINESTRING (0 0, 1 -1, 1 2)'))",
                "GEOMETRYCOLLECTION (POINT (0 0), LINESTRING (1 0, 1 1))");

        // test intersection of envelopes
        assertEnvelopeIntersection("POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))", "POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))", "POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))");
        assertEnvelopeIntersection("POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))", "POLYGON ((-1 4, 1 4, 1 6, -1 6, -1 4))", "POLYGON ((0 4, 1 4, 1 5, 0 5, 0 4))");
        assertEnvelopeIntersection("POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))", "POLYGON ((1 4, 2 4, 2 6, 1 6, 1 4))", "POLYGON ((1 4, 2 4, 2 5, 1 5, 1 4))");
        assertEnvelopeIntersection("POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))", "POLYGON ((4 4, 6 4, 6 6, 4 6, 4 4))", "POLYGON ((4 4, 5 4, 5 5, 4 5, 4 4))");
        assertEnvelopeIntersection("POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))", "POLYGON ((10 10, 11 10, 11 11, 10 11, 10 10))", "POLYGON EMPTY");
        assertEnvelopeIntersection("POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))", "POLYGON ((-1 -1, 0 -1, 0 1, -1 1, -1 -1))", "LINESTRING (0 0, 0 1)");
        assertEnvelopeIntersection("POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))", "POLYGON ((1 -1, 2 -1, 2 0, 1 0, 1 -1))", "LINESTRING (1 0, 2 0)");
        assertEnvelopeIntersection("POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))", "POLYGON ((-1 -1, 0 -1, 0 0, -1 0, -1 -1))", "POINT (0 0)");
    }

    private void assertEnvelopeIntersection(String envelope, String otherEnvelope, String expectedWkt)
    {
        String expression = "ST_Intersection(ST_Envelope(ST_GeometryFromText('%s')), ST_Envelope(ST_GeometryFromText('%s')))"
                .formatted(envelope, otherEnvelope);
        assertSpatialEquals(assertions, expression, expectedWkt);
    }

    @Test
    public void testSTSymmetricDifference()
    {
        assertSpatialEquals(assertions,
                "ST_SymDifference(ST_GeometryFromText('POINT (50 100)'), ST_GeometryFromText('POINT (50 150)'))",
                "MULTIPOINT ((50 100), (50 150))");

        assertSpatialEquals(assertions,
                "ST_SymDifference(ST_GeometryFromText('MULTIPOINT (50 100, 60 200)'), ST_GeometryFromText('MULTIPOINT (60 200, 70 150)'))",
                "MULTIPOINT ((50 100), (70 150))");

        assertSpatialEquals(assertions,
                "ST_SymDifference(ST_GeometryFromText('LINESTRING (50 100, 50 200)'), ST_GeometryFromText('LINESTRING (50 50, 50 150)'))",
                "MULTILINESTRING ((50 50, 50 100), (50 150, 50 200))");

        assertSpatialEquals(assertions,
                "ST_SymDifference(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'), ST_GeometryFromText('MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))'))",
                "MULTILINESTRING ((5 0, 5 1), (1 1, 5 1), (5 1, 5 4), (2 4, 3 4), (4 4, 5 4), (5 4, 6 4))");

        assertSpatialEquals(assertions,
                "ST_SymDifference(ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))'), ST_GeometryFromText('POLYGON ((2 2, 2 5, 5 5, 5 2, 2 2))'))",
                "MULTIPOLYGON (((1 1, 4 1, 4 2, 2 2, 2 4, 1 4, 1 1)), ((4 2, 5 2, 5 5, 2 5, 2 4, 4 4, 4 2)))");

        assertSpatialEquals(assertions,
                "ST_SymDifference(ST_GeometryFromText('MULTIPOLYGON (((0 0 , 0 2, 2 2, 2 0, 0 0)), ((2 2, 2 4, 4 4, 4 2, 2 2)))'), ST_GeometryFromText('POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))'))",
                "MULTIPOLYGON (((2 0, 3 0, 3 2, 2 2, 2 0)), ((0 2, 2 2, 2 3, 0 3, 0 2)), ((3 2, 4 2, 4 4, 2 4, 2 3, 3 3, 3 2)))");
    }

    @Test
    public void testStContains()
    {
        assertThat(assertions.function("ST_Contains", "ST_GeometryFromText(null)", "ST_GeometryFromText('POINT (25 25)')"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("ST_Contains", "ST_GeometryFromText('POINT (20 20)')", "ST_GeometryFromText('POINT (25 25)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Contains", "ST_GeometryFromText('MULTIPOINT (20 20, 25 25)')", "ST_GeometryFromText('POINT (25 25)')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Contains", "ST_GeometryFromText('LINESTRING (20 20, 30 30)')", "ST_GeometryFromText('POINT (25 25)')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Contains", "ST_GeometryFromText('LINESTRING (20 20, 30 30)')", "ST_GeometryFromText('MULTIPOINT (25 25, 31 31)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Contains", "ST_GeometryFromText('LINESTRING (20 20, 30 30)')", "ST_GeometryFromText('LINESTRING (25 25, 27 27)')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Contains", "ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))')", "ST_GeometryFromText('MULTILINESTRING ((3 4, 4 4), (2 1, 6 1))')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Contains", "ST_GeometryFromText('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))')", "ST_GeometryFromText('POLYGON ((1 1, 1 2, 2 2, 2 1, 1 1))')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Contains", "ST_GeometryFromText('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))')", "ST_GeometryFromText('POLYGON ((-1 -1, -1 2, 2 2, 2 -1, -1 -1))')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Contains", "ST_GeometryFromText('MULTIPOLYGON (((0 0 , 0 2, 2 2, 2 0, 0 0)), ((2 2, 2 4, 4 4, 4 2, 2 2)))')", "ST_GeometryFromText('POLYGON ((2 2, 2 3, 3 3, 3 2, 2 2))')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Contains", "ST_GeometryFromText('LINESTRING (20 20, 30 30)')", "ST_GeometryFromText('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Contains", "ST_GeometryFromText('LINESTRING EMPTY')", "ST_GeometryFromText('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Contains", "ST_GeometryFromText('LINESTRING (20 20, 30 30)')", "ST_GeometryFromText('POLYGON EMPTY')"))
                .isEqualTo(false);
    }

    @Test
    public void testSTCrosses()
    {
        assertThat(assertions.function("ST_Crosses", "ST_GeometryFromText('POINT (20 20)')", "ST_GeometryFromText('POINT (25 25)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Crosses", "ST_GeometryFromText('LINESTRING (20 20, 30 30)')", "ST_GeometryFromText('POINT (25 25)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Crosses", "ST_GeometryFromText('LINESTRING (20 20, 30 30)')", "ST_GeometryFromText('MULTIPOINT (25 25, 31 31)')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Crosses", "ST_GeometryFromText('LINESTRING(0 0, 1 1)')", "ST_GeometryFromText('LINESTRING (1 0, 0 1)')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Crosses", "ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))')", "ST_GeometryFromText('POLYGON ((2 2, 2 5, 5 5, 5 2, 2 2))')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Crosses", "ST_GeometryFromText('MULTIPOLYGON (((0 0 , 0 2, 2 2, 2 0, 0 0)), ((2 2, 2 4, 4 4, 4 2, 2 2)))')", "ST_GeometryFromText('POLYGON ((2 2, 2 3, 3 3, 3 2, 2 2))')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Crosses", "ST_GeometryFromText('LINESTRING (-2 -2, 6 6)')", "ST_GeometryFromText('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Crosses", "ST_GeometryFromText('POINT (20 20)')", "ST_GeometryFromText('POINT (20 20)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Crosses", "ST_GeometryFromText('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))')", "ST_GeometryFromText('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Crosses", "ST_GeometryFromText('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))')", "ST_GeometryFromText('LINESTRING (0 0, 0 4, 4 4, 4 0)')"))
                .isEqualTo(false);
    }

    @Test
    public void testSTDisjoint()
    {
        assertThat(assertions.function("ST_Disjoint", "ST_GeometryFromText('POINT (50 100)')", "ST_GeometryFromText('POINT (150 150)')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Disjoint", "ST_GeometryFromText('MULTIPOINT (50 100, 50 200)')", "ST_GeometryFromText('POINT (50 100)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Disjoint", "ST_GeometryFromText('LINESTRING (0 0, 0 1)')", "ST_GeometryFromText('LINESTRING (1 1, 1 0)')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Disjoint", "ST_GeometryFromText('LINESTRING (2 1, 1 2)')", "ST_GeometryFromText('LINESTRING (3 1, 1 3)')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Disjoint", "ST_GeometryFromText('LINESTRING (1 1, 3 3)')", "ST_GeometryFromText('LINESTRING (3 1, 1 3)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Disjoint", "ST_GeometryFromText('LINESTRING (50 100, 50 200)')", "ST_GeometryFromText('LINESTRING (20 150, 100 150)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Disjoint", "ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))')", "ST_GeometryFromText('MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Disjoint", "ST_GeometryFromText('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))')", "ST_GeometryFromText('POLYGON ((4 4, 4 5, 5 5, 5 4, 4 4))')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Disjoint", "ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))')", "ST_GeometryFromText('POLYGON ((0 1, 3 1, 3 3, 0 3, 0 1))')"))
                .isEqualTo(false);
    }

    @Test
    public void testSTEquals()
    {
        assertThat(assertions.function("ST_Equals", "ST_GeometryFromText('POINT (50 100)')", "ST_GeometryFromText('POINT (150 150)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Equals", "ST_GeometryFromText('MULTIPOINT (50 100, 50 200)')", "ST_GeometryFromText('POINT (50 100)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Equals", "ST_GeometryFromText('LINESTRING (0 0, 0 1)')", "ST_GeometryFromText('LINESTRING (1 1, 1 0)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Equals", "ST_GeometryFromText('LINESTRING (0 0, 2 2)')", "ST_GeometryFromText('LINESTRING (0 0, 2 2)')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Equals", "ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))')", "ST_GeometryFromText('MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Equals", "ST_GeometryFromText('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))')", "ST_GeometryFromText('POLYGON ((3 3, 3 1, 1 1, 1 3, 3 3))')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Equals", "ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))')", "ST_GeometryFromText('POLYGON ((0 1, 3 1, 3 3, 0 3, 0 1))')"))
                .isEqualTo(false);
    }

    @Test
    public void testSTIntersects()
    {
        assertThat(assertions.function("ST_Intersects", "ST_GeometryFromText('POINT (50 100)')", "ST_GeometryFromText('POINT (150 150)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Intersects", "ST_GeometryFromText('MULTIPOINT (50 100, 50 200)')", "ST_GeometryFromText('POINT (50 100)')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Intersects", "ST_GeometryFromText('LINESTRING (0 0, 0 1)')", "ST_GeometryFromText('LINESTRING (1 1, 1 0)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Intersects", "ST_GeometryFromText('LINESTRING (50 100, 50 200)')", "ST_GeometryFromText('LINESTRING (20 150, 100 150)')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Intersects", "ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))')", "ST_GeometryFromText('MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Intersects", "ST_GeometryFromText('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))')", "ST_GeometryFromText('POLYGON ((4 4, 4 5, 5 5, 5 4, 4 4))')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Intersects", "ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))')", "ST_GeometryFromText('POLYGON ((0 1, 3 1, 3 3, 0 3, 0 1))')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Intersects", "ST_GeometryFromText('POLYGON ((16.5 54, 16.5 54.1, 16.51 54.1, 16.8 54, 16.5 54))')", "ST_GeometryFromText('LINESTRING (16.6 53, 16.6 56)')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Intersects", "ST_GeometryFromText('POLYGON ((16.5 54, 16.5 54.1, 16.51 54.1, 16.8 54, 16.5 54))')", "ST_GeometryFromText('LINESTRING (16.6667 54.05, 16.8667 54.05)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Intersects", "ST_GeometryFromText('POLYGON ((16.5 54, 16.5 54.1, 16.51 54.1, 16.8 54, 16.5 54))')", "ST_GeometryFromText('LINESTRING (16.6667 54.25, 16.8667 54.25)')"))
                .isEqualTo(false);
    }

    @Test
    public void testSTOverlaps()
    {
        assertThat(assertions.function("ST_Overlaps", "ST_GeometryFromText('POINT (50 100)')", "ST_GeometryFromText('POINT (150 150)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Overlaps", "ST_GeometryFromText('POINT (50 100)')", "ST_GeometryFromText('POINT (50 100)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Overlaps", "ST_GeometryFromText('MULTIPOINT (50 100, 50 200)')", "ST_GeometryFromText('POINT (50 100)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Overlaps", "ST_GeometryFromText('LINESTRING (0 0, 0 1)')", "ST_GeometryFromText('LINESTRING (1 1, 1 0)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Overlaps", "ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))')", "ST_GeometryFromText('MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Overlaps", "ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))')", "ST_GeometryFromText('POLYGON ((3 3, 3 5, 5 5, 5 3, 3 3))')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Overlaps", "ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))')", "ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Overlaps", "ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))')", "ST_GeometryFromText('LINESTRING (1 1, 4 4)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Overlaps", "ST_GeometryFromText('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))')", "ST_GeometryFromText('POLYGON ((4 4, 4 5, 5 5, 5 4, 4 4))')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Overlaps", "ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))')", "ST_GeometryFromText('POLYGON ((0 1, 3 1, 3 3, 0 3, 0 1))')"))
                .isEqualTo(true);
    }

    @Test
    public void testSTRelate()
    {
        assertThat(assertions.function("ST_Relate", "ST_GeometryFromText('LINESTRING (0 0, 3 3)')", "ST_GeometryFromText('LINESTRING (1 1, 4 1)')", "'****T****'"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Relate", "ST_GeometryFromText('POLYGON ((2 0, 2 1, 3 1, 2 0))')", "ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))')", "'****T****'"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Relate", "ST_GeometryFromText('POLYGON ((2 0, 2 1, 3 1, 2 0))')", "ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))')", "'T********'"))
                .isEqualTo(false);
    }

    @Test
    public void testSTTouches()
    {
        assertThat(assertions.function("ST_Touches", "ST_GeometryFromText('POINT (50 100)')", "ST_GeometryFromText('POINT (150 150)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Touches", "ST_GeometryFromText('MULTIPOINT (50 100, 50 200)')", "ST_GeometryFromText('POINT (50 100)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Touches", "ST_GeometryFromText('LINESTRING (50 100, 50 200)')", "ST_GeometryFromText('LINESTRING (20 150, 100 150)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Touches", "ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))')", "ST_GeometryFromText('MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Touches", "ST_GeometryFromText('POINT (1 2)')", "ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Touches", "ST_GeometryFromText('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))')", "ST_GeometryFromText('POLYGON ((4 4, 4 5, 5 5, 5 4, 4 4))')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Touches", "ST_GeometryFromText('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))')", "ST_GeometryFromText('LINESTRING (0 0, 1 1)')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Touches", "ST_GeometryFromText('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))')", "ST_GeometryFromText('POLYGON ((3 3, 3 5, 5 5, 5 3, 3 3))')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Touches", "ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))')", "ST_GeometryFromText('POLYGON ((0 1, 3 1, 3 3, 0 3, 0 1))')"))
                .isEqualTo(false);
    }

    @Test
    public void testSTWithin()
    {
        assertThat(assertions.function("ST_Within", "ST_GeometryFromText('POINT (50 100)')", "ST_GeometryFromText('POINT (150 150)')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Within", "ST_GeometryFromText('POINT (50 100)')", "ST_GeometryFromText('MULTIPOINT (50 100, 50 200)')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Within", "ST_GeometryFromText('LINESTRING (50 100, 50 200)')", "ST_GeometryFromText('LINESTRING (50 50, 50 250)')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Within", "ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))')", "ST_GeometryFromText('MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Within", "ST_GeometryFromText('POINT (3 2)')", "ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Within", "ST_GeometryFromText('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))')", "ST_GeometryFromText('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Within", "ST_GeometryFromText('LINESTRING (1 1, 3 3)')", "ST_GeometryFromText('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))')"))
                .isEqualTo(true);

        assertThat(assertions.function("ST_Within", "ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))')", "ST_GeometryFromText('POLYGON ((0 1, 3 1, 3 3, 0 3, 0 1))')"))
                .isEqualTo(false);

        assertThat(assertions.function("ST_Within", "ST_GeometryFromText('POLYGON ((1 1, 1 5, 5 5, 5 1, 1 1))')", "ST_GeometryFromText('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))')"))
                .isEqualTo(false);
    }

    @Test
    public void testInvalidWKT()
    {
        assertTrinoExceptionThrownBy(assertions.function("ST_LineFromText", "'LINESTRING (0 0, 1)'")::evaluate)
                .hasMessage("Invalid WKT: LINESTRING (0 0, 1)");

        assertTrinoExceptionThrownBy(assertions.function("ST_GeometryFromText", "'POLYGON(0 0)'")::evaluate)
                .hasMessage("Invalid WKT: POLYGON(0 0)");

        assertTrinoExceptionThrownBy(assertions.function("ST_Polygon", "'POLYGON(-1 1, 1 -1)'")::evaluate)
                .hasMessage("Invalid WKT: POLYGON(-1 1, 1 -1)");
    }

    @Test
    public void testGreatCircleDistance()
    {
        assertThat(assertions.function("great_circle_distance", "36.12", "-86.67", "33.94", "-118.40"))
                .isEqualTo(2886.4489734367016);

        assertThat(assertions.function("great_circle_distance", "33.94", "-118.40", "36.12", "-86.67"))
                .isEqualTo(2886.4489734367016);

        assertThat(assertions.function("great_circle_distance", "42.3601", "-71.0589", "42.4430", "-71.2290"))
                .isEqualTo(16.73469743457383);

        assertThat(assertions.function("great_circle_distance", "36.12", "-86.67", "36.12", "-86.67"))
                .isEqualTo(0.0);

        assertTrinoExceptionThrownBy(assertions.function("great_circle_distance", "100", "20", "30", "40")::evaluate)
                .hasMessage("Latitude must be between -90 and 90");

        assertTrinoExceptionThrownBy(assertions.function("great_circle_distance", "10", "20", "300", "40")::evaluate)
                .hasMessage("Latitude must be between -90 and 90");

        assertTrinoExceptionThrownBy(assertions.function("great_circle_distance", "10", "200", "30", "40")::evaluate)
                .hasMessage("Longitude must be between -180 and 180");

        assertTrinoExceptionThrownBy(assertions.function("great_circle_distance", "10", "20", "30", "400")::evaluate)
                .hasMessage("Longitude must be between -180 and 180");

        assertTrinoExceptionThrownBy(assertions.function("great_circle_distance", "nan()", "-86.67", "33.94", "-118.40")::evaluate)
                .hasMessage("Latitude must be between -90 and 90");

        assertTrinoExceptionThrownBy(assertions.function("great_circle_distance", "infinity()", "-86.67", "33.94", "-118.40")::evaluate)
                .hasMessage("Latitude must be between -90 and 90");

        assertTrinoExceptionThrownBy(assertions.function("great_circle_distance", "36.12", "nan()", "33.94", "-118.40")::evaluate)
                .hasMessage("Longitude must be between -180 and 180");

        assertTrinoExceptionThrownBy(assertions.function("great_circle_distance", "36.12", "infinity()", "33.94", "-118.40")::evaluate)
                .hasMessage("Longitude must be between -180 and 180");

        assertTrinoExceptionThrownBy(assertions.function("great_circle_distance", "36.12", "-86.67", "nan()", "-118.40")::evaluate)
                .hasMessage("Latitude must be between -90 and 90");

        assertTrinoExceptionThrownBy(assertions.function("great_circle_distance", "36.12", "-86.67", "infinity()", "-118.40")::evaluate)
                .hasMessage("Latitude must be between -90 and 90");

        assertTrinoExceptionThrownBy(assertions.function("great_circle_distance", "36.12", "-86.67", "33.94", "nan()")::evaluate)
                .hasMessage("Longitude must be between -180 and 180");

        assertTrinoExceptionThrownBy(assertions.function("great_circle_distance", "36.12", "-86.67", "33.94", "infinity()")::evaluate)
                .hasMessage("Longitude must be between -180 and 180");
    }

    @Test
    public void testSTInteriorRings()
    {
        assertInvalidInteriorRings("POINT (2 3)", "POINT");
        assertInvalidInteriorRings("LINESTRING EMPTY", "LINE_STRING");
        assertInvalidInteriorRings("MULTIPOINT (30 20, 60 70)", "MULTI_POINT");
        assertInvalidInteriorRings("MULTILINESTRING ((1 10, 100 1000), (2 2, 1 0, 5 6))", "MULTI_LINE_STRING");
        assertInvalidInteriorRings("MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))", "MULTI_POLYGON");
        assertInvalidInteriorRings("GEOMETRYCOLLECTION (POINT (1 1), POINT (2 3), LINESTRING (5 8, 13 21))", "GEOMETRY_COLLECTION");

        assertThat(assertions.function("ST_InteriorRings", "ST_GeometryFromText('POLYGON EMPTY')"))
                .isNull(new ArrayType(GEOMETRY));

        assertInteriorRings("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
        assertInteriorRings("POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))", "LINESTRING (1 1, 1 2, 2 2, 2 1, 1 1)");
        assertInteriorRings("POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1), (3 3, 3 4, 4 4, 4 3, 3 3))",
                "LINESTRING (1 1, 1 2, 2 2, 2 1, 1 1)", "LINESTRING (3 3, 3 4, 4 4, 4 3, 3 3)");
    }

    private void assertInteriorRings(String wkt, String... expected)
    {
        for (int i = 0; i < expected.length; i++) {
            // Construct the expression for the specific ring (1-based index)
            String actualExpression = "ST_InteriorRingN(ST_GeometryFromText('%s'), %s)".formatted(wkt, i + 1);
            assertSpatialEquals(assertions, actualExpression, expected[i]);
        }
    }

    private void assertInvalidInteriorRings(String wkt, String geometryType)
    {
        assertTrinoExceptionThrownBy(assertions.expression("transform(ST_InteriorRings(geometry), x -> ST_AsText(x))")
                .binding("geometry", "ST_GeometryFromText('%s')".formatted(wkt))
                ::evaluate)
                .hasMessage("ST_InteriorRings only applies to POLYGON. Input type is: %s".formatted(geometryType));
    }

    @Test
    public void testSTNumGeometries()
    {
        assertSTNumGeometries("POINT EMPTY", 0);
        assertSTNumGeometries("LINESTRING EMPTY", 0);
        assertSTNumGeometries("POLYGON EMPTY", 0);
        assertSTNumGeometries("MULTIPOINT EMPTY", 0);
        assertSTNumGeometries("MULTILINESTRING EMPTY", 0);
        assertSTNumGeometries("MULTIPOLYGON EMPTY", 0);
        assertSTNumGeometries("GEOMETRYCOLLECTION EMPTY", 0);
        assertSTNumGeometries("POINT (1 2)", 1);
        assertSTNumGeometries("LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)", 1);
        assertSTNumGeometries("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))", 1);
        assertSTNumGeometries("MULTIPOINT (1 2, 2 4, 3 6, 4 8)", 4);
        assertSTNumGeometries("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", 2);
        assertSTNumGeometries("MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 6 4, 2 4)))", 2);
        assertSTNumGeometries("GEOMETRYCOLLECTION(POINT(2 3), LINESTRING (2 3, 3 4))", 2);
    }

    private void assertSTNumGeometries(String wkt, int expected)
    {
        assertThat(assertions.function("ST_NumGeometries", "ST_GeometryFromText('%s')".formatted(wkt)))
                .isEqualTo(expected);
    }

    @Test
    public void testSTUnion()
    {
        List<String> emptyWkts =
                ImmutableList.of(
                        "POINT EMPTY",
                        "MULTIPOINT EMPTY",
                        "LINESTRING EMPTY",
                        "MULTILINESTRING EMPTY",
                        "POLYGON EMPTY",
                        "MULTIPOLYGON EMPTY",
                        "GEOMETRYCOLLECTION EMPTY");
        List<String> simpleWkts =
                ImmutableList.of(
                        "POINT (1 2)",
                        "MULTIPOINT ((1 2), (3 4))",
                        "LINESTRING (0 0, 2 2, 4 4)",
                        "MULTILINESTRING ((0 0, 2 2, 4 4), (5 5, 7 7, 9 9))",
                        "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
                        "MULTIPOLYGON (((1 1, 3 1, 3 3, 1 3, 1 1)), ((2 4, 6 4, 6 6, 2 6, 2 4)))",
                        "GEOMETRYCOLLECTION (LINESTRING (0 5, 5 5), POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)))");

        // empty geometry
        for (String emptyWkt : emptyWkts) {
            for (String simpleWkt : simpleWkts) {
                assertUnion(emptyWkt, simpleWkt, simpleWkt);
            }
        }

        // self union
        for (String simpleWkt : simpleWkts) {
            assertUnion(simpleWkt, simpleWkt, simpleWkt);
        }

        // touching union
        assertUnion("POINT (1 2)", "MULTIPOINT ((1 2), (3 4))", "MULTIPOINT ((1 2), (3 4))");
        assertUnion("MULTIPOINT ((1 2))", "MULTIPOINT ((1 2), (3 4))", "MULTIPOINT ((1 2), (3 4))");
        assertUnion("LINESTRING (0 1, 1 2)", "LINESTRING (1 2, 3 4)", "LINESTRING (0 1, 1 2, 3 4)");
        assertUnion("MULTILINESTRING ((0 0, 2 2, 4 4), (5 5, 7 7, 9 9))", "MULTILINESTRING ((5 5, 7 7, 9 9), (11 11, 13 13, 15 15))", "MULTILINESTRING ((0 0, 2 2, 4 4), (5 5, 7 7, 9 9), (11 11, 13 13, 15 15))");
        assertUnion("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", "POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))", "POLYGON ((0 0, 1 0, 2 0, 2 1, 1 1, 0 1, 0 0))");
        assertUnion("MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)))", "MULTIPOLYGON (((1 0, 2 0, 2 1, 1 1, 1 0)))", "POLYGON ((0 0, 1 0, 2 0, 2 1, 1 1, 0 1, 0 0))");
        assertUnion("GEOMETRYCOLLECTION (POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0)), POINT (1 2))", "GEOMETRYCOLLECTION (POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0)), MULTIPOINT ((1 2), (3 4)))", "GEOMETRYCOLLECTION (MULTIPOINT ((1 2), (3 4)), POLYGON ((0 0, 1 0, 2 0, 2 1, 1 1, 0 1, 0 0)))");

        // within union
        assertUnion("MULTIPOINT ((20 20), (25 25))", "POINT (25 25)", "MULTIPOINT ((20 20), (25 25))");
        assertUnion("LINESTRING (20 20, 30 30)", "POINT (25 25)", "LINESTRING (20 20, 25 25, 30 30)");
        assertUnion("LINESTRING (20 20, 30 30)", "LINESTRING (25 25, 27 27)", "LINESTRING (20 20, 25 25, 27 27, 30 30)");
        assertUnion("POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))", "POLYGON ((1 1, 1 2, 2 2, 2 1, 1 1))", "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))");
        assertUnion("MULTIPOLYGON (((0 0 , 0 2, 2 2, 2 0, 0 0)), ((2 2, 2 4, 4 4, 4 2, 2 2)))", "POLYGON ((2 2, 2 3, 3 3, 3 2, 2 2))", "MULTIPOLYGON (((2 2, 3 2, 4 2, 4 4, 2 4, 2 3, 2 2)), ((0 0, 2 0, 2 2, 0 2, 0 0)))");
        assertUnion("GEOMETRYCOLLECTION (POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0)), MULTIPOINT ((20 20), (25 25)))", "GEOMETRYCOLLECTION (POLYGON ((1 1, 1 2, 2 2, 2 1, 1 1)), POINT (25 25))", "GEOMETRYCOLLECTION (MULTIPOINT ((20 20), (25 25)), POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0)))");

        // overlap union
        assertUnion("LINESTRING (1 1, 3 1)", "LINESTRING (2 1, 4 1)", "LINESTRING (1 1, 2 1, 3 1, 4 1)");
        assertUnion("MULTILINESTRING ((1 1, 3 1))", "MULTILINESTRING ((2 1, 4 1))", "LINESTRING (1 1, 2 1, 3 1, 4 1)");
        assertUnion("POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))", "POLYGON ((2 2, 4 2, 4 4, 2 4, 2 2))", "POLYGON ((1 1, 3 1, 3 2, 4 2, 4 4, 2 4, 2 3, 1 3, 1 1))");
        assertUnion("MULTIPOLYGON (((1 1, 3 1, 3 3, 1 3, 1 1)))", "MULTIPOLYGON (((2 2, 4 2, 4 4, 2 4, 2 2)))", "POLYGON ((1 1, 3 1, 3 2, 4 2, 4 4, 2 4, 2 3, 1 3, 1 1))");
        assertUnion("GEOMETRYCOLLECTION (POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)), LINESTRING (1 1, 3 1))", "GEOMETRYCOLLECTION (POLYGON ((2 2, 4 2, 4 4, 2 4, 2 2)), LINESTRING (2 1, 4 1))", "GEOMETRYCOLLECTION (LINESTRING (3 1, 4 1), POLYGON ((1 1, 2 1, 3 1, 3 2, 4 2, 4 4, 2 4, 2 3, 1 3, 1 1)))");
    }

    private void assertUnion(String leftWkt, String rightWkt, String expectWkt)
    {
        assertSpatialEquals(assertions,
                "ST_Union(ST_GeometryFromText('%s'), ST_GeometryFromText('%s'))".formatted(leftWkt, rightWkt),
                expectWkt);

        // ST_Union should be symmetric; the result must be spatially equal even if vertex order varies.
        assertSpatialEquals(assertions,
                "ST_Union(ST_GeometryFromText('%s'), ST_GeometryFromText('%s'))".formatted(rightWkt, leftWkt),
                expectWkt);
    }

    @Test
    public void testSTGeometryN()
    {
        assertSTGeometryN("POINT EMPTY", 1, null);
        assertSTGeometryN("LINESTRING EMPTY", 1, null);
        assertSTGeometryN("POLYGON EMPTY", 1, null);
        assertSTGeometryN("MULTIPOINT EMPTY", 1, null);
        assertSTGeometryN("MULTILINESTRING EMPTY", 1, null);
        assertSTGeometryN("MULTIPOLYGON EMPTY", 1, null);
        assertSTGeometryN("POINT EMPTY", 0, null);
        assertSTGeometryN("LINESTRING EMPTY", 0, null);
        assertSTGeometryN("POLYGON EMPTY", 0, null);
        assertSTGeometryN("MULTIPOINT EMPTY", 0, null);
        assertSTGeometryN("MULTILINESTRING EMPTY", 0, null);
        assertSTGeometryN("MULTIPOLYGON EMPTY", 0, null);
        assertSTGeometryN("POINT (1 2)", 1, "POINT (1 2)");
        assertSTGeometryN("POINT (1 2)", -1, null);
        assertSTGeometryN("POINT (1 2)", 2, null);
        assertSTGeometryN("LINESTRING(77.29 29.07, 77.42 29.26, 77.27 29.31, 77.29 29.07)", 1, "LINESTRING (77.29 29.07, 77.42 29.26, 77.27 29.31, 77.29 29.07)");
        assertSTGeometryN("LINESTRING(77.29 29.07, 77.42 29.26, 77.27 29.31, 77.29 29.07)", 2, null);
        assertSTGeometryN("LINESTRING(77.29 29.07, 77.42 29.26, 77.27 29.31, 77.29 29.07)", -1, null);
        assertSTGeometryN("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 1, "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
        assertSTGeometryN("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 2, null);
        assertSTGeometryN("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", -1, null);
        assertSTGeometryN("MULTIPOINT (1 2, 2 4, 3 6, 4 8)", 1, "POINT (1 2)");
        assertSTGeometryN("MULTIPOINT (1 2, 2 4, 3 6, 4 8)", 2, "POINT (2 4)");
        assertSTGeometryN("MULTIPOINT (1 2, 2 4, 3 6, 4 8)", 0, null);
        assertSTGeometryN("MULTIPOINT (1 2, 2 4, 3 6, 4 8)", 5, null);
        assertSTGeometryN("MULTIPOINT (1 2, 2 4, 3 6, 4 8)", -1, null);
        assertSTGeometryN("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", 1, "LINESTRING (1 1, 5 1)");
        assertSTGeometryN("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", 2, "LINESTRING (2 4, 4 4)");
        assertSTGeometryN("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", 0, null);
        assertSTGeometryN("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", 3, null);
        assertSTGeometryN("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", -1, null);
        assertSTGeometryN("MULTIPOLYGON (((1 1, 3 1, 3 3, 1 3, 1 1)), ((2 4, 6 4, 6 6, 2 6, 2 4)))", 1, "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))");
        assertSTGeometryN("MULTIPOLYGON (((1 1, 3 1, 3 3, 1 3, 1 1)), ((2 4, 6 4, 6 6, 2 6, 2 4)))", 2, "POLYGON ((2 4, 6 4, 6 6, 2 6, 2 4))");
        assertSTGeometryN("MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 6 4, 2 4)))", 0, null);
        assertSTGeometryN("MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 6 4, 2 4)))", 3, null);
        assertSTGeometryN("MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 6 4, 2 4)))", -1, null);
        assertSTGeometryN("GEOMETRYCOLLECTION(POINT(2 3), LINESTRING (2 3, 3 4))", 1, "POINT (2 3)");
        assertSTGeometryN("GEOMETRYCOLLECTION(POINT(2 3), LINESTRING (2 3, 3 4))", 2, "LINESTRING (2 3, 3 4)");
        assertSTGeometryN("GEOMETRYCOLLECTION(POINT(2 3), LINESTRING (2 3, 3 4))", 3, null);
        assertSTGeometryN("GEOMETRYCOLLECTION(POINT(2 3), LINESTRING (2 3, 3 4))", 0, null);
        assertSTGeometryN("GEOMETRYCOLLECTION(POINT(2 3), LINESTRING (2 3, 3 4))", -1, null);
    }

    private void assertSTGeometryN(String wkt, int index, String expected)
    {
        if (expected == null) {
            assertThat(assertions.expression("ST_GeometryN(geometry, index)")
                    .binding("geometry", "ST_GeometryFromText('%s')".formatted(wkt))
                    .binding("index", Integer.toString(index)))
                    .isNull(GEOMETRY);
            return;
        }
        assertSpatialEquals(assertions,
                "ST_GeometryN(ST_GeometryFromText('%s'), %d)".formatted(wkt, index),
                expected);
    }

    @Test
    public void testSTLineString()
    {
        // General case, 2+ points
        assertThat(assertions.function("ST_LineString", "array[ST_Point(1,2), ST_Point(3,4)]"))
                .hasType(GEOMETRY)
                .isEqualTo("LINESTRING (1 2, 3 4)");

        assertThat(assertions.function("ST_LineString", "array[ST_Point(1,2), ST_Point(3,4), ST_Point(5, 6)]"))
                .hasType(GEOMETRY)
                .isEqualTo("LINESTRING (1 2, 3 4, 5 6)");

        assertThat(assertions.function("ST_LineString", "array[ST_Point(1,2), ST_Point(3,4), ST_Point(5,6), ST_Point(7,8)]"))
                .hasType(GEOMETRY)
                .isEqualTo("LINESTRING (1 2, 3 4, 5 6, 7 8)");

        // Other ways of creating points
        assertThat(assertions.function("ST_LineString", "array[ST_GeometryFromText('POINT (1 2)'), ST_GeometryFromText('POINT (3 4)')]"))
                .hasType(GEOMETRY)
                .isEqualTo("LINESTRING (1 2, 3 4)");

        // Duplicate consecutive points throws exception
        assertTrinoExceptionThrownBy(assertions.function("ST_LineString", "array[ST_Point(1, 2), ST_Point(1, 2)]")::evaluate)
                .hasMessage("Invalid input to ST_LineString: consecutive duplicate points at index 2");

        assertThat(assertions.function("ST_LineString", "array[ST_Point(1, 2), ST_Point(3, 4), ST_Point(1, 2)]"))
                .hasType(GEOMETRY)
                .isEqualTo("LINESTRING (1 2, 3 4, 1 2)");

        // Single point
        assertThat(assertions.function("ST_LineString", "array[ST_Point(9,10)]"))
                .hasType(GEOMETRY)
                .isEqualTo("LINESTRING EMPTY");

        // Zero points
        assertThat(assertions.function("ST_LineString", "array[]"))
                .hasType(GEOMETRY)
                .isEqualTo("LINESTRING EMPTY");

        // Only points can be passed
        assertTrinoExceptionThrownBy(assertions.function("ST_LineString", "array[ST_Point(7,8), ST_GeometryFromText('LINESTRING (1 2, 3 4)')]")::evaluate)
                .hasMessage("ST_LineString takes only an array of valid points, LineString was passed");

        // Nulls points are invalid
        assertTrinoExceptionThrownBy(assertions.function("ST_LineString", "array[NULL]")::evaluate)
                .hasMessage("Invalid input to ST_LineString: null point at index 1");

        assertTrinoExceptionThrownBy(assertions.function("ST_LineString", "array[ST_Point(1,2), NULL]")::evaluate)
                .hasMessage("Invalid input to ST_LineString: null point at index 2");

        assertTrinoExceptionThrownBy(assertions.function("ST_LineString", "array[ST_Point(1, 2), NULL, ST_Point(3, 4)]")::evaluate)
                .hasMessage("Invalid input to ST_LineString: null point at index 2");

        assertTrinoExceptionThrownBy(assertions.function("ST_LineString", "array[ST_Point(1, 2), NULL, ST_Point(3, 4), NULL]")::evaluate)
                .hasMessage("Invalid input to ST_LineString: null point at index 2");

        // Empty points are invalid
        assertTrinoExceptionThrownBy(assertions.function("ST_LineString", "array[ST_GeometryFromText('POINT EMPTY')]")::evaluate)
                .hasMessage("Invalid input to ST_LineString: empty point at index 1");

        assertTrinoExceptionThrownBy(assertions.function("ST_LineString", "array[ST_Point(1,2), ST_GeometryFromText('POINT EMPTY')]")::evaluate)
                .hasMessage("Invalid input to ST_LineString: empty point at index 2");

        assertTrinoExceptionThrownBy(assertions.function("ST_LineString", "array[ST_Point(1,2), ST_GeometryFromText('POINT EMPTY'), ST_Point(3,4)]")::evaluate)
                .hasMessage("Invalid input to ST_LineString: empty point at index 2");

        assertTrinoExceptionThrownBy(assertions.function("ST_LineString", "array[ST_Point(1,2), ST_GeometryFromText('POINT EMPTY'), ST_Point(3,4), ST_GeometryFromText('POINT EMPTY')]")::evaluate)
                .hasMessage("Invalid input to ST_LineString: empty point at index 2");
    }

    @Test
    public void testMultiPoint()
    {
        // General case, 2+ points
        assertMultiPoint("MULTIPOINT ((1 2), (3 4))", "POINT (1 2)", "POINT (3 4)");
        assertMultiPoint("MULTIPOINT ((1 2), (3 4), (5 6))", "POINT (1 2)", "POINT (3 4)", "POINT (5 6)");
        assertMultiPoint("MULTIPOINT ((1 2), (3 4), (5 6), (7 8))", "POINT (1 2)", "POINT (3 4)", "POINT (5 6)", "POINT (7 8)");

        // Duplicate points work
        assertMultiPoint("MULTIPOINT ((1 2), (1 2))", "POINT (1 2)", "POINT (1 2)");
        assertMultiPoint("MULTIPOINT ((1 2), (3 4), (1 2))", "POINT (1 2)", "POINT (3 4)", "POINT (1 2)");

        // Single point
        assertMultiPoint("MULTIPOINT ((1 2))", "POINT (1 2)");

        // Empty array
        assertThat(assertions.function("ST_MultiPoint", "array[]"))
                .isNull(GEOMETRY);

        // Only points can be passed
        assertInvalidMultiPoint("geometry is not a point: LineString at index 2", "POINT (7 8)", "LINESTRING (1 2, 3 4)");

        // Null point raises exception
        assertTrinoExceptionThrownBy(assertions.function("ST_MultiPoint", "array[null]")::evaluate)
                .hasMessage("Invalid input to ST_MultiPoint: null at index 1");

        assertInvalidMultiPoint("null at index 3", "POINT (1 2)", "POINT (1 2)", null);
        assertInvalidMultiPoint("null at index 2", "POINT (1 2)", null, "POINT (3 4)");
        assertInvalidMultiPoint("null at index 2", "POINT (1 2)", null, "POINT (3 4)", null);

        // Empty point raises exception
        assertInvalidMultiPoint("empty point at index 1", "POINT EMPTY");
        assertInvalidMultiPoint("empty point at index 2", "POINT (1 2)", "POINT EMPTY");
    }

    private void assertMultiPoint(String expectedWkt, String... pointWkts)
    {
        assertThat(assertions.expression("ST_MultiPoint(a)")
                .binding("a", "array[%s]".formatted(Arrays.stream(pointWkts)
                        .map(wkt -> wkt == null ? "null" : "ST_GeometryFromText('%s')".formatted(wkt))
                        .collect(Collectors.joining(",")))))
                .hasType(GEOMETRY)
                .isEqualTo(expectedWkt);
    }

    private void assertInvalidMultiPoint(String errorMessage, String... pointWkts)
    {
        assertTrinoExceptionThrownBy(() -> assertions.expression("ST_MultiPoint(a)")
                .binding("a", "array[%s]".formatted(Arrays.stream(pointWkts)
                        .map(wkt -> wkt == null ? "null" : "ST_GeometryFromText('%s')".formatted(wkt))
                        .collect(Collectors.joining(","))))
                .evaluate())
                .hasMessage("Invalid input to ST_MultiPoint: %s".formatted(errorMessage));
    }

    @Test
    public void testSTPointN()
    {
        assertPointN("LINESTRING(1 2, 3 4, 5 6, 7 8, 1 2)", 1, "POINT (1 2)");
        assertPointN("LINESTRING(1 2, 3 4, 5 6, 7 8, 1 2)", 3, "POINT (5 6)");
        assertPointN("LINESTRING(1 2, 3 4, 5 6, 7 8, 1 2)", 10, null);
        assertPointN("LINESTRING(1 2, 3 4, 5 6, 7 8, 1 2)", 0, null);
        assertPointN("LINESTRING(1 2, 3 4, 5 6, 7 8, 1 2)", -1, null);

        assertInvalidPointN("POINT (1 2)", "POINT");
        assertInvalidPointN("MULTIPOINT (1 1, 2 2)", "MULTI_POINT");
        assertInvalidPointN("MULTILINESTRING ((1 1, 2 2), (3 3, 4 4))", "MULTI_LINE_STRING");
        assertInvalidPointN("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", "POLYGON");
        assertInvalidPointN("MULTIPOLYGON (((1 1, 1 4, 4 4, 4 1, 1 1)), ((1 1, 1 4, 4 4, 4 1, 1 1)))", "MULTI_POLYGON");
        assertInvalidPointN("GEOMETRYCOLLECTION(POINT(4 6),LINESTRING(4 6, 7 10))", "GEOMETRY_COLLECTION");
    }

    private void assertPointN(String wkt, int index, String expected)
    {
        if (expected == null) {
            assertThat(assertions.expression("ST_PointN(geometry, index)")
                    .binding("geometry", "ST_GeometryFromText('%s')".formatted(wkt))
                    .binding("index", Integer.toString(index)))
                    .isNull(GEOMETRY);
            return;
        }
        assertSpatialEquals(assertions,
                "ST_PointN(ST_GeometryFromText('%s'), %d)".formatted(wkt, index),
                expected);
    }

    private void assertInvalidPointN(String wkt, String type)
    {
        assertTrinoExceptionThrownBy(() -> assertions.expression("ST_PointN(geometry, 1)")
                .binding("geometry", "ST_GeometryFromText('%s')".formatted(wkt))
                .evaluate())
                .hasMessage("ST_PointN only applies to LINE_STRING. Input type is: %s".formatted(type));
    }

    @Test
    public void testSTGeometries()
    {
        assertThat(assertions.function("ST_Geometries", "ST_GeometryFromText('POINT EMPTY')"))
                .isNull(new ArrayType(GEOMETRY));

        assertSTGeometries("POINT (1 5)", "POINT (1 5)");
        assertSTGeometries("LINESTRING (77.29 29.07, 77.42 29.26, 77.27 29.31, 77.29 29.07)", "LINESTRING (77.29 29.07, 77.42 29.26, 77.27 29.31, 77.29 29.07)");
        assertSTGeometries("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
        assertSTGeometries("MULTIPOINT (1 2, 4 8, 16 32)", "POINT (1 2)", "POINT (4 8)", "POINT (16 32)");
        assertSTGeometries("MULTILINESTRING ((1 1, 2 2))", "LINESTRING (1 1, 2 2)");
        assertSTGeometries("MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((1 1, 3 1, 3 3, 1 3, 1 1)))",
                "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))");
        assertSTGeometries("GEOMETRYCOLLECTION (POINT (2 3), LINESTRING (2 3, 3 4))", "POINT (2 3)", "LINESTRING (2 3, 3 4)");
    }

    private void assertSTGeometries(String wkt, String... expected)
    {
        assertSpatialArrayEquals(assertions,
                "ST_Geometries(ST_GeometryFromText('%s'))".formatted(wkt),
                expected);
    }

    @Test
    public void testSTInteriorRingN()
    {
        assertInvalidInteriorRingN("POINT EMPTY", 0, "POINT");
        assertInvalidInteriorRingN("LINESTRING (1 2, 2 3, 3 4, 1 2)", 1, "LINE_STRING");
        assertInvalidInteriorRingN("MULTIPOINT (1 1, 2 3, 5 8, 1 1)", -1, "MULTI_POINT");
        assertInvalidInteriorRingN("MULTILINESTRING ((2 4, 4 2), (3 5, 5 3))", 0, "MULTI_LINE_STRING");
        assertInvalidInteriorRingN("MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 6 4, 2 4)))", 2, "MULTI_POLYGON");
        assertInvalidInteriorRingN("GEOMETRYCOLLECTION (POINT (2 2), POINT (10 20))", 1, "GEOMETRY_COLLECTION");

        assertInteriorRingN("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 1, null);
        assertInteriorRingN("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 2, null);
        assertInteriorRingN("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", -1, null);
        assertInteriorRingN("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 0, null);
        assertInteriorRingN("POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))", 1, "LINESTRING (1 1, 1 2, 2 2, 2 1, 1 1)");
        assertInteriorRingN("POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1), (3 3, 3 4, 4 4, 4 3, 3 3))", 2, "LINESTRING (3 3, 3 4, 4 4, 4 3, 3 3)");
    }

    private void assertInteriorRingN(String wkt, int index, String expected)
    {
        String expression = "ST_InteriorRingN(ST_GeometryFromText('%s'), %d)".formatted(wkt, index);
        if (expected == null) {
            assertThat(assertions.expression(expression))
                    .isNull(GEOMETRY);
        }
        else {
            assertSpatialEquals(assertions, expression, expected);
        }
    }

    private void assertInvalidInteriorRingN(String wkt, int index, String geometryType)
    {
        assertTrinoExceptionThrownBy(() -> assertions.expression("ST_InteriorRingN(geometry, index)")
                .binding("geometry", "ST_GeometryFromText('%s')".formatted(wkt))
                .binding("index", Integer.toString(index))
                .evaluate())
                .hasMessage("ST_InteriorRingN only applies to POLYGON. Input type is: %s".formatted(geometryType));
    }

    @Test
    public void testSTGeometryType()
    {
        assertThat(assertions.function("ST_GeometryType", "ST_Point(1, 4)"))
                .hasType(VARCHAR)
                .isEqualTo("ST_Point");

        assertThat(assertions.function("ST_GeometryType", "ST_GeometryFromText('LINESTRING (1 1, 2 2)')"))
                .hasType(VARCHAR)
                .isEqualTo("ST_LineString");

        assertThat(assertions.function("ST_GeometryType", "ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))')"))
                .hasType(VARCHAR)
                .isEqualTo("ST_Polygon");

        assertThat(assertions.function("ST_GeometryType", "ST_GeometryFromText('MULTIPOINT (1 1, 2 2)')"))
                .hasType(VARCHAR)
                .isEqualTo("ST_MultiPoint");

        assertThat(assertions.function("ST_GeometryType", "ST_GeometryFromText('MULTILINESTRING ((1 1, 2 2), (3 3, 4 4))')"))
                .hasType(VARCHAR)
                .isEqualTo("ST_MultiLineString");

        assertThat(assertions.function("ST_GeometryType", "ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 4, 4 4, 4 1, 1 1)), ((1 1, 1 4, 4 4, 4 1, 1 1)))')"))
                .hasType(VARCHAR)
                .isEqualTo("ST_MultiPolygon");

        assertThat(assertions.function("ST_GeometryType", "ST_GeometryFromText('GEOMETRYCOLLECTION(POINT(4 6),LINESTRING(4 6, 7 10))')"))
                .hasType(VARCHAR)
                .isEqualTo("ST_GeomCollection");

        assertThat(assertions.function("ST_GeometryType", "ST_Envelope(ST_GeometryFromText('LINESTRING (1 1, 2 2)'))"))
                .hasType(VARCHAR)
                .isEqualTo("ST_Polygon");
    }

    @Test
    public void testSTGeometryFromBinary()
    {
        assertThat(assertions.function("ST_GeomFromBinary", "null"))
                .isNull(GEOMETRY);

        // empty geometries
        assertGeomFromBinary("POINT EMPTY");
        assertGeomFromBinary("MULTIPOINT EMPTY");
        assertGeomFromBinary("LINESTRING EMPTY");
        assertGeomFromBinary("MULTILINESTRING EMPTY");
        assertGeomFromBinary("POLYGON EMPTY");
        assertGeomFromBinary("MULTIPOLYGON EMPTY");
        assertGeomFromBinary("GEOMETRYCOLLECTION EMPTY");

        // valid nonempty geometries
        assertGeomFromBinary("POINT (1 2)");
        assertGeomFromBinary("MULTIPOINT ((1 2), (3 4))");
        assertGeomFromBinary("LINESTRING (0 0, 1 2, 3 4)");
        assertGeomFromBinary("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))");
        assertGeomFromBinary("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
        assertGeomFromBinary("POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))");
        assertGeomFromBinary("MULTIPOLYGON (((1 1, 3 1, 3 3, 1 3, 1 1)), ((2 4, 6 4, 6 6, 2 6, 2 4)))");
        assertGeomFromBinary("GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 2, 3 4), POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0)))");

        // The EWKB representation of "SRID=4326;POINT (1 1)".
        assertSpatialEquals(assertions,
                "ST_GeomFromBinary(x'0101000020E6100000000000000000F03F000000000000F03F')",
                "POINT (1 1)");

        // array of geometries
        assertSpatialArrayEquals(assertions,
                "transform(ARRAY[ST_AsBinary(ST_Point(1, 2)), ST_AsBinary(ST_Point(3, 4))], wkb -> ST_GeomFromBinary(wkb))",
                "POINT (1 2)", "POINT (3 4)");

        // invalid geometries
        assertGeomFromBinary("MULTIPOINT ((0 0), (0 1), (1 1), (0 1))");
        assertGeomFromBinary("LINESTRING (0 0, 0 1, 0 1, 1 1, 1 0, 0 0)");

        // invalid binary
        assertTrinoExceptionThrownBy(assertions.function("ST_GeomFromBinary", "from_hex('deadbeef')")::evaluate)
                .hasMessage("Invalid WKB");
    }

    private void assertGeomFromBinary(String wkt)
    {
        assertSpatialEquals(assertions,
                "ST_GeomFromBinary(ST_AsBinary(ST_GeometryFromText('%s')))".formatted(wkt),
                wkt);
    }

    @Test
    public void testGeometryFromHadoopShape()
    {
        assertThat(assertions.function("geometry_from_hadoop_shape", "null"))
                .isNull(GEOMETRY);

        // empty geometries
        assertGeometryFromHadoopShape("000000000101000000FFFFFFFFFFFFEFFFFFFFFFFFFFFFEFFF", "POINT EMPTY");
        assertGeometryFromHadoopShape("000000000203000000000000000000F87F000000000000F87F000000000000F87F000000000000F87F0000000000000000", "LINESTRING EMPTY");
        assertGeometryFromHadoopShape("000000000305000000000000000000F87F000000000000F87F000000000000F87F000000000000F87F0000000000000000", "POLYGON EMPTY");
        assertGeometryFromHadoopShape("000000000408000000000000000000F87F000000000000F87F000000000000F87F000000000000F87F00000000", "MULTIPOINT EMPTY");
        assertGeometryFromHadoopShape("000000000503000000000000000000F87F000000000000F87F000000000000F87F000000000000F87F0000000000000000", "MULTILINESTRING EMPTY");
        assertGeometryFromHadoopShape("000000000605000000000000000000F87F000000000000F87F000000000000F87F000000000000F87F0000000000000000", "MULTIPOLYGON EMPTY");

        // valid nonempty geometries
        assertGeometryFromHadoopShape("000000000101000000000000000000F03F0000000000000040", "POINT (1 2)");
        assertGeometryFromHadoopShape("000000000203000000000000000000000000000000000000000000000000000840000000000000104001000000030000000000000000000000000000000000000000000000000000000000F03F000000000000004000000000000008400000000000001040", "LINESTRING (0 0, 1 2, 3 4)");
        assertGeometryFromHadoopShape("00000000030500000000000000000000000000000000000000000000000000F03F000000000000F03F010000000500000000000000000000000000000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000", "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
        assertGeometryFromHadoopShape("000000000408000000000000000000F03F00000000000000400000000000000840000000000000104002000000000000000000F03F000000000000004000000000000008400000000000001040", "MULTIPOINT ((1 2), (3 4))");
        assertGeometryFromHadoopShape("000000000503000000000000000000F03F000000000000F03F0000000000001440000000000000104002000000040000000000000002000000000000000000F03F000000000000F03F0000000000001440000000000000F03F0000000000000040000000000000104000000000000010400000000000001040", "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))");
        assertGeometryFromHadoopShape("000000000605000000000000000000F03F000000000000F03F00000000000018400000000000001840020000000A0000000000000005000000000000000000F03F000000000000F03F000000000000F03F0000000000000840000000000000084000000000000008400000000000000840000000000000F03F000000000000F03F000000000000F03F0000000000000040000000000000104000000000000000400000000000001840000000000000184000000000000018400000000000001840000000000000104000000000000000400000000000001040", "MULTIPOLYGON (((1 1, 3 1, 3 3, 1 3, 1 1)), ((2 4, 6 4, 6 6, 2 6, 2 4)))");

        // given hadoop shape is too short
        assertTrinoExceptionThrownBy(assertions.function("geometry_from_hadoop_shape", "from_hex('1234')")::evaluate)
                .hasMessage("Hadoop shape input is too short");

        // hadoop shape type invalid
        assertTrinoExceptionThrownBy(assertions.function("geometry_from_hadoop_shape", "from_hex('000000000701000000FFFFFFFFFFFFEFFFFFFFFFFFFFFFEFFF')")::evaluate)
                .hasMessage("Invalid Hadoop shape type: 7");

        assertTrinoExceptionThrownBy(assertions.function("geometry_from_hadoop_shape", "from_hex('00000000FF01000000FFFFFFFFFFFFEFFFFFFFFFFFFFFFEFFF')")::evaluate)
                .hasMessage("Invalid Hadoop shape type: -1");

        // esri shape invalid
        assertTrinoExceptionThrownBy(assertions.function("geometry_from_hadoop_shape", "from_hex('000000000101000000FFFFFFFFFFFFEFFFFFFFFFFFFFFFEF')")::evaluate)
                .hasMessage("Invalid Hadoop shape");

        // shape type is invalid for given shape
        assertTrinoExceptionThrownBy(assertions.function("geometry_from_hadoop_shape", "from_hex('000000000501000000000000000000F03F0000000000000040')")::evaluate)
                .hasMessage("Invalid Hadoop shape");
    }

    private void assertGeometryFromHadoopShape(String hadoopHex, String expectedWkt)
    {
        assertSpatialEquals(assertions,
                "geometry_from_hadoop_shape(from_hex('%s'))".formatted(hadoopHex),
                expectedWkt);
    }

    @Test
    public void testSphericalGeographyJsonConversion()
    {
        // empty geometries should return empty
        // empty geometries are represented by an empty JSON array in GeoJSON
        assertGeographyToAndFromJson("POINT EMPTY");
        assertGeographyToAndFromJson("LINESTRING EMPTY");
        assertGeographyToAndFromJson("POLYGON EMPTY");
        assertGeographyToAndFromJson("MULTIPOINT EMPTY");
        assertGeographyToAndFromJson("MULTILINESTRING EMPTY");
        assertGeographyToAndFromJson("MULTIPOLYGON EMPTY");
        assertGeographyToAndFromJson("GEOMETRYCOLLECTION EMPTY");

        // valid nonempty geometries should return as is.
        assertGeographyToAndFromJson("POINT (1 2)");
        assertGeographyToAndFromJson("MULTIPOINT ((1 2), (3 4))");
        assertGeographyToAndFromJson("LINESTRING (0 0, 1 2, 3 4)");
        assertGeographyToAndFromJson("MULTILINESTRING (" +
                "(1 1, 5 1), " +
                "(2 4, 4 4))");
        assertGeographyToAndFromJson("POLYGON (" +
                "(0 0, 1 0, 1 1, 0 1, 0 0))");
        assertGeographyToAndFromJson("POLYGON (" +
                "(0 0, 3 0, 3 3, 0 3, 0 0), " +
                "(1 1, 1 2, 2 2, 2 1, 1 1))");
        assertGeographyToAndFromJson("MULTIPOLYGON (" +
                "((1 1, 3 1, 3 3, 1 3, 1 1)), " +
                "((2 4, 6 4, 6 6, 2 6, 2 4)))");
        assertGeographyToAndFromJson("GEOMETRYCOLLECTION (" +
                "POINT (1 2), " +
                "LINESTRING (0 0, 1 2, 3 4), " +
                "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0)))");

        // invalid geometries should return as is.
        assertGeographyToAndFromJson("MULTIPOINT ((0 0), (0 1), (1 1), (0 1))");
        assertGeographyToAndFromJson("LINESTRING (0 0, 0 1, 0 1, 1 1, 1 0, 0 0)");
        assertGeographyToAndFromJson("LINESTRING (0 0, 1 1, 1 0, 0 1, 0 0)");

        // extra properties are stripped from JSON
        assertValidGeometryJson("{\"type\":\"Point\", \"coordinates\":[0,0], \"mykey\":\"myvalue\"}", "POINT (0 0)");

        // explicit JSON test cases should valid but return empty
        assertValidGeometryJson("{\"type\":\"Point\", \"coordinates\":[]}", "POINT EMPTY");
        assertValidGeometryJson("{\"type\":\"LineString\", \"coordinates\":[]}", "LINESTRING EMPTY");
        assertValidGeometryJson("{\"type\":\"Polygon\", \"coordinates\":[]}", "POLYGON EMPTY");
        assertValidGeometryJson("{\"type\":\"MultiPoint\", \"coordinates\":[]}", "MULTIPOINT EMPTY");
        assertValidGeometryJson("{\"type\":\"MultiPolygon\", \"coordinates\":[]}", "MULTIPOLYGON EMPTY");
        assertValidGeometryJson(
                "{\"type\":\"MultiLineString\", \"coordinates\":[[[0.0,0.0],[1,10]],[[10,10],[20,30]],[[123,123],[456,789]]]}",
                "MULTILINESTRING ((0 0, 1 10), (10 10, 20 30), (123 123, 456 789))");
        assertValidGeometryJson("{\"type\":\"Point\"}", "POINT EMPTY");
        assertValidGeometryJson("{\"type\":\"LineString\",\"coordinates\":null}", "LINESTRING EMPTY");
        assertValidGeometryJson("{\"type\":\"MultiPoint\",\"invalidField\":[[10,10],[20,30]]}", "MULTIPOINT EMPTY");
        assertValidGeometryJson("{\"type\":\"FeatureCollection\",\"features\":[]}", "GEOMETRYCOLLECTION EMPTY");

        // Valid JSON with invalid Geometry definition
        assertInvalidGeometryJson("{ \"data\": {\"type\":\"Point\",\"coordinates\":[0,0]}}",
                "Invalid GeoJSON: Could not parse Geometry from Json string.  No 'type' property found.");
        assertInvalidGeometryJson("{\"type\":\"Feature\",\"geometry\":[],\"property\":\"foo\"}",
                "Invalid GeoJSON: Could not parse Feature from GeoJson string.");
        assertInvalidGeometryJson("{\"coordinates\":[[[0.0,0.0],[1,10]],[[10,10],[20,30]],[[123,123],[456,789]]]}",
                "Invalid GeoJSON: Could not parse Geometry from Json string.  No 'type' property found.");

        // Invalid JSON
        assertInvalidGeometryJson("{\"type\":\"MultiPoint\",\"crashMe\"}",
                "Invalid GeoJSON: Unexpected token RIGHT BRACE(}) at position 30.");
    }

    private void assertGeographyToAndFromJson(String wkt)
    {
        assertSpatialEquals(assertions,
                "to_geometry(from_geojson_geometry(to_geojson_geometry(to_spherical_geography(ST_GeometryFromText('%s')))))".formatted(wkt),
                wkt);
    }

    private void assertValidGeometryJson(String json, String wkt)
    {
        assertSpatialEquals(assertions,
                "to_geometry(from_geojson_geometry('%s'))".formatted(json),
                wkt);
    }

    private void assertInvalidGeometryJson(String json, String message)
    {
        assertTrinoExceptionThrownBy(assertions.function("from_geojson_geometry", "'%s'".formatted(json))::evaluate)
                .hasMessage(message);
    }

    @Test
    public void testGeometryJsonConversion()
    {
        // empty geometries should return empty
        // empty geometries are represented by an empty JSON array in GeoJSON
        assertGeometryToAndFromJson("POINT EMPTY");
        assertGeometryToAndFromJson("LINESTRING EMPTY");
        assertGeometryToAndFromJson("POLYGON EMPTY");
        assertGeometryToAndFromJson("MULTIPOINT EMPTY");
        assertGeometryToAndFromJson("MULTILINESTRING EMPTY");
        assertGeometryToAndFromJson("MULTIPOLYGON EMPTY");
        assertGeometryToAndFromJson("GEOMETRYCOLLECTION EMPTY");

        // valid nonempty geometries should return as is.
        assertGeometryToAndFromJson("POINT (1 2)");
        assertGeometryToAndFromJson("MULTIPOINT ((1 2), (3 4))");
        assertGeometryToAndFromJson("LINESTRING (0 0, 1 2, 3 4)");
        assertGeometryToAndFromJson("MULTILINESTRING (" +
                "(1 1, 5 1), " +
                "(2 4, 4 4))");
        assertGeometryToAndFromJson("POLYGON (" +
                "(0 0, 1 0, 1 1, 0 1, 0 0))");
        assertGeometryToAndFromJson("POLYGON (" +
                "(0 0, 3 0, 3 3, 0 3, 0 0), " +
                "(1 1, 1 2, 2 2, 2 1, 1 1))");
        assertGeometryToAndFromJson("MULTIPOLYGON (" +
                "((1 1, 3 1, 3 3, 1 3, 1 1)), " +
                "((2 4, 6 4, 6 6, 2 6, 2 4)))");
        assertGeometryToAndFromJson("GEOMETRYCOLLECTION (" +
                "POINT (1 2), " +
                "LINESTRING (0 0, 1 2, 3 4), " +
                "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0)))");

        // invalid geometries should return as is.
        assertGeometryToAndFromJson("MULTIPOINT ((0 0), (0 1), (1 1), (0 1))");
        assertGeometryToAndFromJson("LINESTRING (0 0, 0 1, 0 1, 1 1, 1 0, 0 0)");
        assertGeometryToAndFromJson("LINESTRING (0 0, 1 1, 1 0, 0 1, 0 0)");
    }

    private void assertGeometryToAndFromJson(String wkt)
    {
        assertSpatialEquals(assertions,
                "to_geometry(from_geojson_geometry(to_geojson_geometry(ST_GeometryFromText('%s'))))".formatted(wkt),
                wkt);
    }

    @Test
    public void testSTGeomFromKML()
    {
        assertSpatialEquals(assertions,
                "ST_GeomFromKML('<Point><coordinates>-2,2</coordinates></Point>')",
                "POINT (-2 2)");

        assertTrinoExceptionThrownBy(assertions.function("ST_GeomFromKML", "'<Point>'")::evaluate)
                .hasMessage("Invalid KML: <Point>");
    }
}
