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
package io.trino.plugin.geospatial.aggregation;

import com.google.common.base.Joiner;
import io.trino.plugin.geospatial.GeoPlugin;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.List;

import static io.trino.plugin.geospatial.GeometryType.GEOMETRY;
import static java.lang.String.format;
import static java.util.Collections.reverse;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestGeometryUnionGeoAggregation
        extends AbstractTestGeoAggregationFunctions
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

    private static final Joiner COMMA_JOINER = Joiner.on(",");

    @Test
    public void testPoint()
    {
        test(
                "identity",
                "POINT (1 2)",
                "POINT (1 2)", "POINT (1 2)", "POINT (1 2)");

        test(
                "no input yields null",
                null);

        test(
                "empty with non-empty",
                "POINT (1 2)",
                "POINT EMPTY", "POINT (1 2)");

        test(
                "disjoint returns multipoint",
                "MULTIPOINT ((1 2), (3 4))",
                "POINT (1 2)", "POINT (3 4)");
    }

    @Test
    public void testLinestring()
    {
        test(
                "identity",
                "LINESTRING (1 1, 2 2)",
                "LINESTRING (1 1, 2 2)", "LINESTRING (1 1, 2 2)", "LINESTRING (1 1, 2 2)");

        test(
                "empty with non-empty",
                "LINESTRING (1 1, 2 2)",
                "LINESTRING EMPTY", "LINESTRING (1 1, 2 2)");

        test(
                "overlap",
                "LINESTRING (1 1, 2 2, 3 3, 4 4)",
                "LINESTRING (1 1, 2 2, 3 3)", "LINESTRING (2 2, 3 3, 4 4)");

        test(
                "disjoint returns multistring",
                "MULTILINESTRING ((1 1, 2 2, 3 3), (1 2, 2 3, 3 4))",
                "LINESTRING (1 1, 2 2, 3 3)", "LINESTRING (1 2, 2 3, 3 4)");

        test(
                "cut through returns multistring",
                "MULTILINESTRING ((1 1, 2 2), (3 1, 2 2), (2 2, 3 3), (2 2, 1 3))",
                "LINESTRING (1 1, 3 3)", "LINESTRING (3 1, 1 3)");
    }

    @Test
    public void testPolygon()
    {
        test(
                "identity",
                "POLYGON ((2 2, 1 1, 3 1, 2 2))",
                "POLYGON ((2 2, 1 1, 3 1, 2 2))", "POLYGON ((2 2, 1 1, 3 1, 2 2))", "POLYGON ((2 2, 1 1, 3 1, 2 2))");

        test(
                "empty with non-empty",
                "POLYGON ((2 2, 1 1, 3 1, 2 2))",
                "POLYGON EMPTY)", "POLYGON ((2 2, 1 1, 3 1, 2 2))");

        test(
                "three overlapping triangles",
                "POLYGON ((1 1, 2 1, 3 1, 4 1, 5 1, 4 2, 3.5 1.5, 3 2, 2.5 1.5, 2 2, 1 1))",
                "POLYGON ((2 2, 3 1, 1 1, 2 2))", "POLYGON ((3 2, 4 1, 2 1, 3 2))", "POLYGON ((4 2, 5 1, 3 1, 4 2))");

        test(
                "two triangles touching at 3 1 returns multipolygon",
                "MULTIPOLYGON (((1 1, 3 1, 2 2, 1 1)), ((3 1, 5 1, 4 2, 3 1)))",
                "POLYGON ((2 2, 3 1, 1 1, 2 2))", "POLYGON ((4 2, 5 1, 3 1, 4 2))");

        test(
                "two disjoint triangles returns multipolygon",
                "MULTIPOLYGON (((1 1, 3 1, 2 2, 1 1)), ((4 1, 6 1, 5 2, 4 1)))",
                "POLYGON ((2 2, 3 1, 1 1, 2 2))", "POLYGON ((5 2, 6 1, 4 1, 5 2))");

        test(
                "polygon with hole that is filled is simplified",
                "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1))",
                "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1), (3 3, 4 3, 4 4, 3 4, 3 3))", "POLYGON ((3 3, 4 3, 4 4, 3 4, 3 3))");

        test(
                "polygon with hole with shape larger than hole is simplified",
                "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1))",
                "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1), (3 3, 4 3, 4 4, 3 4, 3 3))", "POLYGON ((2 2, 5 2, 5 5, 2 5, 2 2))");

        test(
                "polygon with hole with shape smaller than hole becomes multipolygon",
                "MULTIPOLYGON (((1 1, 6 1, 6 6, 1 6, 1 1), (3 3, 3 4, 4 4, 4 3, 3 3)), ((3.25 3.25, 3.75 3.25, 3.75 3.75, 3.25 3.75, 3.25 3.25)))",
                "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1), (3 3, 4 3, 4 4, 3 4, 3 3))", "POLYGON ((3.25 3.25, 3.75 3.25, 3.75 3.75, 3.25 3.75, 3.25 3.25))");

        test(
                "polygon with hole with several smaller pieces which fill hole simplify into polygon",
                "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1))",
                "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1), (3 3, 4 3, 4 4, 3 4, 3 3))", "POLYGON ((3 3, 3 3.5, 3.5 3.5, 3.5 3, 3 3))",
                "POLYGON ((3.5 3.5, 3.5 4, 4 4, 4 3.5, 3.5 3.5))", "POLYGON ((3 3.5, 3 4, 3.5 4, 3.5 3.5, 3 3.5))",
                "POLYGON ((3.5 3, 3.5 3.5, 4 3.5, 4 3, 3.5 3))");

        test(
                "two overlapping rectangles becomes cross",
                "POLYGON ((3 1, 4 1, 4 3, 6 3, 6 4, 4 4, 4 6, 3 6, 3 4, 1 4, 1 3, 3 3, 3 1))",
                "POLYGON ((1 3, 1 4, 6 4, 6 3, 1 3))", "POLYGON ((3 1, 4 1, 4 6, 3 6, 3 1))");

        test(
                "touching squares become single cross",
                "POLYGON ((3 1, 4 1, 4 3, 6 3, 6 4, 4 4, 4 6, 3 6, 3 4, 1 4, 1 3, 3 3, 3 1))",
                "POLYGON ((1 3, 1 4, 3 4, 3 3, 1 3))", "POLYGON ((3 3, 3 4, 4 4, 4 3, 3 3))", "POLYGON ((4 3, 4 4, 6 4, 6 3, 4 3))",
                "POLYGON ((3 1, 4 1, 4 3, 3 3, 3 1))", "POLYGON ((3 4, 3 6, 4 6, 4 4, 3 4))");

        test(
                "square with touching point becomes simplified polygon",
                "POLYGON ((1 1, 3 1, 3 2, 3 3, 1 3, 1 1))",
                "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))", "POINT (3 2)");
    }

    @Test
    public void testMultipoint()
    {
        test(
                "identity",
                "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))",
                "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))", "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))", "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))");

        test(
                "empty with non-empty",
                "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))",
                "MULTIPOINT EMPTY", "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))");

        test(
                "disjoint",
                "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))",
                "MULTIPOINT ((1 2), (2 4))", "MULTIPOINT ((3 6), (4 8))");

        test(
                "overlap",
                "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))",
                "MULTIPOINT ((1 2), (2 4))", "MULTIPOINT ((2 4), (3 6))", "MULTIPOINT ((3 6), (4 8))");
    }

    @Test
    public void testMultilinestring()
    {
        test(
                "identity",
                "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))",
                "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))", "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))", "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))");

        test(
                "empty with non-empty",
                "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))",
                "MULTILINESTRING EMPTY", "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))");

        test(
                "disjoint",
                "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1), (3 5, 6 1), (4 5, 7 1))",
                "MULTILINESTRING ((1 5, 4 1), (3 5, 6 1))", "MULTILINESTRING ((2 5, 5 1), (4 5, 7 1))");

        test(
                "disjoint aggregates with cut through",
                "MULTILINESTRING ((2.5 3, 4 1), (3.5 3, 5 1), (4.5 3, 6 1), (5.5 3, 7 1), (1 3, 2.5 3), (2.5 3, 3.5 3), (1 5, 2.5 3), (3.5 3, 4.5 3), (2 5, 3.5 3), (4.5 3, 5.5 3), (3 5, 4.5 3), (5.5 3, 8 3), (4 5, 5.5 3))",
                "MULTILINESTRING ((1 5, 4 1), (3 5, 6 1))", "MULTILINESTRING ((2 5, 5 1), (4 5, 7 1))", "LINESTRING (1 3, 8 3)");
    }

    @Test
    public void testMultipolygon()
    {
        test(
                "identity",
                "MULTIPOLYGON (((4 2, 3 1, 5 1, 4 2)), ((14 12, 13 11, 15 11, 14 12)))",
                "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)), ((14 12, 15 11, 13 11, 14 12)))",
                "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)), ((14 12, 15 11, 13 11, 14 12)))");

        test(
                "empty with non-empty",
                "MULTIPOLYGON (((4 2, 3 1, 5 1, 4 2)), ((14 12, 13 11, 15 11, 14 12)))",
                "MULTIPOLYGON EMPTY", "MULTIPOLYGON (((4 2, 5 1, 3 1, 4 2)), ((14 12, 15 11, 13 11, 14 12)))");

        test(
                "disjoint",
                "MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), ((3 0, 5 0, 5 2, 3 2, 3 0)), ((0 3, 2 3, 2 5, 0 5, 0 3)), ((3 3, 5 3, 5 5, 3 5, 3 3)))",
                "MULTIPOLYGON ((( 0 0, 0 2, 2 2, 2 0, 0 0 )), (( 0 3, 0 5, 2 5, 2 3, 0 3 )))",
                "MULTIPOLYGON ((( 3 0, 3 2, 5 2, 5 0, 3 0 )), (( 3 3, 3 5, 5 5, 5 3, 3 3 )))");

        test(
                "overlapping multipolygons are simplified",
                "POLYGON ((1 1, 2 1, 3 1, 4 1, 5 1, 4 2, 3.5 1.5, 3 2, 2.5 1.5, 2 2, 1 1))",
                "MULTIPOLYGON (((2 2, 3 1, 1 1, 2 2)), ((3 2, 4 1, 2 1, 3 2)))", "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)))");

        test(
                "overlapping multipolygons become single cross",
                "POLYGON ((3 1, 4 1, 4 3, 6 3, 6 4, 4 4, 4 6, 3 6, 3 4, 1 4, 1 3, 3 3, 3 1))",
                "MULTIPOLYGON (((1 3, 1 4, 3 4, 3 3, 1 3)), ((3 3, 3 4, 4 4, 4 3, 3 3)), ((4 3, 4 4, 6 4, 6 3, 4 3)))",
                "MULTIPOLYGON (((3 1, 4 1, 4 3, 3 3, 3 1)), ((3 4, 3 6, 4 6, 4 4, 3 4)))");
    }

    @Test
    public void testGeometryCollection()
    {
        test(
                "identity",
                "MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), ((3 0, 5 0, 5 2, 3 2, 3 0)))",
                "MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), ((3 0, 5 0, 5 2, 3 2, 3 0)))",
                "GEOMETRYCOLLECTION ( POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((3 0, 5 0, 5 2, 3 2, 3 0)))",
                "GEOMETRYCOLLECTION ( POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((3 0, 5 0, 5 2, 3 2, 3 0)))");

        test(
                "empty collection with empty collection",
                "GEOMETRYCOLLECTION EMPTY",
                "GEOMETRYCOLLECTION EMPTY",
                "GEOMETRYCOLLECTION EMPTY");

        test(
                "empty with non-empty",
                "MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), ((3 0, 5 0, 5 2, 3 2, 3 0)))",
                "GEOMETRYCOLLECTION EMPTY",
                "GEOMETRYCOLLECTION ( POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((3 0, 5 0, 5 2, 3 2, 3 0)))");

        test(
                "overlapping geometry collections are simplified",
                "POLYGON ((1 1, 2 1, 3 1, 4 1, 5 1, 4 2, 3.5 1.5, 3 2, 2.5 1.5, 2 2, 1 1))",
                "GEOMETRYCOLLECTION ( POLYGON ((2 2, 3 1, 1 1, 2 2)), POLYGON ((3 2, 4 1, 2 1, 3 2)) )",
                "GEOMETRYCOLLECTION ( POLYGON ((4 2, 5 1, 3 1, 4 2)) )");

        test(
                "disjoint geometry collection of polygons becomes multipolygon",
                "MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), ((3 0, 5 0, 5 2, 3 2, 3 0)), ((0 3, 2 3, 2 5, 0 5, 0 3)), ((3 3, 5 3, 5 5, 3 5, 3 3)))",
                "GEOMETRYCOLLECTION ( POLYGON (( 0 0, 0 2, 2 2, 2 0, 0 0 )), POLYGON (( 0 3, 0 5, 2 5, 2 3, 0 3 )) )",
                "GEOMETRYCOLLECTION ( POLYGON (( 3 0, 3 2, 5 2, 5 0, 3 0 )), POLYGON (( 3 3, 3 5, 5 5, 5 3, 3 3 )) )");

        test(
                "square with a line crossed becomes geometry collection",
                "GEOMETRYCOLLECTION (MULTILINESTRING ((0 2, 1 2), (3 2, 5 2)), POLYGON ((1 1, 3 1, 3 2, 3 3, 1 3, 1 2, 1 1)))",
                "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))", "LINESTRING (0 2, 5 2)");

        test(
                "square with adjacent line becomes geometry collection",
                "GEOMETRYCOLLECTION (LINESTRING (0 5, 5 5), POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)))",
                "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))", "LINESTRING (0 5, 5 5)");

        test(
                "square with adjacent point becomes geometry collection",
                "GEOMETRYCOLLECTION (POINT (5 2), POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)))",
                "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))", "POINT (5 2)");
    }

    private void test(String testDescription, String expectedWkt, String... wkts)
    {
        assertAggregatedGeometries(testDescription, expectedWkt, wkts);
        assertArrayAggAndGeometryUnion(expectedWkt, wkts);
    }

    @Override
    protected String getFunctionName()
    {
        return "geometry_union_agg";
    }

    private void assertArrayAggAndGeometryUnion(String expectedWkt, String[] wkts)
    {
        List<String> wktList = Arrays.stream(wkts).map(wkt -> format("ST_GeometryFromText('%s')", wkt)).collect(toList());
        String wktArray = format("ARRAY[%s]", COMMA_JOINER.join(wktList));
        // ST_Union(ARRAY[ST_GeometryFromText('...'), ...])
        assertThat(assertions.function("geometry_union", wktArray))
                .hasType(GEOMETRY)
                .isEqualTo(expectedWkt);

        reverse(wktList);
        wktArray = format("ARRAY[%s]", COMMA_JOINER.join(wktList));
        assertThat(assertions.function("geometry_union", wktArray))
                .hasType(GEOMETRY)
                .isEqualTo(expectedWkt);
    }
}
