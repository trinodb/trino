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

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static com.google.common.io.Resources.getResource;

public class TestGeometryConvexHullGeoAggregation
        extends AbstractTestGeoAggregationFunctions
{
    @Test
    public void testPoint()
    {
        assertAggregatedGeometries(
                "identity",
                "POINT (1 2)",
                "POINT (1 2)",
                "POINT (1 2)",
                "POINT (1 2)");

        assertAggregatedGeometries(
                "no input yields null",
                null);

        assertAggregatedGeometries(
                "null before value yields the value",
                "POINT (1 2)",
                null, "POINT (1 2)");

        assertAggregatedGeometries(
                "null after value yields the value",
                "POINT (1 2)",
                "POINT (1 2)",
                null);

        assertAggregatedGeometries(
                "empty with non-empty",
                "POINT (1 2)",
                "POINT EMPTY",
                "POINT (1 2)");

        assertAggregatedGeometries(
                "2 disjoint points return linestring",
                "LINESTRING (1 2, 3 4)",
                "POINT (1 2)",
                "POINT (3 4)");

        assertAggregatedGeometries(
                "points lying on the same line return linestring",
                "LINESTRING (3 3, 1 1)",
                "POINT (1 1)",
                "POINT (2 2)",
                "POINT (3 3)");

        assertAggregatedGeometries(
                "points forming a polygon return polygon",
                "POLYGON ((5 8, 2 3, 1 1, 5 8))",
                "POINT (1 1)",
                "POINT (2 3)",
                "POINT (5 8)");
    }

    @Test

    public void testLinestring()
    {
        assertAggregatedGeometries(
                "identity",
                "LINESTRING (1 1, 2 2)",
                "LINESTRING (1 1, 2 2)",
                "LINESTRING (1 1, 2 2)",
                "LINESTRING (1 1, 2 2)");

        assertAggregatedGeometries(
                "empty with non-empty",
                "LINESTRING (1 1, 2 2)",
                "LINESTRING EMPTY",
                "LINESTRING (1 1, 2 2)");

        assertAggregatedGeometries(
                "overlap",
                "LINESTRING (1 1, 4 4)",
                "LINESTRING (1 1, 2 2, 3 3)",
                "LINESTRING (2 2, 3 3, 4 4)");

        assertAggregatedGeometries(
                "disjoint returns polygon",
                "POLYGON ((1 1, 3 3, 3 4, 1 2, 1 1))",
                "LINESTRING (1 1, 2 2, 3 3)",
                "LINESTRING (1 2, 2 3, 3 4)");

        assertAggregatedGeometries(
                "cut through returns polygon",
                "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)),",
                "LINESTRING (1 1, 3 3)",
                "LINESTRING (3 1, 1 3)");
    }

    @Test
    public void testPolygon()
    {
        assertAggregatedGeometries(
                "identity",
                "POLYGON ((2 2, 1 1, 3 1, 2 2))",
                "POLYGON ((2 2, 1 1, 3 1, 2 2))",
                "POLYGON ((2 2, 1 1, 3 1, 2 2))",
                "POLYGON ((2 2, 1 1, 3 1, 2 2))");

        assertAggregatedGeometries(
                "empty with non-empty",
                "POLYGON ((2 2, 1 1, 3 1, 2 2))",
                "POLYGON EMPTY",
                "POLYGON ((2 2, 1 1, 3 1, 2 2))");

        assertAggregatedGeometries(
                "three overlapping triangles",
                "POLYGON ((1 1, 5 1, 4 2, 2 2, 1 1))",
                "POLYGON ((2 2, 3 1, 1 1, 2 2))",
                "POLYGON ((3 2, 4 1, 2 1, 3 2))",
                "POLYGON ((4 2, 5 1, 3 1, 4 2))");

        assertAggregatedGeometries(
                "two triangles touching at 3 1 returns polygon",
                "POLYGON ((1 1, 5 1, 4 2, 2 2, 1 1))",
                "POLYGON ((2 2, 3 1, 1 1, 2 2))",
                "POLYGON ((4 2, 5 1, 3 1, 4 2))");

        assertAggregatedGeometries(
                "two disjoint triangles returns polygon",
                "POLYGON ((1 1, 6 1, 5 2, 2 2, 1 1))",
                "POLYGON ((2 2, 3 1, 1 1, 2 2))",
                "POLYGON ((5 2, 6 1, 4 1, 5 2))");

        assertAggregatedGeometries(
                "polygon with hole returns the exterior polygon",
                "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1))",
                "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1), (3 3, 4 3, 4 4, 3 4, 3 3))",
                "POLYGON ((3 3, 4 3, 4 4, 3 4, 3 3))");

        assertAggregatedGeometries(
                "polygon with hole with shape larger than hole is simplified",
                "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1))",
                "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1), (3 3, 4 3, 4 4, 3 4, 3 3))",
                "POLYGON ((2 2, 5 2, 5 5, 2 5, 2 2))");

        assertAggregatedGeometries(
                "polygon with hole with shape smaller than hole returns the exterior polygon",
                "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1))",
                "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1), (3 3, 4 3, 4 4, 3 4, 3 3))",
                "POLYGON ((3.25 3.25, 3.75 3.25, 3.75 3.75, 3.25 3.75, 3.25 3.25))");

        assertAggregatedGeometries(
                "polygon with hole with several smaller pieces which fill hole returns the exterior polygon",
                "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1))",
                "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1), (3 3, 4 3, 4 4, 3 4, 3 3))",
                "POLYGON ((3 3, 3 3.5, 3.5 3.5, 3.5 3, 3 3))",
                "POLYGON ((3.5 3.5, 3.5 4, 4 4, 4 3.5, 3.5 3.5))",
                "POLYGON ((3 3.5, 3 4, 3.5 4, 3.5 3.5, 3 3.5))",
                "POLYGON ((3.5 3, 3.5 3.5, 4 3.5, 4 3, 3.5 3))");

        assertAggregatedGeometries(
                "two overlapping rectangles",
                "POLYGON ((3 1, 4 1, 6 3, 6 4, 4 6, 3 6, 1 4, 1 3, 3 1))",
                "POLYGON ((1 3, 1 4, 6 4, 6 3, 1 3))",
                "POLYGON ((3 1, 4 1, 4 6, 3 6, 3 1))");

        assertAggregatedGeometries(
                "touching squares",
                "POLYGON ((3 1, 4 1, 6 3, 6 4, 4 6, 3 6, 1 4, 1 3, 3 1))",
                "POLYGON ((1 3, 1 4, 3 4, 3 3, 1 3))",
                "POLYGON ((3 3, 3 4, 4 4, 4 3, 3 3))",
                "POLYGON ((4 3, 4 4, 6 4, 6 3, 4 3))",
                "POLYGON ((3 1, 4 1, 4 3, 3 3, 3 1))",
                "POLYGON ((3 4, 3 6, 4 6, 4 4, 3 4))");

        assertAggregatedGeometries(
                "square with touching point becomes simplified polygon",
                "POLYGON ((1 1, 3 1, 3 2, 3 3, 1 3, 1 1))",
                "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))",
                "POINT (3 2)");
    }

    @Test
    public void testMultipoint()
    {
        assertAggregatedGeometries(
                "lying on the same line",
                "LINESTRING (1 2, 4 8)",
                "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))",
                "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))",
                "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))");

        assertAggregatedGeometries(
                "empty with non-empty",
                "LINESTRING (1 2, 4 8)",
                "MULTIPOINT EMPTY",
                "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))");

        assertAggregatedGeometries(
                "disjoint",
                "LINESTRING (1 2, 4 8)",
                "MULTIPOINT ((1 2), (2 4))",
                "MULTIPOINT ((3 6), (4 8))");

        assertAggregatedGeometries(
                "overlap",
                "LINESTRING (1 2, 4 8)",
                "MULTIPOINT ((1 2), (2 4))",
                "MULTIPOINT ((2 4), (3 6))",
                "MULTIPOINT ((3 6), (4 8))");
    }

    @Test
    public void testMultilinestring()
    {
        assertAggregatedGeometries(
                "identity",
                "POLYGON ((4 1, 5 1, 2 5, 1 5, 4 1))",
                "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))",
                "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))",
                "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))");

        assertAggregatedGeometries(
                "empty with non-empty",
                "POLYGON ((4 1, 5 1, 2 5, 1 5, 4 1))",
                "MULTILINESTRING EMPTY",
                "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))");

        assertAggregatedGeometries(
                "disjoint",
                "POLYGON ((4 5, 1 5, 4 1, 7 1, 4 5))",
                "MULTILINESTRING ((1 5, 4 1), (3 5, 6 1))",
                "MULTILINESTRING ((2 5, 5 1), (4 5, 7 1))");

        assertAggregatedGeometries(
                "disjoint aggregates with cut through",
                "POLYGON ((1 3, 4 1, 6 1, 8 3, 3 5, 1 5, 1 3))",
                "MULTILINESTRING ((1 5, 4 1), (3 5, 6 1))",
                "LINESTRING (1 3, 8 3)");
    }

    @Test
    public void testMultipolygon()
    {
        assertAggregatedGeometries(
                "identity",
                "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)))",
                "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)))",
                "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)))",
                "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)))");

        assertAggregatedGeometries(
                "empty with non-empty",
                "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)))",
                "MULTIPOLYGON EMPTY",
                "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)))");

        assertAggregatedGeometries(
                "disjoint",
                "POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))",
                "MULTIPOLYGON ((( 0 0, 0 2, 2 2, 2 0, 0 0 )), (( 0 3, 0 5, 2 5, 2 3, 0 3 )))",
                "MULTIPOLYGON ((( 3 0, 3 2, 5 2, 5 0, 3 0 )), (( 3 3, 3 5, 5 5, 5 3, 3 3 )))");

        assertAggregatedGeometries(
                "overlapping multipolygon",
                "POLYGON ((1 1, 5 1, 4 2, 2 2, 1 1))",
                "MULTIPOLYGON (((2 2, 3 1, 1 1, 2 2)), ((3 2, 4 1, 2 1, 3 2)))",
                "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)))");
    }

    @Test
    public void test1000Points()
            throws Exception
    {
        Path filePath = new File(getResource("1000_points.txt").toURI()).toPath();
        List<String> points = Files.readAllLines(filePath);

        assertAggregatedGeometries(
                "1000points",
                "POLYGON ((0.7642699 0.000490129, 0.92900103 0.005068898, 0.97419316 0.019917727, 0.99918157 0.063635945, 0.9997078 0.10172784, 0.9973114 0.41161585, 0.9909166 0.94222105, 0.9679412 0.9754768, 0.95201814 0.9936909, 0.44082636 0.9999601, 0.18622541 0.998157, 0.07163471 0.98902994, 0.066090584 0.9885783, 0.024429202 0.9685611, 0.0044354796 0.8878008, 0.0025004745 0.81172496, 0.0015820265 0.39900982, 0.001614511 0.00065791607, 0.7642699 0.000490129))",
                points.toArray(new String[0]));
    }

    @Test
    public void testGeometryCollection()
    {
        assertAggregatedGeometries(
                "identity",
                "POLYGON ((0 0, 5 0, 5 2, 0 2, 0 0))",
                "MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), ((3 0, 5 0, 5 2, 3 2, 3 0)))",
                "GEOMETRYCOLLECTION ( POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((3 0, 5 0, 5 2, 3 2, 3 0)))",
                "GEOMETRYCOLLECTION ( POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((3 0, 5 0, 5 2, 3 2, 3 0)))");

        assertAggregatedGeometries(
                "empty with non-empty",
                "POLYGON ((0 0, 5 0, 5 2, 0 2, 0 0))",
                "GEOMETRYCOLLECTION EMPTY",
                "GEOMETRYCOLLECTION ( POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((3 0, 5 0, 5 2, 3 2, 3 0)))");

        assertAggregatedGeometries(
                "overlapping geometry collections",
                "POLYGON ((1 1, 5 1, 4 2, 2 2, 1 1))",
                "GEOMETRYCOLLECTION ( POLYGON ((2 2, 3 1, 1 1, 2 2)), POLYGON ((3 2, 4 1, 2 1, 3 2)) )",
                "GEOMETRYCOLLECTION ( POLYGON ((4 2, 5 1, 3 1, 4 2)) )");

        assertAggregatedGeometries(
                "disjoint geometry collection of polygons",
                "POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))",
                "GEOMETRYCOLLECTION ( POLYGON (( 0 0, 0 2, 2 2, 2 0, 0 0 )), POLYGON (( 0 3, 0 5, 2 5, 2 3, 0 3 )) )",
                "GEOMETRYCOLLECTION ( POLYGON (( 3 0, 3 2, 5 2, 5 0, 3 0 )), POLYGON (( 3 3, 3 5, 5 5, 5 3, 3 3 )) )");

        assertAggregatedGeometries(
                "square with a line crossed",
                "POLYGON ((0 2, 1 1, 3 1, 5 2, 3 3, 1 3, 0 2))",
                "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))", "LINESTRING (0 2, 5 2)");

        assertAggregatedGeometries(
                "square with adjacent line",
                "POLYGON ((0 5, 1 1, 3 1, 5 5, 0 5))",
                "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))", "LINESTRING (0 5, 5 5)");

        assertAggregatedGeometries(
                "square with adjacent point",
                "POLYGON ((5 2, 3 3, 1 3, 1 1, 3 1, 5 2))",
                "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))", "POINT (5 2)");
    }

    @Override
    protected String getFunctionName()
    {
        return "convex_hull_agg";
    }
}
