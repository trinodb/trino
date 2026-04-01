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
package io.trino.geospatial;

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import static io.trino.geospatial.GeometryUtils.contains;
import static io.trino.geospatial.GeometryUtils.estimateMemorySize;
import static io.trino.geospatial.GeometryUtils.jsonFromJtsGeometry;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGeometryUtils
{
    @Test
    void testJsonFromJtsGeometry()
            throws ParseException
    {
        String json = jsonFromJtsGeometry(new WKTReader().read("POINT (1 1)"));
        assertThat(json)
                .isNotNull()
                .doesNotContain("crs");
    }

    @Test
    void testEstimateMemorySize()
            throws ParseException
    {
        Geometry point = new WKTReader().read("POINT (1 1)");
        Geometry lineString = new WKTReader().read("LINESTRING (1 1, 2 2)");
        Geometry geometryCollection = new WKTReader().read("GEOMETRYCOLLECTION (POINT (1 1), LINESTRING (1 1, 2 2))");

        assertThat(estimateMemorySize(null)).isZero();
        assertThat(estimateMemorySize(point)).isPositive();
        assertThat(estimateMemorySize(geometryCollection))
                .isGreaterThan(estimateMemorySize(point) + estimateMemorySize(lineString));
    }

    @Test
    void testContainsUsesJtsSemanticsForMultiLineString()
            throws ParseException
    {
        Geometry multiLineString = new WKTReader().read("MULTILINESTRING ((0 0, 1 0), (1 0, 2 0))");
        Geometry multiPoint = new WKTReader().read("MULTIPOINT ((0.25 0), (1.75 0))");

        assertThat(multiLineString.contains(multiPoint)).isTrue();
        assertThat(multiLineString.getGeometryN(0).contains(multiPoint)).isFalse();
        assertThat(multiLineString.getGeometryN(1).contains(multiPoint)).isFalse();
        assertThat(contains(multiLineString, multiPoint)).isTrue();
    }

    @Test
    void testContainsRecursesForGeometryCollection()
            throws ParseException
    {
        Geometry geometryCollection = new WKTReader().read("GEOMETRYCOLLECTION (POINT (10 10), POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0)))");
        Geometry polygon = new WKTReader().read("POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))");

        assertThat(geometryCollection.getGeometryN(1).contains(polygon)).isTrue();
        assertThat(contains(geometryCollection, polygon)).isTrue();
    }
}
