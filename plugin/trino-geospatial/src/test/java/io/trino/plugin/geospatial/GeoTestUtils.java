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

import io.trino.sql.query.QueryAssertions;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.geospatial.GeoFunctions.stEquals;
import static io.trino.plugin.geospatial.GeoFunctions.stGeometryFromText;
import static io.trino.plugin.geospatial.GeoFunctions.stIsEmpty;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared test utility methods for geospatial tests.
 * These methods use ST_Equals for geometric comparison, which is insensitive
 * to vertex ordering and starting point differences between geometry libraries.
 */
public final class GeoTestUtils
{
    private GeoTestUtils() {}

    /**
     * Check if two WKT strings represent spatially equal geometries.
     */
    public static boolean spatiallyEquals(String wkt1, String wkt2)
    {
        var geom1 = stGeometryFromText(utf8Slice(wkt1));
        var geom2 = stGeometryFromText(utf8Slice(wkt2));

        if (stIsEmpty(geom1) && stIsEmpty(geom2)) {
            return true;
        }
        return stEquals(geom1, geom2);
    }

    /**
     * Assert that an expression evaluates to a geometry spatially equal to the expected WKT.
     */
    public static void assertSpatialEquals(QueryAssertions assertions, String actualExpression, String expectedWkt)
    {
        // Evaluate Actual to WKT
        String actualWkt = (String) assertions.expression("ST_AsText(%s)".formatted(actualExpression))
                .evaluate()
                .value();

        assertThat(actualWkt)
                .withFailMessage("Actual geometry expression evaluated to NULL")
                .isNotNull();

        var expectedGeometry = stGeometryFromText(utf8Slice(expectedWkt));
        var actualGeometry = stGeometryFromText(utf8Slice(actualWkt));
        if (stIsEmpty(expectedGeometry)) {
            assertThat(stIsEmpty(actualGeometry))
                    .withFailMessage("Expected empty geometry, but got: %s", actualWkt)
                    .isTrue();
            return;
        }

        assertThat(stEquals(expectedGeometry, actualGeometry))
                .withFailMessage("Geometry mismatch!\nExpected: %s\nActual:   %s", expectedWkt, actualWkt)
                .isTrue();
    }

    public static void assertSpatialArrayEquals(QueryAssertions assertions, String actualExpression, String... expectedWkts)
    {
        // Evaluate Actual to WKT List
        @SuppressWarnings("unchecked")
        List<String> actualWkts = (List<String>) assertions.expression(
                "transform(%s, g -> COALESCE(ST_AsText(g), 'NULL'))".formatted(actualExpression))
                .evaluate()
                .value();

        assertThat(actualWkts)
                .describedAs("Mismatch in array size for expression: %s", actualExpression)
                .isNotNull()
                .hasSize(expectedWkts.length);

        for (int i = 0; i < expectedWkts.length; i++) {
            String actual = actualWkts.get(i);
            String expected = expectedWkts[i];

            // Robust spatial check using internal functions (ignoring "1" vs "1.0" or winding order)
            var actGeom = stGeometryFromText(utf8Slice(actual));
            var expGeom = stGeometryFromText(utf8Slice(expected));

            assertThat(stEquals(actGeom, expGeom))
                    .withFailMessage("Geometry mismatch at array index %d.\nExpected: %s\nActual:   %s", i, expected, actual)
                    .isTrue();
        }
    }
}
