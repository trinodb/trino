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

import io.trino.plugin.geospatial.aggregation.GeometryState;
import io.trino.plugin.geospatial.aggregation.GeometryStateFactory;
import io.trino.plugin.geospatial.aggregation.GeometryUnionAgg;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.operation.overlayng.OverlayNG;
import org.locationtech.jts.operation.overlayng.OverlayNGRobust;

import java.util.Map;

import static io.trino.geospatial.GeometryUtils.legacyLenientOverlay;
import static io.trino.plugin.geospatial.GeoTestUtils.assertSpatialEquals;
import static io.trino.spi.type.TinyintType.TINYINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class LegacyLenientOverlayEnabledCase
{
    private static final String INVALID_BOW_TIE = "POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))";
    private static final String LEFT_HALF = "POLYGON ((0 0, 1 0, 1 2, 0 2, 0 0))";
    private static final String DISJOINT_SQUARE = "POLYGON ((10 10, 12 10, 12 12, 10 12, 10 10))";
    private static final String REPAIRED_BOW_TIE_WITH_DISJOINT_SQUARE =
            "MULTIPOLYGON (((0 0, 1 1, 2 0, 0 0)), ((0 2, 2 2, 1 1, 0 2)), ((10 10, 10 12, 12 12, 12 10, 10 10)))";

    @Test
    void testScalarOverlayFallback()
    {
        assertThat(legacyLenientOverlay()).isTrue();

        Geometry invalid = geometry(INVALID_BOW_TIE);
        Geometry clipping = geometry(LEFT_HALF);

        assertThat(invalid.isValid()).isFalse();
        assertThatThrownBy(() -> OverlayNGRobust.overlay(invalid, clipping, OverlayNG.INTERSECTION))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("side location conflict");

        assertGeometry(
                GeoFunctions.stIntersection(invalid, clipping),
                "MULTIPOLYGON (((1 1, 1 0, 0 0, 1 1)), ((0 2, 1 2, 1 1, 0 2)))");
        assertGeometry(
                GeoFunctions.stDifference(invalid, clipping),
                "MULTIPOLYGON (((2 0, 1 0, 1 1, 2 0)), ((2 2, 1 1, 1 2, 2 2)))");
        assertGeometry(
                GeoFunctions.stDifference(clipping, invalid),
                "POLYGON ((0 0, 0 2, 1 1, 0 0))");
        assertGeometry(
                GeoFunctions.stSymmetricDifference(invalid, clipping),
                "MULTIPOLYGON (((0 0, 0 2, 1 1, 0 0)), ((2 0, 1 0, 1 1, 2 0)), ((2 2, 1 1, 1 2, 2 2)))");
        assertGeometry(
                GeoFunctions.stUnion(invalid, clipping),
                "POLYGON ((2 0, 1 0, 0 0, 0 2, 1 2, 2 2, 1 1, 2 0))");

        // GeometryFixer must not mutate the input object.
        assertThat(invalid.isValid()).isFalse();
        assertThat(invalid.getSRID()).isEqualTo(4326);
    }

    @Test
    void testFallbackPreservesSridAndZ()
    {
        Geometry invalid = geometry("POLYGON Z ((0 0 1, 2 2 2, 0 2 3, 2 0 4, 0 0 1))");
        Geometry clipping = geometry("POLYGON Z ((0 0 10, 1 0 11, 1 2 12, 0 2 13, 0 0 10))");

        Geometry result = GeoFunctions.stIntersection(invalid, clipping);

        assertSridAndZ(result);
        assertSridAndZ(GeoFunctions.stUnion(invalid, clipping));

        GeometryState unionState = new GeometryStateFactory.SingleGeometryState();
        GeometryUnionAgg.input(unionState, invalid);
        GeometryUnionAgg.input(unionState, clipping);
        assertSridAndZ(unionState.getGeometry());
    }

    @Test
    void testAggregationFallbackInBothInputOrders()
    {
        // The square is disjoint from the bow-tie, so both results carry the repaired coordinates
        assertUnionAggregation(INVALID_BOW_TIE, DISJOINT_SQUARE);
        assertUnionAggregation(DISJOINT_SQUARE, INVALID_BOW_TIE);
    }

    @Test
    void testSqlPathRepairsInvalidGeometry()
    {
        try (QueryAssertions assertions = new QueryAssertions()) {
            assertions.addPlugin(new GeoPlugin());
            // JTS 1.20 OverlayNGRobust throws "side location conflict" for this self-intersecting polygon.
            String invalidGeometry = "ST_SetSRID(ST_GeometryFromText('POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))'), 4326)";
            String clippingGeometry = "ST_SetSRID(ST_GeometryFromText('POLYGON ((0 0, 1 0, 1 2, 0 2, 0 0))'), 4326)";

            assertThat(assertions.function("ST_IsValid", invalidGeometry))
                    .isEqualTo(false);

            Map<String, String> expectedResults = Map.of(
                    "ST_Intersection(%s, %s)".formatted(invalidGeometry, clippingGeometry), "MULTIPOLYGON (((1 1, 1 0, 0 0, 1 1)), ((0 2, 1 2, 1 1, 0 2)))",
                    "ST_Difference(%s, %s)".formatted(invalidGeometry, clippingGeometry), "MULTIPOLYGON (((2 0, 1 0, 1 1, 2 0)), ((2 2, 1 1, 1 2, 2 2)))",
                    "ST_Difference(%s, %s)".formatted(clippingGeometry, invalidGeometry), "POLYGON ((0 0, 0 2, 1 1, 0 0))",
                    "ST_SymDifference(%s, %s)".formatted(invalidGeometry, clippingGeometry), "MULTIPOLYGON (((0 0, 0 2, 1 1, 0 0)), ((2 0, 1 0, 1 1, 2 0)), ((2 2, 1 1, 1 2, 2 2)))",
                    "ST_Union(%s, %s)".formatted(invalidGeometry, clippingGeometry), "POLYGON ((2 0, 1 0, 0 0, 0 2, 1 2, 2 2, 1 1, 2 0))",
                    "geometry_union(ARRAY[%s, %s])".formatted(invalidGeometry, clippingGeometry), "POLYGON ((2 0, 1 0, 0 0, 0 2, 1 2, 2 2, 1 1, 2 0))");

            expectedResults.forEach((expression, expectedWkt) -> {
                assertSpatialEquals(assertions, expression, expectedWkt);
                assertThat(assertions.function("ST_IsValid", expression))
                        .isEqualTo(true);
                assertThat(assertions.function("ST_SRID", expression))
                        .isEqualTo(4326);
            });

            String invalidGeometryWithZ = "ST_SetSRID(ST_GeometryFromText('POLYGON Z ((0 0 1, 2 2 2, 0 2 3, 2 0 4, 0 0 1))'), 4326)";
            String clippingGeometryWithZ = "ST_SetSRID(ST_GeometryFromText('POLYGON Z ((0 0 10, 1 0 11, 1 2 12, 0 2 13, 0 0 10))'), 4326)";
            String intersectionWithZ = "ST_Intersection(%s, %s)".formatted(invalidGeometryWithZ, clippingGeometryWithZ);

            assertThat(assertions.function("ST_IsValid", intersectionWithZ))
                    .isEqualTo(true);
            assertThat(assertions.function("ST_SRID", intersectionWithZ))
                    .isEqualTo(4326);
            assertThat(assertions.function("ST_CoordDim", intersectionWithZ))
                    .hasType(TINYINT)
                    .isEqualTo((byte) 3);
        }
    }

    private static void assertUnionAggregation(String first, String second)
    {
        GeometryState state = new GeometryStateFactory.SingleGeometryState();
        GeometryUnionAgg.input(state, geometry(first));
        GeometryUnionAgg.input(state, geometry(second));
        assertGeometry(state.getGeometry(), REPAIRED_BOW_TIE_WITH_DISJOINT_SQUARE);
        // 2 for the repaired bow-tie plus 4 for the disjoint square
        assertThat(state.getGeometry().getArea()).isEqualTo(6.0);
    }

    private static Geometry geometry(String wkt)
    {
        try {
            Geometry geometry = new WKTReader().read(wkt);
            geometry.setSRID(4326);
            return geometry;
        }
        catch (ParseException e) {
            throw new AssertionError(e);
        }
    }

    private static void assertGeometry(Geometry actual, String expectedWkt)
    {
        Geometry expected = geometry(expectedWkt);
        assertThat(actual.isValid()).isTrue();
        assertThat(actual.getSRID()).isEqualTo(4326);
        assertThat(actual.equalsTopo(expected))
                .describedAs("expected %s, but got %s", expected, actual)
                .isTrue();
    }

    private static void assertSridAndZ(Geometry geometry)
    {
        assertThat(geometry.isValid()).isTrue();
        assertThat(geometry.getSRID()).isEqualTo(4326);
        assertThat(geometry.getCoordinates())
                .allMatch(coordinate -> Double.isFinite(coordinate.getZ()))
                .anyMatch(coordinate -> coordinate.getZ() == 2.5);
    }
}
