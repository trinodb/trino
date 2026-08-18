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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.geospatial.aggregation.ConvexHullAggregation;
import io.trino.plugin.geospatial.aggregation.GeometryState;
import io.trino.plugin.geospatial.aggregation.GeometryStateFactory;
import io.trino.plugin.geospatial.aggregation.GeometryUnionAgg;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.operation.overlayng.OverlayNG;
import org.locationtech.jts.operation.overlayng.OverlayNGRobust;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestInvalidGeometryOverlayFallback
{
    private static final String INVALID_BOW_TIE = "POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))";
    private static final String LEFT_HALF = "POLYGON ((0 0, 1 0, 1 2, 0 2, 0 0))";
    private static final String OUTER = "POLYGON ((-1 -1, 3 -1, 3 3, -1 3, -1 -1))";
    private static final String DISJOINT_SQUARE = "POLYGON ((10 10, 12 10, 12 12, 10 12, 10 10))";
    private static final String REPAIRED_BOW_TIE_WITH_DISJOINT_SQUARE =
            "MULTIPOLYGON (((0 0, 1 1, 2 0, 0 0)), ((0 2, 2 2, 1 1, 0 2)), ((10 10, 10 12, 12 12, 12 10, 10 10)))";
    private static final String HULL_OVER_BOTH = "POLYGON ((0 0, 2 0, 12 10, 12 12, 10 12, 0 2, 0 0))";

    @Test
    void testScalarOverlayFallback()
    {
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
    void testStrictInvalidGeometryModeRaisesUserError()
    {
        Geometry invalid = geometry(INVALID_BOW_TIE);
        Geometry clipping = geometry(LEFT_HALF);

        assertThatThrownBy(() -> GeoFunctions.overlayRobustly(invalid, clipping, OverlayNG.INTERSECTION, true))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()))
                .hasMessageContaining("Invalid input geometry")
                .hasMessageContaining("Self-intersection at or near (1.0 1.0)");

        // OverlayNGRobust accepts this invalid degenerate polygon and returns linework without
        // throwing. Strict mode must validate before invoking JTS.
        Geometry degenerate = geometry("POLYGON ((0 0, 1 0, 2 0, 0 0))");
        assertThat(OverlayNGRobust.overlay(degenerate, degenerate, OverlayNG.INTERSECTION)).isNotNull();
        assertThatThrownBy(() -> GeoFunctions.overlayRobustly(degenerate, degenerate, OverlayNG.INTERSECTION, true))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()))
                .hasMessageContaining("Invalid input geometry");

        assertThatThrownBy(() -> GeoFunctions.stUnionGeometries(ImmutableList.of(invalid, clipping), true))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()))
                .hasMessageContaining("Invalid input geometry")
                .hasMessageContaining("Self-intersection at or near (1.0 1.0)");

        // UnaryUnionOp can return this singleton invalid polygon without an exception. Strict mode
        // must reject it deterministically before invoking JTS.
        assertThatThrownBy(() -> GeoFunctions.stUnionGeometries(ImmutableList.of(invalid), true))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()))
                .hasMessageContaining("Self-intersection at or near (1.0 1.0)");

        // Strict mode does not affect operations on valid inputs
        Geometry outer = geometry(OUTER);
        assertThat(GeoFunctions.overlayRobustly(clipping, outer, OverlayNG.INTERSECTION, true).equalsTopo(clipping)).isTrue();
        assertThat(GeoFunctions.stUnionGeometries(ImmutableList.of(clipping, outer), true).equalsTopo(outer)).isTrue();
    }

    @Test
    void testAggregationFallbackInBothInputOrders()
    {
        // The valid square is disjoint from the bow-tie, so both the union and the hull need the
        // repaired bow-tie's own coordinates and would not match if it had been dropped instead.
        assertUnionAggregation(INVALID_BOW_TIE, DISJOINT_SQUARE);
        assertUnionAggregation(DISJOINT_SQUARE, INVALID_BOW_TIE);

        GeometryState convexHullState = new GeometryStateFactory.SingleGeometryState();
        ConvexHullAggregation.input(convexHullState, geometry(DISJOINT_SQUARE));
        ConvexHullAggregation.input(convexHullState, geometry(INVALID_BOW_TIE));
        assertGeometry(convexHullState.getGeometry(), HULL_OVER_BOTH);
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
