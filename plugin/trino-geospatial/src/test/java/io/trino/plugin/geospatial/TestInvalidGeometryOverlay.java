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
import io.trino.plugin.geospatial.aggregation.GeometryState;
import io.trino.plugin.geospatial.aggregation.GeometryStateFactory;
import io.trino.plugin.geospatial.aggregation.GeometryUnionAgg;
import io.trino.spi.TrinoException;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.operation.overlayng.OverlayNG;
import org.locationtech.jts.operation.overlayng.OverlayNGRobust;

import java.util.List;

import static io.trino.geospatial.GeometryUtils.legacyLenientOverlay;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestInvalidGeometryOverlay
{
    private static final String INVALID_BOW_TIE = "POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))";
    private static final String LEFT_HALF = "POLYGON ((0 0, 1 0, 1 2, 0 2, 0 0))";
    private static final String OUTER = "POLYGON ((-1 -1, 3 -1, 3 3, -1 3, -1 -1))";

    @Test
    void testInvalidInputsAreRejectedByDefault()
    {
        assertThat(legacyLenientOverlay()).isFalse();

        // Operations that fail on the self-intersecting input report a user error
        Geometry invalid = geometry(INVALID_BOW_TIE);
        Geometry clipping = geometry(LEFT_HALF);
        assertThatThrownBy(() -> GeoFunctions.stIntersection(invalid, clipping))
                .isInstanceOfSatisfying(TrinoException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()))
                .hasMessageContaining("Self-intersection at or near (1.0 1.0)");
        assertThatThrownBy(() -> GeoFunctions.stUnion(invalid, clipping))
                .isInstanceOfSatisfying(TrinoException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()));

        // The union aggregation rejects in every position, including combine()
        GeometryState unionState = new GeometryStateFactory.SingleGeometryState();
        GeometryUnionAgg.input(unionState, geometry(OUTER));
        assertThatThrownBy(() -> GeometryUnionAgg.input(unionState, invalid))
                .isInstanceOfSatisfying(TrinoException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()));
        assertThatThrownBy(() -> GeometryUnionAgg.combine(geometryState(geometry(OUTER)), geometryState(invalid)))
                .isInstanceOfSatisfying(TrinoException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()));

        assertSqlPathRejectsInvalidGeometry();
    }

    /**
     * Covers the SQL entry points, which need a test owning the JVM because the property is read once.
     */
    private static void assertSqlPathRejectsInvalidGeometry()
    {
        String invalidGeometry = "ST_GeometryFromText('POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))')";
        String clippingGeometry = "ST_GeometryFromText('POLYGON ((0 0, 1 0, 1 2, 0 2, 0 0))')";

        try (QueryAssertions assertions = new QueryAssertions()) {
            assertions.addPlugin(new GeoPlugin());

            assertThat(assertions.query("SELECT ST_Intersection(%s, %s)".formatted(invalidGeometry, clippingGeometry)))
                    .failure()
                    .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                    .hasMessageContaining("Self-intersection at or near (1.0 1.0)");

            assertThat(assertions.query("SELECT geometry_union_agg(geometry) FROM (VALUES %s, %s) t(geometry)".formatted(clippingGeometry, invalidGeometry)))
                    .failure()
                    .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                    .hasMessageContaining("Self-intersection at or near (1.0 1.0)");

            // Valid inputs keep working, so the default rejects rather than breaks the functions
            assertThat(assertions.query("SELECT ST_AsText(ST_Intersection(%s, %s))".formatted(clippingGeometry, clippingGeometry)))
                    .matches("VALUES VARCHAR 'POLYGON ((0 0, 0 2, 1 2, 1 0, 0 0))'");
        }
    }

    @Test
    void testExplicitRepairFlagDisabled()
    {
        Geometry invalid = geometry(INVALID_BOW_TIE);
        Geometry clipping = geometry(LEFT_HALF);

        assertThatThrownBy(() -> GeoFunctions.overlayRobustly(invalid, clipping, OverlayNG.INTERSECTION, false))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()))
                .hasMessageContaining("Invalid input geometry")
                .hasMessageContaining("Self-intersection at or near (1.0 1.0)");

        // A degenerate invalid polygon that the overlay accepts comes back unchanged
        Geometry degenerate = geometry("POLYGON ((0 0, 1 0, 2 0, 0 0))");
        assertThat(GeoFunctions.overlayRobustly(degenerate, degenerate, OverlayNG.INTERSECTION, false)
                .equalsTopo(OverlayNGRobust.overlay(degenerate, degenerate, OverlayNG.INTERSECTION))).isTrue();

        assertThatThrownBy(() -> GeoFunctions.stUnionGeometries(ImmutableList.of(invalid, clipping), false))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()))
                .hasMessageContaining("Invalid input geometry")
                .hasMessageContaining("Self-intersection at or near (1.0 1.0)");

        // A singleton invalid input that the union accepts comes back without repair
        assertThat(GeoFunctions.stUnionGeometries(ImmutableList.of(invalid), false).isValid()).isFalse();

        // The flag does not affect operations on valid inputs
        Geometry outer = geometry(OUTER);
        assertThat(GeoFunctions.overlayRobustly(clipping, outer, OverlayNG.INTERSECTION, false).equalsTopo(clipping)).isTrue();
        assertThat(GeoFunctions.stUnionGeometries(ImmutableList.of(clipping, outer), false).equalsTopo(outer)).isTrue();
    }

    @Test
    void testInvalidInputMessageTruncatesAfterThreeInputs()
    {
        // UnaryUnionOp fails on these four disjoint bow-ties, so the truncation runs on a live path
        List<Geometry> invalidInputs = List.of(
                geometry("POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))"),
                geometry("POLYGON ((10 0, 12 2, 10 2, 12 0, 10 0))"),
                geometry("POLYGON ((20 0, 22 2, 20 2, 22 0, 20 0))"),
                geometry("POLYGON ((30 0, 32 2, 30 2, 32 0, 30 0))"));

        assertThatThrownBy(() -> GeoFunctions.stUnionGeometries(invalidInputs, false))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()))
                .hasMessageContaining("Self-intersection at or near (1.0 1.0)")
                .hasMessageContaining("Self-intersection at or near (11.0 1.0)")
                .hasMessageContaining("Self-intersection at or near (21.0 1.0)")
                // The fourth input is summarised rather than described, keeping the message bounded
                .hasMessageNotContaining("31.0")
                .hasMessageEndingWith("(and 1 more invalid inputs)");
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

    private static GeometryState geometryState(Geometry geometry)
    {
        GeometryState state = new GeometryStateFactory.SingleGeometryState();
        state.setGeometry(geometry);
        return state;
    }
}
