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

import io.trino.plugin.geospatial.aggregation.ConvexHullAggregation;
import io.trino.plugin.geospatial.aggregation.GeometryState;
import io.trino.plugin.geospatial.aggregation.GeometryStateFactory;
import io.trino.plugin.geospatial.aggregation.GeometryUnionAgg;
import io.trino.spi.TrinoException;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import static io.trino.geospatial.GeometryUtils.strictInvalidOverlay;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class StrictInvalidOverlayEnabledCase
{
    @Test
    void testStrictJVMPropertyControlsPublicScalarAndAggregationPaths()
            throws ParseException
    {
        assertThat(strictInvalidOverlay()).isTrue();

        Geometry invalid = new WKTReader().read("POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))");
        assertThatThrownBy(() -> GeoFunctions.stIntersection(invalid, invalid))
                .isInstanceOfSatisfying(TrinoException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()));
        assertThatThrownBy(() -> GeoFunctions.stUnion(invalid, invalid))
                .isInstanceOfSatisfying(TrinoException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()));

        GeometryState unionState = new GeometryStateFactory.SingleGeometryState();
        assertThatThrownBy(() -> GeometryUnionAgg.input(unionState, invalid))
                .isInstanceOfSatisfying(TrinoException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()));
        assertThat(unionState.getGeometry()).isNull();

        Geometry valid = new WKTReader().read("POLYGON ((10 10, 12 10, 12 12, 10 12, 10 10))");
        Geometry expected = new WKTReader().read("MULTIPOLYGON (((0 0, 1 1, 2 0, 0 0)), ((0 2, 2 2, 1 1, 0 2)), ((10 10, 10 12, 12 12, 12 10, 10 10)))");

        // Synthetic invalid partials exercise every combine position. Strict input validation
        // makes such a state unlikely, but it stays possible because overlay output is not
        // guaranteed valid in every case (locationtech/jts#1000); combine treats partial state
        // as an engine value rather than applying raw-input validation again.
        GeometryState validThenInvalid = geometryState(valid.copy());
        GeometryUnionAgg.combine(validThenInvalid, geometryState(invalid.copy()));
        assertCombinedGeometry(validThenInvalid.getGeometry(), expected);

        GeometryState invalidThenValid = geometryState(invalid.copy());
        GeometryUnionAgg.combine(invalidThenValid, geometryState(valid.copy()));
        assertCombinedGeometry(invalidThenValid.getGeometry(), expected);

        GeometryState emptyThenInvalid = new GeometryStateFactory.SingleGeometryState();
        GeometryUnionAgg.combine(emptyThenInvalid, geometryState(invalid.copy()));
        assertThat(emptyThenInvalid.getGeometry().isValid()).isFalse();
        assertThat(emptyThenInvalid.getGeometry().equalsExact(invalid)).isTrue();
        GeometryUnionAgg.combine(emptyThenInvalid, geometryState(valid.copy()));
        assertCombinedGeometry(emptyThenInvalid.getGeometry(), expected);

        // Convex hull is defined over coordinates and is outside the overlay strictness policy.
        GeometryState convexHullState = new GeometryStateFactory.SingleGeometryState();
        ConvexHullAggregation.input(convexHullState, valid);
        ConvexHullAggregation.input(convexHullState, invalid);
        assertThat(convexHullState.getGeometry().isValid()).isTrue();

        GeometryState invalidConvexHullPartial = new GeometryStateFactory.SingleGeometryState();
        invalidConvexHullPartial.setGeometry(invalid);
        ConvexHullAggregation.combine(convexHullState, invalidConvexHullPartial);
        assertThat(convexHullState.getGeometry().isValid()).isTrue();

        assertSqlPathRejectsInvalidGeometry();
    }

    /**
     * The property is read once per JVM, so the SQL path can only be covered from a test that owns
     * the JVM. Without this, the scalar and aggregation entry points are only reached through direct
     * static calls, leaving the function implementations users actually invoke untested.
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

            assertThat(assertions.query("SELECT geometry_union_agg(geometry) FROM (VALUES %s) t(geometry)".formatted(invalidGeometry)))
                    .failure()
                    .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                    .hasMessageContaining("Self-intersection at or near (1.0 1.0)");

            // Valid inputs keep working, so strict mode rejects rather than breaks the functions
            assertThat(assertions.query("SELECT ST_AsText(ST_Intersection(%s, %s))".formatted(clippingGeometry, clippingGeometry)))
                    .matches("VALUES VARCHAR 'POLYGON ((0 0, 0 2, 1 2, 1 0, 0 0))'");
        }
    }

    private static GeometryState geometryState(Geometry geometry)
    {
        GeometryState state = new GeometryStateFactory.SingleGeometryState();
        state.setGeometry(geometry);
        return state;
    }

    private static void assertCombinedGeometry(Geometry actual, Geometry expected)
    {
        assertThat(actual.isValid()).isTrue();
        assertThat(actual.equalsTopo(expected)).isTrue();
        assertThat(actual.getArea()).isEqualTo(6.0);
    }
}
