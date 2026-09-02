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

import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.util.List;

import static io.trino.geospatial.GeometryUtils.contains;
import static io.trino.geospatial.GeometryUtils.estimateMemorySize;
import static io.trino.geospatial.GeometryUtils.invalidInputGeometryException;
import static io.trino.geospatial.GeometryUtils.jsonFromJtsGeometry;
import static io.trino.geospatial.GeometryUtils.parseLegacyLenientOverlay;
import static io.trino.geospatial.GeometryUtils.safeUnion;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void testSafeUnionFailsOnInvalidGeometryWithoutRepair()
            throws ParseException
    {
        WKTReader reader = new WKTReader();
        Geometry invalid = reader.read("POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))");
        Geometry valid = reader.read("POLYGON ((-1 -1, 3 -1, 3 3, -1 3, -1 -1))");

        assertThatThrownBy(() -> safeUnion(invalid, valid, false))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()))
                .hasMessageContaining("Invalid input geometry")
                .hasMessageContaining("Self-intersection at or near (1.0 1.0)");

        // An invalid singleton the union accepts comes back as is
        Geometry empty = reader.read("GEOMETRYCOLLECTION EMPTY");
        assertThat(safeUnion(invalid, empty, false).equalsExact(invalid)).isTrue();

        // The flag has no effect on operations that succeed
        assertThat(safeUnion(valid, valid, false).equalsTopo(valid)).isTrue();
    }

    @Test
    void testLegacyLenientOverlayPropertyParsing()
    {
        assertThat(parseLegacyLenientOverlay(null)).isFalse();
        assertThat(parseLegacyLenientOverlay("false")).isFalse();
        assertThat(parseLegacyLenientOverlay("true")).isTrue();

        assertThatThrownBy(() -> parseLegacyLenientOverlay("yes"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be 'true' or 'false'");
        assertThatThrownBy(() -> parseLegacyLenientOverlay("TRUE"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be 'true' or 'false'");
    }

    @Test
    void testRepairFailureBecomesUserErrorAndPreservesDiagnostics()
            throws ParseException
    {
        Geometry invalid = new WKTReader().read("POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))");
        TopologyException originalFailure = new TopologyException("original overlay failure");
        RuntimeException retryFailure = new IllegalStateException("repair retry failure");

        TrinoException failure = invalidInputGeometryException(List.of(invalid), originalFailure, retryFailure);

        assertThat(failure.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode());
        assertThat(failure).hasMessageContaining("Self-intersection at or near (1.0 1.0)");
        assertThat(failure.getCause()).isSameAs(originalFailure);
        assertThat(originalFailure.getSuppressed()).containsExactly(retryFailure);
    }

    @Test
    void testRepairFailureThroughSafeUnionBecomesUserError()
            throws ParseException
    {
        WKTReader reader = new WKTReader();
        Geometry unrepairable = new UnrepairablePolygon((Polygon) reader.read("POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))"));
        Geometry valid = reader.read("POLYGON ((-1 -1, 3 -1, 3 3, -1 3, -1 -1))");

        // A repair failure keeps the original failure as the cause and attaches the retry failure
        assertThatThrownBy(() -> safeUnion(unrepairable, valid, true))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()))
                .cause()
                .isInstanceOf(TopologyException.class)
                .satisfies(failure -> assertThat(failure.getSuppressed())
                        .satisfiesExactly(suppressed -> assertThat(suppressed)
                                .isInstanceOf(IllegalStateException.class)
                                .hasMessage("synthetic repair failure")));
    }

    @Test
    void testSafeUnionRepairsInvalidGeometryWithoutDroppingValidComponents()
            throws ParseException
    {
        WKTReader reader = new WKTReader();
        // The invalid input is disjoint from the valid one, so its repaired coordinates are asserted
        Geometry invalid = reader.read("POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))");
        Geometry valid = reader.read("GEOMETRYCOLLECTION (POINT (20 20), LINESTRING (20 10, 21 11), POLYGON ((10 10, 12 10, 12 12, 10 12, 10 10)))");
        Geometry expected = reader.read("GEOMETRYCOLLECTION (POINT (20 20), LINESTRING (20 10, 21 11), POLYGON ((0 0, 1 1, 2 0, 0 0)), POLYGON ((0 2, 2 2, 1 1, 0 2)), POLYGON ((10 10, 10 12, 12 12, 12 10, 10 10)))");

        assertThat(invalid.isValid()).isFalse();

        Geometry result = safeUnion(invalid, valid, true);

        assertThat(result.isValid()).isTrue();
        // 2 for the repaired bow-tie plus 4 for the disjoint square
        assertThat(result.getArea()).isEqualTo(6.0);
        assertThat(result.norm()).isEqualTo(expected.norm());
    }

    /**
     * Bow-tie polygon whose repair fails: the first union runs on the real coordinates, and the
     * isValid() call that follows arms the double so only GeometryFixer throws.
     */
    private static final class UnrepairablePolygon
            extends Polygon
    {
        private boolean armed;

        UnrepairablePolygon(Polygon polygon)
        {
            super(polygon.getExteriorRing(), new LinearRing[0], polygon.getFactory());
        }

        @Override
        public boolean isValid()
        {
            armed = true;
            return false;
        }

        @Override
        public GeometryFactory getFactory()
        {
            if (armed) {
                throw new IllegalStateException("synthetic repair failure");
            }
            return super.getFactory();
        }
    }
}
