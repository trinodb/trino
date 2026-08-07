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
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.util.List;

import static io.trino.geospatial.GeometryUtils.contains;
import static io.trino.geospatial.GeometryUtils.estimateMemorySize;
import static io.trino.geospatial.GeometryUtils.invalidInputGeometryException;
import static io.trino.geospatial.GeometryUtils.jsonFromJtsGeometry;
import static io.trino.geospatial.GeometryUtils.parseStrictInvalidOverlay;
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
    void testSafeUnionStrictModeFailsOnInvalidGeometry()
            throws ParseException
    {
        WKTReader reader = new WKTReader();
        Geometry invalid = reader.read("POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))");
        Geometry valid = reader.read("POLYGON ((-1 -1, 3 -1, 3 3, -1 3, -1 -1))");

        assertThatThrownBy(() -> safeUnion(invalid, valid, true))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()))
                .hasMessageContaining("Invalid input geometry")
                .hasMessageContaining("Self-intersection at or near (1.0 1.0)");

        // UnaryUnionOp returns a singleton invalid polygon without throwing. Strict mode must
        // validate before invoking JTS rather than relying on TopologyException as the trigger.
        Geometry empty = reader.read("GEOMETRYCOLLECTION EMPTY");
        assertThatThrownBy(() -> safeUnion(invalid, empty, true))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()))
                .hasMessageContaining("Self-intersection at or near (1.0 1.0)");

        // The strict flag has no effect on operations that succeed
        assertThat(safeUnion(valid, valid, true).equalsTopo(valid)).isTrue();
    }

    @Test
    void testStrictInvalidOverlayPropertyParsing()
    {
        assertThat(parseStrictInvalidOverlay(null)).isFalse();
        assertThat(parseStrictInvalidOverlay("false")).isFalse();
        assertThat(parseStrictInvalidOverlay("true")).isTrue();

        assertThatThrownBy(() -> parseStrictInvalidOverlay("yes"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be 'true' or 'false'");
        assertThatThrownBy(() -> parseStrictInvalidOverlay("TRUE"))
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
    void testSafeUnionRepairsInvalidGeometryWithoutDroppingValidComponents()
            throws ParseException
    {
        WKTReader reader = new WKTReader();
        Geometry invalid = reader.read("POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))");
        Geometry valid = reader.read("GEOMETRYCOLLECTION (POINT (12 12), LINESTRING (10 10, 11 11), POLYGON ((-1 -1, 3 -1, 3 3, -1 3, -1 -1)))");
        Geometry expected = reader.read("GEOMETRYCOLLECTION (POINT (12 12), LINESTRING (10 10, 11 11), POLYGON ((-1 -1, 3 -1, 3 3, -1 3, -1 -1)))");

        assertThat(invalid.isValid()).isFalse();

        Geometry result = safeUnion(invalid, valid);

        assertThat(result.isValid()).isTrue();
        assertThat(result.norm()).isEqualTo(expected.norm());
    }
}
