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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.sql.query.QueryAssertions;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.io.Resources.getResource;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.geospatial.GeometryType.GEOMETRY;
import static io.trino.plugin.geospatial.SphericalGeographyType.SPHERICAL_GEOGRAPHY;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.testng.Assert.assertEquals;

@TestInstance(PER_CLASS)
public class TestSphericalGeoFunctions
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

    @Test
    public void testGetObjectValue()
    {
        List<String> wktList = ImmutableList.of(
                "POINT EMPTY",
                "MULTIPOINT EMPTY",
                "LINESTRING EMPTY",
                "MULTILINESTRING EMPTY",
                "POLYGON EMPTY",
                "MULTIPOLYGON EMPTY",
                "GEOMETRYCOLLECTION EMPTY",
                "POINT (-40.2 28.9)",
                "MULTIPOINT ((-40.2 28.9), (-40.2 31.9))",
                "LINESTRING (-40.2 28.9, -40.2 31.9, -37.2 31.9)",
                "MULTILINESTRING ((-40.2 28.9, -40.2 31.9), (-40.2 31.9, -37.2 31.9))",
                "POLYGON ((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9))",
                "POLYGON ((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9), (-39.2 29.9, -39.2 30.9, -38.2 30.9, -38.2 29.9, -39.2 29.9))",
                "MULTIPOLYGON (((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9)), ((-39.2 29.9, -38.2 29.9, -38.2 30.9, -39.2 30.9, -39.2 29.9)))",
                "GEOMETRYCOLLECTION (POINT (-40.2 28.9), LINESTRING (-40.2 28.9, -40.2 31.9, -37.2 31.9), POLYGON ((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9)))");

        BlockBuilder builder = SPHERICAL_GEOGRAPHY.createBlockBuilder(null, wktList.size());
        for (String wkt : wktList) {
            SPHERICAL_GEOGRAPHY.writeSlice(builder, GeoFunctions.toSphericalGeography(GeoFunctions.stGeometryFromText(utf8Slice(wkt))));
        }
        Block block = builder.build();
        for (int i = 0; i < wktList.size(); i++) {
            assertEquals(wktList.get(i), SPHERICAL_GEOGRAPHY.getObjectValue(null, block, i));
        }
    }

    @Test
    public void testToAndFromSphericalGeography()
    {
        // empty geometries
        assertThat(assertions.function("to_geometry", toSphericalGeography("POINT EMPTY")))
                .hasType(GEOMETRY)
                .matches("ST_GeometryFromText('POINT EMPTY')");

        assertThat(assertions.function("to_geometry", toSphericalGeography("MULTIPOINT EMPTY")))
                .hasType(GEOMETRY)
                .matches("ST_GeometryFromText('MULTIPOINT EMPTY')");

        assertThat(assertions.function("to_geometry", toSphericalGeography("LINESTRING EMPTY")))
                .hasType(GEOMETRY)
                .matches("ST_GeometryFromText('LINESTRING EMPTY')");

        assertThat(assertions.function("to_geometry", toSphericalGeography("MULTILINESTRING EMPTY")))
                .hasType(GEOMETRY)
                .matches("ST_GeometryFromText('MULTILINESTRING EMPTY')");

        assertThat(assertions.function("to_geometry", toSphericalGeography("POLYGON EMPTY")))
                .hasType(GEOMETRY)
                .matches("ST_GeometryFromText('POLYGON EMPTY')");

        assertThat(assertions.function("to_geometry", toSphericalGeography("MULTIPOLYGON EMPTY")))
                .hasType(GEOMETRY)
                .matches("ST_GeometryFromText('MULTIPOLYGON EMPTY')");

        assertThat(assertions.function("to_geometry", toSphericalGeography("GEOMETRYCOLLECTION EMPTY")))
                .hasType(GEOMETRY)
                .matches("ST_GeometryFromText('GEOMETRYCOLLECTION EMPTY')");

        // valid nonempty geometries
        assertThat(assertions.function("to_geometry", toSphericalGeography("POINT (-40.2 28.9)")))
                .hasType(GEOMETRY)
                .matches("ST_GeometryFromText('POINT (-40.2 28.9)')");

        assertThat(assertions.function("to_geometry", toSphericalGeography("MULTIPOINT ((-40.2 28.9), (-40.2 31.9))")))
                .hasType(GEOMETRY)
                .matches("ST_GeometryFromText('MULTIPOINT ((-40.2 28.9), (-40.2 31.9))')");

        assertThat(assertions.function("to_geometry", toSphericalGeography("LINESTRING (-40.2 28.9, -40.2 31.9, -37.2 31.9)")))
                .hasType(GEOMETRY)
                .matches("ST_GeometryFromText('LINESTRING (-40.2 28.9, -40.2 31.9, -37.2 31.9)')");

        assertThat(assertions.function("to_geometry", toSphericalGeography("MULTILINESTRING ((-40.2 28.9, -40.2 31.9), (-40.2 31.9, -37.2 31.9))")))
                .hasType(GEOMETRY)
                .matches("ST_GeometryFromText('MULTILINESTRING ((-40.2 28.9, -40.2 31.9), (-40.2 31.9, -37.2 31.9))')");

        assertThat(assertions.function("to_geometry", toSphericalGeography("POLYGON ((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9))")))
                .hasType(GEOMETRY)
                .matches("ST_GeometryFromText('POLYGON ((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9))')");

        assertThat(assertions.function("to_geometry", toSphericalGeography("POLYGON ((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9), (-39.2 29.9, -39.2 30.9, -38.2 30.9, -38.2 29.9, -39.2 29.9))")))
                .hasType(GEOMETRY)
                .matches("ST_GeometryFromText('POLYGON ((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9), (-39.2 29.9, -39.2 30.9, -38.2 30.9, -38.2 29.9, -39.2 29.9))')");

        assertThat(assertions.function("to_geometry", toSphericalGeography("MULTIPOLYGON (((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9)), ((-39.2 29.9, -38.2 29.9, -38.2 30.9, -39.2 30.9, -39.2 29.9)))")))
                .hasType(GEOMETRY)
                .matches("ST_GeometryFromText('MULTIPOLYGON (((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9)), ((-39.2 29.9, -38.2 29.9, -38.2 30.9, -39.2 30.9, -39.2 29.9)))')");

        assertThat(assertions.function("to_geometry", toSphericalGeography("GEOMETRYCOLLECTION (POINT (-40.2 28.9), LINESTRING (-40.2 28.9, -40.2 31.9, -37.2 31.9), POLYGON ((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9)))")))
                .hasType(GEOMETRY)
                .matches("ST_GeometryFromText('GEOMETRYCOLLECTION (POINT (-40.2 28.9), LINESTRING (-40.2 28.9, -40.2 31.9, -37.2 31.9), POLYGON ((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9)))')");

        // geometries containing invalid latitude or longitude values
        assertTrinoExceptionThrownBy(assertions.function("to_spherical_geography", "ST_GeometryFromText('POINT (-340.2 28.9)')")::evaluate)
                .hasMessage("Longitude must be between -180 and 180");

        assertTrinoExceptionThrownBy(assertions.function("to_spherical_geography", "ST_GeometryFromText('MULTIPOINT ((-40.2 128.9), (-40.2 31.9))')")::evaluate)
                .hasMessage("Latitude must be between -90 and 90");

        assertTrinoExceptionThrownBy(assertions.function("to_spherical_geography", "ST_GeometryFromText('LINESTRING (-40.2 28.9, -40.2 31.9, 237.2 31.9)')")::evaluate)
                .hasMessage("Longitude must be between -180 and 180");

        assertTrinoExceptionThrownBy(assertions.function("to_spherical_geography", "ST_GeometryFromText('MULTILINESTRING ((-40.2 28.9, -40.2 31.9), (-40.2 131.9, -37.2 31.9))')")::evaluate)
                .hasMessage("Latitude must be between -90 and 90");

        assertTrinoExceptionThrownBy(assertions.function("to_spherical_geography", "ST_GeometryFromText('POLYGON ((-40.2 28.9, -40.2 31.9, 237.2 31.9, -37.2 28.9, -40.2 28.9))')")::evaluate)
                .hasMessage("Longitude must be between -180 and 180");

        assertTrinoExceptionThrownBy(assertions.function("to_spherical_geography", "ST_GeometryFromText('POLYGON ((-40.2 28.9, -40.2 31.9, -37.2 131.9, -37.2 28.9, -40.2 28.9), (-39.2 29.9, -39.2 30.9, -38.2 30.9, -38.2 29.9, -39.2 29.9))')")::evaluate)
                .hasMessage("Latitude must be between -90 and 90");

        assertTrinoExceptionThrownBy(assertions.function("to_spherical_geography", "ST_GeometryFromText('MULTIPOLYGON (((-40.2 28.9, -40.2 31.9, -37.2 31.9, -37.2 28.9, -40.2 28.9)), ((-39.2 29.9, -39.2 30.9, 238.2 30.9, -38.2 29.9, -39.2 29.9)))')")::evaluate)
                .hasMessage("Longitude must be between -180 and 180");

        assertTrinoExceptionThrownBy(assertions.function("to_spherical_geography", "ST_GeometryFromText('GEOMETRYCOLLECTION (POINT (-40.2 28.9), LINESTRING (-40.2 28.9, -40.2 131.9, -37.2 31.9), POLYGON ((-40.2 28.9, -40.2 31.9, -37.2 31.9, -37.2 28.9, -40.2 28.9)))')")::evaluate)
                .hasMessage("Latitude must be between -90 and 90");
    }

    @Test
    public void testDistance()
    {
        assertThat(assertions.function("ST_Distance", toSphericalGeography("POINT (-86.67 36.12)"), toSphericalGeography("POINT (-118.40 33.94)")))
                .isEqualTo(2886448.9734367016);

        assertThat(assertions.function("ST_Distance", toSphericalGeography("POINT (-118.40 33.94)"), toSphericalGeography("POINT (-86.67 36.12)")))
                .isEqualTo(2886448.9734367016);

        assertThat(assertions.function("ST_Distance", toSphericalGeography("POINT (-71.0589 42.3601)"), toSphericalGeography("POINT (-71.2290 42.4430)")))
                .isEqualTo(16734.69743457383);

        assertThat(assertions.function("ST_Distance", toSphericalGeography("POINT (-86.67 36.12)"), toSphericalGeography("POINT (-86.67 36.12)")))
                .isEqualTo(0.0);

        assertThat(assertions.function("ST_Distance", toSphericalGeography("POINT EMPTY"), toSphericalGeography("POINT (40 30)")))
                .isNull(DOUBLE);

        assertThat(assertions.function("ST_Distance", toSphericalGeography("POINT (20 10)"), toSphericalGeography("POINT EMPTY")))
                .isNull(DOUBLE);

        assertThat(assertions.function("ST_Distance", toSphericalGeography("POINT EMPTY"), toSphericalGeography("POINT EMPTY")))
                .isNull(DOUBLE);
    }

    @Test
    public void testArea()
            throws Exception
    {
        // Empty polygon
        assertThat(assertions.expression("ST_Area(to_spherical_geography(ST_GeometryFromText('POLYGON EMPTY')))"))
                .hasType(DOUBLE)
                .isEqualTo((Object) null);

        // Invalid polygon (too few vertices)
        assertTrinoExceptionThrownBy(assertions.expression("ST_Area(to_spherical_geography(ST_GeometryFromText('POLYGON((90 0, 0 0))')))")::evaluate)
                .hasMessage("Polygon is not valid: a loop contains less then 3 vertices.");

        // Invalid data type (point)
        assertTrinoExceptionThrownBy(assertions.expression("ST_Area(to_spherical_geography(ST_GeometryFromText('POINT (0 1)')))")::evaluate)
                .hasMessage("When applied to SphericalGeography inputs, ST_Area only supports POLYGON or MULTI_POLYGON. Input type is: POINT");

        //Invalid Polygon (duplicated point)
        assertTrinoExceptionThrownBy(assertions.expression("ST_Area(to_spherical_geography(ST_GeometryFromText('POLYGON((0 0, 0 1, 1 1, 1 1, 1 0, 0 0))')))")::evaluate)
                .hasMessage("Polygon is not valid: it has two identical consecutive vertices");

        // A polygon around the North Pole
        assertThat(assertions.function("ST_Area", toSphericalGeography("POLYGON((-135 85, -45 85, 45 85, 135 85, -135 85))")))
                .satisfies(approximatelyEqualTo(619.00E9, 0.00001));

        assertThat(assertions.function("ST_Area", toSphericalGeography("POLYGON((0 0, 0 1, 1 1, 1 0))")))
                .satisfies(approximatelyEqualTo(123.64E8, 0.00001));

        assertThat(assertions.function("ST_Area", toSphericalGeography("POLYGON((-122.150124 37.486095, -122.149201 37.486606,  -122.145725 37.486580, -122.145923 37.483961 , -122.149324 37.482480 ,  -122.150837 37.483238,  -122.150901 37.485392))")))
                .satisfies(approximatelyEqualTo(163290.93943446054, 0.00001));

        double angleOfOneKm = 0.008993201943349;
        assertThat(assertions.function("ST_Area", toSphericalGeography(format("POLYGON((0 0, %.15f 0, %.15f %.15f, 0 %.15f))", angleOfOneKm, angleOfOneKm, angleOfOneKm, angleOfOneKm))))
                .satisfies(approximatelyEqualTo(1E6, 0.00001));

        // 1/4th of an hemisphere, ie 1/8th of the planet, should be close to 4PiR2/8 = 637.58E11
        assertThat(assertions.function("ST_Area", toSphericalGeography("POLYGON((90 0, 0 0, 0 90))")))
                .satisfies(approximatelyEqualTo(637.58E11, 0.00001));

        //A Polygon with a large hole
        assertThat(assertions.function("ST_Area", toSphericalGeography("POLYGON((90 0, 0 0, 0 90), (89 1, 1 1, 1 89))")))
                .satisfies(approximatelyEqualTo(348.04E10, 0.00001));

        Path geometryPath = new File(getResource("us-states.tsv").toURI()).toPath();
        Map<String, String> stateGeometries;
        try (Stream<String> lines = Files.lines(geometryPath)) {
            stateGeometries = lines
                    .map(line -> line.split("\t"))
                    .collect(Collectors.toMap(parts -> parts[0], parts -> parts[1]));
        }

        Path areaPath = new File(getResource("us-state-areas.tsv").toURI()).toPath();
        Map<String, Double> stateAreas;
        try (Stream<String> lines = Files.lines(areaPath)) {
            stateAreas = lines
                    .map(line -> line.split("\t"))
                    .filter(parts -> parts.length >= 2)
                    .collect(Collectors.toMap(parts -> parts[0], parts -> Double.valueOf(parts[1])));
        }

        for (String state : stateGeometries.keySet()) {
            String wkt = stateGeometries.get(state);
            double expectedArea = stateAreas.get(state);
            assertThat(assertions.function("ST_Area", toSphericalGeography(wkt)))
                    .satisfies(approximatelyEqualTo(expectedArea, 0.00001));
        }
    }

    private static Condition<Object> approximatelyEqualTo(double expected, double tolerance)
    {
        return new Condition<>((Object value) -> Math.abs(((Double) value) / expected - 1) < tolerance, "Approximately equal to %s within %s%%", expected, tolerance * 100);
    }

    private static String toSphericalGeography(String wkt)
    {
        return "to_spherical_geography(ST_GeometryFromText('%s'))".formatted(wkt);
    }
}
