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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.plugin.geospatial.GeometryType.GEOMETRY;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestEncodedPolylineFunctions
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
    public void testFromEncodedPolyline()
    {
        assertThat(assertions.function("from_encoded_polyline", "''"))
                .hasType(GEOMETRY)
                .matches("ST_GeometryFromText('LINESTRING EMPTY')");

        assertThat(assertions.function("from_encoded_polyline", "'iiqeFjs_jV'"))
                .hasType(GEOMETRY)
                .matches("ST_GeometryFromText('LINESTRING EMPTY')");

        assertThat(assertions.function("from_encoded_polyline", "'_p~iF~ps|U_ulLnnqC_mqNvxq`@'"))
                .hasType(GEOMETRY)
                .matches("ST_GeometryFromText('LINESTRING (-120.2 38.5, -120.95 40.7, -126.45300000000002 43.252)')");
    }

    @Test
    public void testToEncodedPolyline()
    {
        assertThat(assertions.function("to_encoded_polyline", "ST_GeometryFromText('LINESTRING EMPTY')"))
                .hasType(VARCHAR)
                .isEqualTo("");

        assertThat(assertions.function("to_encoded_polyline", "ST_GeometryFromText('LINESTRING (-120.2 38.5, -120.95 40.7, -126.453 43.252)')"))
                .hasType(VARCHAR)
                .isEqualTo("_p~iF~ps|U_ulLnnqC_mqNvxq`@");

        assertThat(assertions.function("to_encoded_polyline", "ST_GeometryFromText('LINESTRING (-120.2 38.5, -120.95 40.7, -126.453 43.252, -128.318 46.102)')"))
                .hasType(VARCHAR)
                .isEqualTo("_p~iF~ps|U_ulLnnqC_mqNvxq`@oskPfgkJ");

        assertThat(assertions.function("to_encoded_polyline", "ST_GeometryFromText('MULTIPOINT EMPTY')"))
                .hasType(VARCHAR)
                .isEqualTo("");

        assertThat(assertions.function("to_encoded_polyline", "ST_GeometryFromText('MULTIPOINT (-120.2 38.5)')"))
                .hasType(VARCHAR)
                .isEqualTo("_p~iF~ps|U");

        assertThat(assertions.function("to_encoded_polyline", "ST_GeometryFromText('MULTIPOINT (-120.2 38.5, -120.95 40.7, -126.453 43.252)')"))
                .hasType(VARCHAR)
                .isEqualTo("_p~iF~ps|U_ulLnnqC_mqNvxq`@");

        assertThat(assertions.function("to_encoded_polyline", "ST_GeometryFromText('MULTIPOINT (-120.2 38.5, -120.95 40.7, -126.453 43.252, -128.318 46.102)')"))
                .hasType(VARCHAR)
                .isEqualTo("_p~iF~ps|U_ulLnnqC_mqNvxq`@oskPfgkJ");

        assertTrinoExceptionThrownBy(assertions.expression("to_encoded_polyline(ST_GeometryFromText('POINT (-120.2 38.5)'))")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.expression("to_encoded_polyline(ST_GeometryFromText('MULTILINESTRING ((-122.39174 37.77701))'))")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }
}
