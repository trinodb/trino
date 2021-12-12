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

import io.trino.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public class TestEncodedPolylineFunctions
        extends AbstractTestFunctions
{
    @BeforeClass
    public void registerFunctions()
    {
        functionAssertions.installPlugin(new GeoPlugin());
    }

    @Test
    public void testFromEncodedPolyline()
    {
        assertFromEncodedPolyline("", "LINESTRING EMPTY");
        assertFromEncodedPolyline("iiqeFjs_jV", "LINESTRING EMPTY");
        assertFromEncodedPolyline("_p~iF~ps|U_ulLnnqC_mqNvxq`@", "LINESTRING (-120.2 38.5, -120.95 40.7, -126.45300000000002 43.252)");
    }

    private void assertFromEncodedPolyline(String polyline, String wkt)
    {
        assertFunction(format("ST_AsText(from_encoded_polyline('%s'))", polyline), VARCHAR, wkt);
    }

    @Test
    public void testToEncodedPolyline()
    {
        assertToEncodedPolyline("LINESTRING EMPTY", "");
        assertToEncodedPolyline("LINESTRING (-120.2 38.5, -120.95 40.7, -126.453 43.252)", "_p~iF~ps|U_ulLnnqC_mqNvxq`@");
        assertToEncodedPolyline("LINESTRING (-120.2 38.5, -120.95 40.7, -126.453 43.252, -128.318 46.102)", "_p~iF~ps|U_ulLnnqC_mqNvxq`@oskPfgkJ");

        assertToEncodedPolyline("MULTIPOINT EMPTY", "");
        assertToEncodedPolyline("MULTIPOINT (-120.2 38.5)", "_p~iF~ps|U");
        assertToEncodedPolyline("MULTIPOINT (-120.2 38.5, -120.95 40.7, -126.453 43.252)", "_p~iF~ps|U_ulLnnqC_mqNvxq`@");
        assertToEncodedPolyline("MULTIPOINT (-120.2 38.5, -120.95 40.7, -126.453 43.252, -128.318 46.102)", "_p~iF~ps|U_ulLnnqC_mqNvxq`@oskPfgkJ");

        assertInvalidFunction("to_encoded_polyline(ST_GeometryFromText('POINT (-120.2 38.5)'))", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("to_encoded_polyline(ST_GeometryFromText('MULTILINESTRING ((-122.39174 37.77701))'))", INVALID_FUNCTION_ARGUMENT);
    }

    private void assertToEncodedPolyline(String wkt, String polyline)
    {
        assertFunction(format("to_encoded_polyline(ST_GeometryFromText('%s'))", wkt), VARCHAR, polyline);
    }
}
