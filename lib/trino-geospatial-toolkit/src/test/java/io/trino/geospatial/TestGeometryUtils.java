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
import com.esri.core.geometry.ogc.OGCGeometry;
import org.testng.annotations.Test;

import static io.trino.geospatial.GeometryUtils.getExtent;
import static org.testng.Assert.assertEquals;

public class TestGeometryUtils
{
    @Test
    public void testGetExtent()
    {
        assertGetExtent(
                "POINT (-23.4 12.2)",
                new Rectangle(-23.4, 12.2, -23.4, 12.2));
        assertGetExtent(
                "LINESTRING (-75.9375 23.6359, -75.9375 23.6364)",
                new Rectangle(-75.9375, 23.6359, -75.9375, 23.6364));
        assertGetExtent(
                "GEOMETRYCOLLECTION (" +
                        "  LINESTRING (-75.9375 23.6359, -75.9375 23.6364)," +
                        "  MULTIPOLYGON (((-75.9375 23.45520, -75.9371 23.4554, -75.9375 23.46023325, -75.9375 23.45520)))" +
                        ")",
                new Rectangle(-75.9375, 23.4552, -75.9371, 23.6364));
    }

    private void assertGetExtent(String wkt, Rectangle expected)
    {
        assertEquals(getExtent(OGCGeometry.fromText(wkt)), expected);
    }
}
