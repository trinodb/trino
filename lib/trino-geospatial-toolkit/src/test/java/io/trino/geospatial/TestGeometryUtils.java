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

import org.junit.jupiter.api.Test;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import static io.trino.geospatial.GeometryUtils.jsonFromJtsGeometry;
import static org.assertj.core.api.Assertions.assertThat;

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
}
