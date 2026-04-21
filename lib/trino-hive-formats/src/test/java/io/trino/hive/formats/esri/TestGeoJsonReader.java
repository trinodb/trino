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
package io.trino.hive.formats.esri;

import io.trino.hive.formats.line.Column;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKTReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import static io.trino.hive.formats.esri.EsriDeserializer.Format.GEO_JSON;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGeoJsonReader
{
    private static final List<Column> TEST_COLUMNS = List.of(
            new Column("name", VARCHAR, 0),
            new Column("geometry", VARBINARY, 1));

    @Test
    public void testReadFeatureCollection()
            throws IOException
    {
        String json =
                """
                {
                   "type": "FeatureCollection",
                   "features": [{
                       "type": "Feature",
                       "geometry": {
                           "type": "Point",
                           "coordinates": [102.0, 0.5]
                       },
                       "properties": {
                           "name": "feature1"
                       }
                   }, {
                       "type": "Feature",
                       "geometry": {
                           "type": "LineString",
                           "coordinates": [
                               [102.0, 0.0],
                               [103.0, 1.0],
                               [104.0, 0.0],
                               [105.0, 1.0]
                           ]
                       },
                       "properties": {
                           "name": "feature2"
                       }
                   }, {
                       "type": "Feature",
                       "geometry": {
                           "type": "Polygon",
                           "coordinates": [
                               [
                                   [100.0, 0.0],
                                   [101.0, 0.0],
                                   [101.0, 1.0],
                                   [100.0, 1.0],
                                   [100.0, 0.0]
                               ]
                           ]
                       },
                       "properties": {
                           "name": "feature3"
                       }
                   }]
                }
                """;

        Page page = readAll(json);
        assertThat(page.getPositionCount()).isEqualTo(3);

        for (int i = 0; i < 3; i++) {
            assertThat(VARCHAR.getSlice(page.getBlock(0), i).toStringUtf8()).isEqualTo("feature" + (i + 1));
        }
        assertGeometry(page, "POINT (102.0 0.5)", 4326, 1, 0);
        assertGeometry(page, "LINESTRING (102.0 0.0, 103.0 1.0, 104.0 0.0, 105.0 1.0)", 4326, 1, 1);
        assertGeometry(page, "POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0))", 4326, 1, 2);
    }

    @Test
    public void testEmptyFeatures()
            throws IOException
    {
        String json =
                """
                {
                    "type": "FeatureCollection",
                    "features": []
                }
                """;

        Page page = readAll(json);
        assertThat(page.getPositionCount()).isZero();
    }

    @Test
    public void testNullFeatures()
            throws IOException
    {
        String json =
                """
                {
                    "type": "FeatureCollection",
                    "features": null
                }
                """;

        Page page = readAll(json);
        assertThat(page.getPositionCount()).isZero();
    }

    static void assertGeometry(Page page, String expectedWkt, int expectedSrid, int channel, int position)
    {
        assertThat(page.getBlock(channel).isNull(position)).isFalse();

        try {
            byte[] actualWkb = VARBINARY.getSlice(page.getBlock(channel), position).getBytes();
            Geometry actualGeometry = new WKBReader().read(actualWkb);
            Geometry expectedGeometry = new WKTReader().read(expectedWkt);
            assertThat(actualGeometry.equalsExact(expectedGeometry)).isTrue();
            assertThat(actualGeometry.getSRID()).isEqualTo(expectedSrid);
        }
        catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private static Page readAll(String json)
            throws IOException
    {
        EsriDeserializer deserializer = new EsriDeserializer(TEST_COLUMNS, GEO_JSON);
        EsriReader reader = new EsriReader(new ByteArrayInputStream(json.getBytes(UTF_8)), deserializer);
        PageBuilder pageBuilder = new PageBuilder(deserializer.getTypes());
        try {
            while (reader.next(pageBuilder)) {
                assertThat(reader.isClosed()).isFalse();
            }
        }
        finally {
            reader.close();
            assertThat(reader.getBytesRead()).isPositive();
            if (!pageBuilder.isEmpty()) {
                assertThat(reader.getReadTimeNanos()).isPositive();
            }
            assertThat(reader.isClosed()).isTrue();
        }

        return pageBuilder.build();
    }
}
