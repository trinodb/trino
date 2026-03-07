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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

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
                           "name": "feature2"
                       }
                   }]
                }
                """;

        Page page = readAll(json);
        assertThat(page.getPositionCount()).isEqualTo(3);
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

    private static Page readAll(String json)
            throws IOException
    {
        GeoJsonDeserializer deserializer = new GeoJsonDeserializer(TEST_COLUMNS);
        GeoJsonReader reader = new GeoJsonReader(new ByteArrayInputStream(json.getBytes(UTF_8)), deserializer);
        PageBuilder pageBuilder = new PageBuilder(deserializer.getTypes());
        try {
            boolean closed = reader.isClosed();
            while (reader.next(pageBuilder)) {
                assertThat(closed).isFalse();
                closed = reader.isClosed();
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
