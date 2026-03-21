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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.google.common.collect.ImmutableList;
import io.trino.hive.formats.line.Column;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static io.trino.hive.formats.esri.EsriDeserializer.Format.GEO_JSON;
import static io.trino.hive.formats.esri.TestEsriDeserializer.assertGeometry;
import static io.trino.plugin.base.util.JsonUtils.jsonFactory;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGeoJsonDeserializer
{
    private static final JsonFactory JSON_FACTORY = jsonFactory();
    private static final List<Column> COLUMNS = ImmutableList.of(
            new Column("id", BIGINT, 0),
            new Column("name", VARCHAR, 1),
            new Column("active", BOOLEAN, 2),
            new Column("value", DOUBLE, 3),
            new Column("date", DATE, 4),
            new Column("timestamp", TIMESTAMP_MILLIS, 5),
            new Column("geometry", VARBINARY, 6),
            new Column("count", INTEGER, 7),
            new Column("price", DecimalType.createDecimalType(10, 2), 8),
            new Column("small_num", SMALLINT, 9),
            new Column("tiny_num", TINYINT, 10),
            new Column("real_num", REAL, 11),
            new Column("fixed_text", CharType.createCharType(10), 12));

    @Test
    void testPolygon()
            throws IOException
    {
        String json =
                """
                {
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
                        "bigint": 1337,
                        "name": "test"
                    }
                }
                """;

        Page page = parse(json);
        assertThat(page.getPositionCount()).isEqualTo(1);
    }

    @Test
    public void testDeserializeSimpleFeature()
            throws IOException
    {
        String json =
                """
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [13.0, 37.0]
                    },
                    "properties": {
                        "id": 1,
                        "name": "Test Feature",
                        "active": true,
                        "value": 123.45,
                        "date": 1741034025839,
                        "timestamp": 1741034025839,
                        "count": 42,
                        "price": "1234.56"
                    }
                }
                """;

        Page page = parse(json);
        assertThat(page.getPositionCount()).isEqualTo(1);

        for (int i = 0; i < 9; i++) {
            assertThat(page.getBlock(i).isNull(0))
                    .as("Column at index " + i + " should not be null")
                    .isFalse();
        }
        assertThat(BIGINT.getLong(page.getBlock(0), 0)).isEqualTo(1L);
        assertThat(VARCHAR.getSlice(page.getBlock(1), 0).toStringUtf8()).isEqualTo("Test Feature");
        assertThat(BOOLEAN.getBoolean(page.getBlock(2), 0)).isTrue();
        assertThat(DOUBLE.getDouble(page.getBlock(3), 0)).isEqualTo(123.45);
        assertThat(DATE.getLong(page.getBlock(4), 0)).isEqualTo(20150);
        assertThat(TIMESTAMP_MILLIS.getLong(page.getBlock(5), 0)).isEqualTo(1741034025839000L);
        assertGeometry(page, "POINT (13.0, 37.0)");
        assertThat(INTEGER.getLong(page.getBlock(7), 0)).isEqualTo(42);

        DecimalType decimalType = DecimalType.createDecimalType(10, 2);
        Block decimalBlock = page.getBlock(8);
        assertThat(decimalType.getLong(decimalBlock, 0)).isEqualTo(123456L);
    }

    @Test
    public void testDeserializeNullValues()
            throws IOException
    {
        String json =
                """
                {
                    "type": "Feature",
                    "properties": {
                        "id": null,
                        "name": null,
                        "active": null,
                        "value": null,
                        "date": null,
                        "timestamp": null,
                        "count": null,
                        "price": null
                    }
                }
                """;

        Page page = parse(json);
        for (int i = 0; i < 9; i++) {
            assertThat(page.getBlock(i).isNull(0))
                    .as("Column at index " + i + " should be null but was " + page.getBlock(i).getClass().getSimpleName())
                    .isTrue();
        }
    }

    private static Page parse(String json)
            throws IOException
    {
        return parse(json, COLUMNS);
    }

    private static Page parse(String json, List<Column> columns)
            throws IOException
    {
        JsonParser jsonParser = JSON_FACTORY.createParser(json);
        assertThat(jsonParser.nextToken()).isEqualTo(START_OBJECT);

        EsriDeserializer deserializer = new EsriDeserializer(columns, GEO_JSON);
        PageBuilder pageBuilder = new PageBuilder(deserializer.getTypes());
        deserializer.deserialize(pageBuilder, jsonParser);
        Page page = pageBuilder.build();
        assertThat(page.getPositionCount()).isEqualTo(1);
        return page;
    }
}
