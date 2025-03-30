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

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.google.common.collect.ImmutableList;
import io.trino.hive.formats.line.Column;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.type.DecimalType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static io.trino.plugin.base.util.JsonUtils.jsonFactory;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestEsriDeserializer
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
            new Column("price", DecimalType.createDecimalType(10, 2), 8));

    @Test
    void testDeserializeSimpleFeature()
            throws IOException
    {
        String json = """
                {
                    "attributes": {
                        "id": 1,
                        "name": "Test Feature",
                        "active": true,
                        "value": 123.45,
                        "date": 1741034025839,
                        "timestamp": 1741034025839,
                        "count": 42,
                        "price": "1234.56"
                    },
                    "geometry": {
                        "x": 10,
                        "y": 20
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
        assertGeometry(page, new Point(10, 20));
        assertThat(INTEGER.getLong(page.getBlock(7), 0)).isEqualTo(42);

        DecimalType decimalType = DecimalType.createDecimalType(10, 2);
        Block decimalBlock = page.getBlock(8);
        assertThat(decimalType.getLong(decimalBlock, 0)).isEqualTo(123456L);
    }

    @Test
    void testDeserializeNullValues() throws IOException
    {
        String json = """
            {
                "attributes": {
                    "id": null,
                    "name": null,
                    "active": null,
                    "value": null,
                    "date": null,
                    "timestamp": null,
                    "count": null,
                    "price": null
                },
                "geometry": null
            }
            """;

        Page page = parse(json);
        for (int i = 0; i < 9; i++) {
            assertThat(page.getBlock(i).isNull(0))
                    .as("Column at index " + i + " should be null")
                    .isTrue();
        }
    }

    @Test
    void testDeserializeMissingColumns() throws IOException
    {
        String json = """
            {
                "attributes": {
                },
                "geometry": null
            }
            """;

        Page page = parse(json);
        for (int i = 0; i < 9; i++) {
            assertThat(page.getBlock(i).isNull(0))
                    .as("Column at index " + i + " should be null")
                    .isTrue();
        }
    }

    @Test
    void testDeserializeInvalidJson()
    {
        String invalidJson = "{ This is not valid JSON }";
        assertThatThrownBy(() -> parse(invalidJson))
                .isInstanceOf(JsonParseException.class);
    }

    @Test
    void testDeserializeDateFormats() throws IOException {
        // Test valid epoch milliseconds (as number)
        String jsonEpoch = """
            {
                "attributes": {
                    "date": 1741034025839
                },
                "geometry": null
            }
            """;

        Page page = parse(jsonEpoch);
        assertThat(DATE.getLong(page.getBlock(4), 0)).isEqualTo(20150);

        // Test valid ISO date formats
        String[] validIsoFormats = {
                "2025-3-3",    // without leading zeros
                "2025-03-03"   // with leading zeros
        };

        for (String dateStr : validIsoFormats) {
            String json = String.format("""
                {
                    "attributes": {
                        "date": "%s"
                    },
                    "geometry": null
                }
                """, dateStr);

            page = parse(json);
            assertThat(DATE.getLong(page.getBlock(4), 0)).isEqualTo(20150);
        }

        // Test invalid date formats - should result in null
        String[] invalidDateFormats = {
                "2025/03/03",     // slash-separated date
                "03/03/2025"      // US-style date
        };

        for (String dateStr : invalidDateFormats) {
            String json = String.format("""
                {
                    "attributes": {
                        "date": "%s"
                    },
                    "geometry": null
                }
                """, dateStr);

            page = parse(json);
            assertThat(page.getBlock(4).isNull(0))
                    .as("Date '%s' should be parsed as null", dateStr)
                    .isTrue();
        }
    }

    @Test
    void testDeserializeTimestampFormats() throws IOException {
        String[] validTimestampFormats = {
                "2025-03-03 00:00:00.000",
                "2025-03-03 00:00:00",
                "2025-03-03 00:00",
                "2025-03-03"
        };

        String[] invalidTimestampFormats = {
                "2025/03/03 00:00:00",         // slash-separated date with time
                "03/03/2025 00:00:00 AM",      // US-style date with time
                "2025-03-03T00:00:00.000Z"     // ISO 8601 format
        };

        // Test valid epoch milliseconds (as number)
        String jsonEpoch = """
        {
            "attributes": {
                "timestamp": 1741034025839
            },
            "geometry": null
        }
        """;

        Page page = parse(jsonEpoch);
        assertThat(TIMESTAMP_MILLIS.getLong(page.getBlock(5), 0))
                .isEqualTo(1741034025839000L);

        // Test valid timestamps
        for (String timestampStr : validTimestampFormats) {
            String json = String.format("""
            {
                "attributes": {
                    "timestamp": "%s"
                },
                "geometry": null
            }
            """, timestampStr);

            page = parse(json);
            assertThat(TIMESTAMP_MILLIS.getLong(page.getBlock(5), 0))
                    .isEqualTo(1740960000000000L);
        }

        // Test invalid timestamps
        for (String timestampStr : invalidTimestampFormats) {
            String json = String.format("""
            {
                "attributes": {
                    "timestamp": "%s"
                },
                "geometry": null
            }
            """, timestampStr);

            page = parse(json);
            assertThat(page.getBlock(5).isNull(0))
                    .as("Timestamp '%s' should be parsed as null", timestampStr)
                    .isTrue();
        }
    }

    @Test
    void testDeserializeInvalidDateFormat() throws IOException {
        String json = """
            {
                "attributes": {
                    "date": "invalid-date"
                },
                "geometry": null
            }
            """;

        Page page = parse(json);
        assertThat(page.getBlock(4).isNull(0)).isTrue();
    }

    @Test
    void testDeserializeInvalidTimestampFormat() throws IOException {
        String json = """
            {
                "attributes": {
                    "timestamp": "invalid-timestamp"
                },
                "geometry": null
            }
            """;

        Page page = parse(json);
        assertThat(page.getBlock(5).isNull(0)).isTrue();
    }

    @Test
    void testDeserializeDateOutOfRange() throws IOException {
        String json = """
            {
                "attributes": {
                    "date": "9999-12-31"
                },
                "geometry": null
            }
            """;

        Page page = parse(json);
        assertThat(page.getBlock(4).isNull(0)).isFalse();
        assertThat(DATE.getLong(page.getBlock(4), 0)).isEqualTo(2932896);
    }

    @Test
    void testDeserializeInvalid()
            throws IOException
    {
        String json = """
                {
                    "extra-junk": {
                        "geometry": null,
                        "attributes": {
                            "id": 42
                        }
                    },
                    "attributes": {
                        "id": 1
                    },
                    "geometry": {
                        "x": 10,
                        "y": 20
                    }
                }
                """;

        Page page = parse(json);
        assertThat(BIGINT.getLong(page.getBlock(0), 0)).isEqualTo(1L);
        assertThat(page.getBlock(6).isNull(0)).isFalse();
        assertThat(VARBINARY.getSlice(page.getBlock(6), 0)).isNotNull();
    }

    @Test
    void testMissingAttributes()
            throws IOException
    {
        String json = """
                {
                    "geometry": {
                        "x": 10,
                        "y": 20
                    }
                }
                """;

        Page page = parse(json);
        assertGeometry(page, new Point(10, 20));
    }

    @Test
    void testDuplicateGeometry()
            throws IOException
    {
        String json = """
                {
                    "geometry": {
                        "x": 88,
                        "y": 99
                    },
                    "geometry": {
                        "x": 10,
                        "y": 20
                    }
                }
                """;

        Page page = parse(json);
        assertGeometry(page, new Point(10, 20));
    }

    @Test
    void testArrayGeometryFails()
    {
        String json = """
                {
                    "geometry": []
                }
                """;

        assertThatThrownBy(() -> parse(json))
                .isInstanceOf(IOException.class)
                .hasMessage("Invalid JSON: Geometry is not an object");
    }

    @Test
    void testNumberGeometryFails()
    {
        String json = """
                {
                    "geometry": 42
                }
                """;

        assertThatThrownBy(() -> parse(json))
                .isInstanceOf(IOException.class)
                .hasMessage("Invalid JSON: Geometry is not an object");
    }

    @Test
    void testNullAttributes()
            throws IOException
    {
        String json = """
                {
                    "geometry": {
                        "x": 5,
                        "y": 7
                    }
                }
                """;

        Page page = parse(json);
        assertGeometry(page, new Point(5, 7));
    }

    @Test
    void testArrayAttributes()
    {
        String json = """
                {
                    "attributes": []
                }
                """;

        assertThatThrownBy(() -> parse(json))
                .isInstanceOf(IOException.class)
                .hasMessage("Invalid JSON: Attributes is not an object");
    }

    @Test
    void testNumberAttributes()
    {
        String json = """
                {
                    "attributes": 42
                }
                """;

        assertThatThrownBy(() -> parse(json))
                .isInstanceOf(IOException.class)
                .hasMessage("Invalid JSON: Attributes is not an object");
    }

    @Test
    void testDuplicateAttribute()
            throws IOException
    {
        String json = """
                      {
                          "attributes": {
                              "id": 1,
                              "id": 2
                          }
                      }
                      """;

        Page page = parse(json);
        assertThat(BIGINT.getLong(page.getBlock(0), 0)).isEqualTo(2L);
    }

    private static Page parse(String json)
            throws IOException
    {
        JsonParser jsonParser = JSON_FACTORY.createParser(json);
        assertThat(jsonParser.nextToken()).isEqualTo(START_OBJECT);

        EsriDeserializer deserializer = new EsriDeserializer(COLUMNS);
        PageBuilder pageBuilder = new PageBuilder(deserializer.getTypes());
        deserializer.deserialize(pageBuilder, jsonParser);
        Page page = pageBuilder.build();
        assertThat(page.getPositionCount()).isEqualTo(1);
        return page;
    }

    private static void assertGeometry(Page page, Geometry expected)
    {
        if (expected == null) {
            assertThat(page.getBlock(6).isNull(0)).isTrue();
            return;
        }

        assertThat(page.getBlock(6).isNull(0)).isFalse();
        assertThat(OGCGeometry.fromEsriShape(VARBINARY.getSlice(page.getBlock(6), 0).toByteBuffer()).getEsriGeometry())
                .isEqualTo(expected);
    }
}
