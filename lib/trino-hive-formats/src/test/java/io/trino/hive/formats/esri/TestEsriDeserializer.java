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
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
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
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestEsriDeserializer
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
    public void testDeserializeSimpleFeature()
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
    public void testDeserializeNullValues()
            throws IOException
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
    public void testSupportedAttributeTypes()
            throws IOException
    {
        String json = """
            {
                "attributes": {
                    "id": 9223372036854775807,
                    "name": "string value",
                    "active": true,
                    "value": 123.456789,
                    "date": "2025-03-03",
                    "timestamp": "2025-03-03 12:34:56.789",
                    "count": 2147483647,
                    "price": "99999999.99",
                    "small_num": 32767,
                    "tiny_num": 127,
                    "real_num": 3.14159,
                    "fixed_text": "FIXED      "
                }
            }
            """;

        Page page = parse(json);

        // Test BIGINT
        assertThat(BIGINT.getLong(page.getBlock(0), 0)).isEqualTo(9223372036854775807L);

        // Test VARCHAR
        assertThat(VARCHAR.getSlice(page.getBlock(1), 0).toStringUtf8()).isEqualTo("string value");

        // Test BOOLEAN
        assertThat(BOOLEAN.getBoolean(page.getBlock(2), 0)).isTrue();

        // Test DOUBLE
        assertThat(DOUBLE.getDouble(page.getBlock(3), 0)).isEqualTo(123.456789);

        // Test DATE
        assertThat(DATE.getLong(page.getBlock(4), 0)).isEqualTo(20150);

        // Test TIMESTAMP
        assertThat(TIMESTAMP_MILLIS.getLong(page.getBlock(5), 0)).isEqualTo(1741005296789000L);

        // Test INTEGER
        assertThat(INTEGER.getLong(page.getBlock(7), 0)).isEqualTo(2147483647);

        // Test DECIMAL
        DecimalType decimalType = DecimalType.createDecimalType(10, 2);
        Block decimalBlock = page.getBlock(8);
        assertThat(decimalType.getLong(decimalBlock, 0)).isEqualTo(9999999999L);

        // Test SMALLINT
        assertThat(SMALLINT.getLong(page.getBlock(9), 0)).isEqualTo(32767);

        // Test TINYINT
        assertThat(TINYINT.getLong(page.getBlock(10), 0)).isEqualTo(127);

        // Test REAL
        float expectedFloat = 3.14159f;
        assertThat(REAL.getLong(page.getBlock(11), 0)).isEqualTo(floatToRawIntBits(expectedFloat));

        // Test CHAR
        CharType charType = CharType.createCharType(10);
        assertThat(charType.getSlice(page.getBlock(12), 0).toStringUtf8()).isEqualTo("FIXED");
    }

    @Test
    public void testUnsupportedAttributeTypes()
    {
        String json = """
        {
            "attributes": {
                "id": 1,
                "name": "Test Feature",
                "varbinary_field": "Some binary data",
                "value": 123.45
            },
            "geometry": {
                "x": 10,
                "y": 20
            }
        }
        """;

        List<Column> columns = ImmutableList.of(
                new Column("id", BIGINT, 0),
                new Column("name", VARCHAR, 1),
                new Column("varbinary_field", VARBINARY, 2),
                new Column("value", DOUBLE, 3)
        );

        assertThatThrownBy(() -> parse(json, columns))
                .isInstanceOf(EsriDeserializer.UnsupportedTypeException.class)
                .hasMessageContaining("Column 'varbinary_field' with type: varbinary is not supported");
    }

    @Test
    public void testDeserializeMissingColumns()
            throws IOException
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
    public void testDeserializeInvalidJson()
    {
        String invalidJson = "{ This is not valid JSON }";
        assertThatThrownBy(() -> parse(invalidJson))
                .isInstanceOf(JsonParseException.class);
    }

    @Test
    public void testDeserializeDateFormats()
            throws IOException
    {
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
    public void testDeserializeTimestampFormats()
            throws IOException
    {
        String[] validTimestampFormats = {
                "2025-03-03 00:00:00.000", // with leading zeros
                "2025-03-03 00:00:00",
                "2025-03-03 00:00",
                "2025-03-03",
                "2025-3-3 00:00:00.000", // without leading zeros
                "2025-3-3 00:00:00",
                "2025-3-3 00:00",
                "2025-3-3"
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
    public void testDeserializeInvalidDateFormat()
            throws IOException
    {
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
    public void testDeserializeInvalidTimestampFormat()
            throws IOException
    {
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
    public void testDeserializeDateOutOfRange()
            throws IOException
    {
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
    public void testDeserializeInvalid()
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
    public void testMissingAttributes()
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
    public void testDuplicateGeometry()
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
    public void testArrayGeometryFails()
    {
        String json = """
                {
                    "geometry": []
                }
                """;

        assertThatThrownBy(() -> parse(json))
                .isInstanceOf(IOException.class)
                .hasMessage("Invalid JSON: geometry is not an object");
    }

    @Test
    public void testNumberGeometryFails()
    {
        String json = """
                {
                    "geometry": 42
                }
                """;

        assertThatThrownBy(() -> parse(json))
                .isInstanceOf(IOException.class)
                .hasMessage("Invalid JSON: geometry is not an object");
    }

    @Test
    public void testNullAttributes()
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
    public void testArrayAttributes()
    {
        String json = """
                {
                    "attributes": []
                }
                """;

        assertThatThrownBy(() -> parse(json))
                .isInstanceOf(IOException.class)
                .hasMessage("Invalid JSON: attributes is not an object");
    }

    @Test
    public void testNumberAttributes()
    {
        String json = """
                {
                    "attributes": 42
                }
                """;

        assertThatThrownBy(() -> parse(json))
                .isInstanceOf(IOException.class)
                .hasMessage("Invalid JSON: attributes is not an object");
    }

    @Test
    public void testDuplicateAttribute()
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
        return parse(json, COLUMNS);
    }

    private static Page parse(String json, List<Column> columns)
            throws IOException
    {
        JsonParser jsonParser = JSON_FACTORY.createParser(json);
        assertThat(jsonParser.nextToken()).isEqualTo(START_OBJECT);

        EsriDeserializer deserializer = new EsriDeserializer(columns);
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

        byte[] actual = VARBINARY.getSlice(page.getBlock(6), 0).getBytes();

        byte[] expectedShape = GeometryEngine.geometryToEsriShape(expected);
        byte[] expectedBytes = new byte[4 + 1 + expectedShape.length];

        OGCType ogcType = switch (expected.getType()) {
            case Point -> OGCType.ST_POINT;
            case Line -> OGCType.ST_LINESTRING;
            case Polygon -> OGCType.ST_POLYGON;
            case MultiPoint -> OGCType.ST_MULTIPOINT;
            case Polyline -> OGCType.ST_MULTILINESTRING;
            default -> OGCType.UNKNOWN;
        };
        expectedBytes[4] = ogcType.getIndex();
        System.arraycopy(expectedShape, 0, expectedBytes, 5, expectedShape.length);

        assertThat(actual).isEqualTo(expectedBytes);
    }
}
