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
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.google.common.collect.ImmutableList;
import io.trino.hive.formats.line.Column;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.type.DecimalType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
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

public class TestEsriDeserializer
{
    private static final JsonFactory JSON_FACTORY = jsonFactory();

    private EsriDeserializer deserializer;
    private PageBuilder pageBuilder;

    @BeforeEach
    void setUp()
    {
        List<Column> columns = ImmutableList.of(
                new Column("id", BIGINT, 0),
                new Column("name", VARCHAR, 1),
                new Column("active", BOOLEAN, 2),
                new Column("value", DOUBLE, 3),
                new Column("date", DATE, 4),
                new Column("timestamp", TIMESTAMP_MILLIS, 5),
                new Column("geometry", VARBINARY, 6),
                new Column("count", INTEGER, 7),
                new Column("price", DecimalType.createDecimalType(10, 2), 8));

        deserializer = new EsriDeserializer(columns);
        pageBuilder = new PageBuilder(columns.stream().map(Column::type).collect(toImmutableList()));
    }

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

        JsonParser jsonParser = JSON_FACTORY.createParser(json);
        deserializer.deserialize(pageBuilder, jsonParser);

        assertThat(pageBuilder.getPositionCount()).isEqualTo(1);

        assertThat(BIGINT.getLong(pageBuilder.getBlockBuilder(0).build(), 0)).isEqualTo(1L);
        assertThat(VARCHAR.getSlice(pageBuilder.getBlockBuilder(1).build(), 0).toStringUtf8()).isEqualTo("Test Feature");
        assertThat(BOOLEAN.getBoolean(pageBuilder.getBlockBuilder(2).build(), 0)).isTrue();
        assertThat(DOUBLE.getDouble(pageBuilder.getBlockBuilder(3).build(), 0)).isEqualTo(123.45);
        assertThat(DATE.getLong(pageBuilder.getBlockBuilder(4).build(), 0)).isEqualTo(20150);
        assertThat(TIMESTAMP_MILLIS.getLong(pageBuilder.getBlockBuilder(5).build(), 0)).isEqualTo(1741034025839000L);
        assertThat(VARBINARY.getSlice(pageBuilder.getBlockBuilder(6).build(), 0)).isNotNull();
        assertThat(INTEGER.getLong(pageBuilder.getBlockBuilder(7).build(), 0)).isEqualTo(42);

        DecimalType decimalType = DecimalType.createDecimalType(10, 2);
        Block decimalBlock = pageBuilder.getBlockBuilder(8).build();
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

        JsonParser jsonParser = JSON_FACTORY.createParser(json);
        deserializer.deserialize(pageBuilder, jsonParser);

        assertThat(pageBuilder.getPositionCount()).isEqualTo(1);
        for (int i = 0; i < 9; i++) {
            assertThat(pageBuilder.getBlockBuilder(i).build().isNull(0))
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

        JsonParser jsonParser = JSON_FACTORY.createParser(json);
        deserializer.deserialize(pageBuilder, jsonParser);

        assertThat(pageBuilder.getPositionCount()).isEqualTo(1);
        for (int i = 0; i < 9; i++) {
            assertThat(pageBuilder.getBlockBuilder(i).build().isNull(0))
                    .as("Column at index " + i + " should be null")
                    .isTrue();
        }
    }

    @Test
    void testDeserializeMultipleFeatures() throws IOException {
        String json1 = """
            {
                "attributes": {
                    "id": 1,
                    "name": "Feature 1"
                },
                "geometry": {
                    "x": 10,
                    "y": 20
                }
            }
            """;
        String json2 = """
            {
                "attributes": {
                    "id": 2,
                    "name": "Feature 2"
                },
                "geometry": {
                    "x": 30,
                    "y": 40
                }
            }
            """;

        JsonParser jsonParser = JSON_FACTORY.createParser(json1);
        deserializer.deserialize(pageBuilder, jsonParser);

        jsonParser = JSON_FACTORY.createParser(json2);
        deserializer.deserialize(pageBuilder, jsonParser);

        assertThat(pageBuilder.getPositionCount()).isEqualTo(2);

        Block idBlock = pageBuilder.getBlockBuilder(0).build();
        assertThat(BIGINT.getLong(idBlock, 0)).isEqualTo(1L);
        assertThat(BIGINT.getLong(idBlock, 1)).isEqualTo(2L);

        Block nameBlock = pageBuilder.getBlockBuilder(1).build();
        assertThat(VARCHAR.getSlice(nameBlock, 0).toStringUtf8()).isEqualTo("Feature 1");
        assertThat(VARCHAR.getSlice(nameBlock, 1).toStringUtf8()).isEqualTo("Feature 2");
    }

    @Test
    void testDeserializeInvalidJson()
            throws IOException
    {
        String invalidJson = "This is not valid JSON";
        JsonParser jsonParser = JSON_FACTORY.createParser(invalidJson);
        assertThatThrownBy(() -> deserializer.deserialize(pageBuilder, jsonParser))
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

        JsonParser jsonParser = JSON_FACTORY.createParser(jsonEpoch);
        deserializer.deserialize(pageBuilder, jsonParser);
        assertThat(pageBuilder.getPositionCount()).isEqualTo(1);
        assertThat(DATE.getLong(pageBuilder.getBlockBuilder(4).build(), 0)).isEqualTo(20150);
        pageBuilder.reset();

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

            jsonParser = JSON_FACTORY.createParser(json);
            deserializer.deserialize(pageBuilder, jsonParser);
            assertThat(pageBuilder.getPositionCount()).isEqualTo(1);
            assertThat(DATE.getLong(pageBuilder.getBlockBuilder(4).build(), 0)).isEqualTo(20150);
            pageBuilder.reset();
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

            jsonParser = JSON_FACTORY.createParser(json);
            deserializer.deserialize(pageBuilder, jsonParser);
            assertThat(pageBuilder.getPositionCount()).isEqualTo(1);
            assertThat(pageBuilder.getBlockBuilder(4).build().isNull(0))
                    .as("Date '%s' should be parsed as null", dateStr)
                    .isTrue();

            pageBuilder.reset();
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

        JsonParser jsonParser = JSON_FACTORY.createParser(jsonEpoch);
        deserializer.deserialize(pageBuilder, jsonParser);
        assertThat(pageBuilder.getPositionCount()).isEqualTo(1);
        assertThat(TIMESTAMP_MILLIS.getLong(pageBuilder.getBlockBuilder(5).build(), 0))
                .isEqualTo(1741034025839000L);

        pageBuilder.reset();

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

            jsonParser = JSON_FACTORY.createParser(json);
            deserializer.deserialize(pageBuilder, jsonParser);
            assertThat(pageBuilder.getPositionCount()).isEqualTo(1);
            assertThat(TIMESTAMP_MILLIS.getLong(pageBuilder.getBlockBuilder(5).build(), 0))
                    .isEqualTo(1740960000000000L);

            pageBuilder.reset();
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

            jsonParser = JSON_FACTORY.createParser(json);
            deserializer.deserialize(pageBuilder, jsonParser);
            assertThat(pageBuilder.getPositionCount()).isEqualTo(1);
            assertThat(pageBuilder.getBlockBuilder(5).build().isNull(0))
                    .as("Timestamp '%s' should be parsed as null", timestampStr)
                    .isTrue();

            pageBuilder.reset();
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

        JsonParser jsonParser = JSON_FACTORY.createParser(json);
        deserializer.deserialize(pageBuilder, jsonParser);
        assertThat(pageBuilder.getPositionCount()).isEqualTo(1);
        assertThat(pageBuilder.getBlockBuilder(4).build().isNull(0)).isTrue();
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

        JsonParser jsonParser = JSON_FACTORY.createParser(json);
        deserializer.deserialize(pageBuilder, jsonParser);
        assertThat(pageBuilder.getPositionCount()).isEqualTo(1);
        assertThat(pageBuilder.getBlockBuilder(5).build().isNull(0)).isTrue();
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

        JsonParser jsonParser = JSON_FACTORY.createParser(json);
        deserializer.deserialize(pageBuilder, jsonParser);
        assertThat(pageBuilder.getPositionCount()).isEqualTo(1);
        assertThat(pageBuilder.getBlockBuilder(4).build().isNull(0)).isFalse();
        assertThat(DATE.getLong(pageBuilder.getBlockBuilder(4).build(), 0)).isEqualTo(2932896);
    }
}
