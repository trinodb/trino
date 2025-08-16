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

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestEsriReader
{
    private static final List<Column> TEST_COLUMNS = List.of(
            new Column("id", BIGINT, 0),
            new Column("name", VARCHAR, 1),
            new Column("geometry", VARBINARY, 2));

    @Test
    public void testReadSimpleFeatures()
            throws IOException
    {
        String json = """
                {
                    "features": [
                        {
                            "attributes": {
                                "id": 1,
                                "name": "Feature 1"
                            },
                            "geometry": {
                                "x": 10,
                                "y": 20
                            }
                        },
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
                    ]
                }
                """;

        Page page = readAll(json);
        assertThat(page.getPositionCount()).isEqualTo(2);
    }

    @Test
    public void testEmptyFeatures()
            throws IOException
    {
        String json = """
                {
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
        String json = """
                {
                    "features": null
                }
                """;

        Page page = readAll(json);
        assertThat(page.getPositionCount()).isZero();
    }

    @Test
    public void testNumberFeaturesFails()
    {
        String json = """
                {
                    "features": 42
                }
                """;

        assertThatThrownBy(() -> new EsriReader(new ByteArrayInputStream(json.getBytes(UTF_8)), new EsriDeserializer(TEST_COLUMNS)))
                .isInstanceOf(IOException.class)
                .hasMessage("Invalid JSON: Features field must be an array");
    }

    @Test
    public void testObjectFeaturesFails()
    {
        String json = """
                {
                    "features": {}
                }
                """;

        assertThatThrownBy(() -> new EsriReader(new ByteArrayInputStream(json.getBytes(UTF_8)), new EsriDeserializer(TEST_COLUMNS)))
                .isInstanceOf(IOException.class)
                .hasMessage("Invalid JSON: Features field must be an array");
    }

    @Test
    public void testNoFeaturesArray()
            throws IOException
    {
        String json = """
                {
                    "someOtherField": []
                }
                """;

        Page page = readAll(json);
        assertThat(page.getPositionCount()).isZero();
    }

    @Test
    public void testLargeRead()
            throws IOException
    {
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{\"features\":[");
        for (int i = 0; i < 1000; i++) {
            if (i > 0) {
                jsonBuilder.append(",");
            }
            jsonBuilder.append(String.format(
                    """
                            {
                                "attributes": {
                                    "id": %d,
                                    "name": "Feature %d"
                                },
                                "geometry": {
                                    "x": %d,
                                    "y": %d
                                }
                            }
                            """, i, i, i * 10, i * 20));
        }
        jsonBuilder.append("]}");

        EsriDeserializer deserializer = new EsriDeserializer(TEST_COLUMNS);
        PageBuilder pageBuilder = new PageBuilder(deserializer.getTypes());

        try (EsriReader reader = new EsriReader(new ByteArrayInputStream(jsonBuilder.toString().getBytes(UTF_8)), deserializer)) {
            int count = 0;
            while (reader.next(pageBuilder)) {
                count++;
            }
            assertThat(count).isEqualTo(1000);
        }
    }

    @Test
    public void testTruncatedFeaturesAllowed()
            throws IOException
    {
        String json = """
                {
                    "features": [
                        {
                            "attributes": {
                                "id": 1
                            }
                        }
                    ]
                    EVERYTHING AFTER ARRAY CLOSE IS IGNORED
                """;

        Page page = readAll(json);
        assertThat(page.getPositionCount()).isEqualTo(1);
    }

    @Test
    public void testDuplicateFeaturesIgnored()
            throws IOException
    {
        String json = """
                {
                    "features": [
                        {
                            "attributes": {
                                "id": 1
                            }
                        }
                    ],
                    "features": [
                        {
                            "attributes": {
                                "id": 2
                            }
                        }
                    ]
                }
                """;

        Page page = readAll(json);
        assertThat(page.getPositionCount()).isEqualTo(1);
    }

    @Test
    public void testNestedFeaturesIgnored()
            throws IOException
    {
        String json = """
                {
                    "bad": {
                        "features": [
                            {
                                "attributes": {
                                    "id": 1
                                }
                            }
                        ]
                    },
                    "features": [
                        {
                            "attributes": {
                                "id": 77
                            }
                        }
                    ]
                }
                """;

        Page page = readAll(json);
        assertThat(page.getPositionCount()).isEqualTo(1);
        assertThat(BIGINT.getLong(page.getBlock(0), 0)).isEqualTo(77);
    }

    private static Page readAll(String json)
            throws IOException
    {
        EsriDeserializer deserializer = new EsriDeserializer(TEST_COLUMNS);
        EsriReader reader = new EsriReader(new ByteArrayInputStream(json.getBytes(UTF_8)), deserializer);
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
