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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEsriReader
{
    @TempDir
    Path tempDir;

    @Test
    void testReadSimpleFeatures()
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

        try (EsriReader reader = new EsriReader(new ByteArrayInputStream(json.getBytes(UTF_8)))) {
            String feature1 = reader.next();
            assertThat(feature1)
                    .isNotNull()
                    .contains("\"id\":1")
                    .contains("\"name\":\"Feature 1\"")
                    .contains("\"x\":10")
                    .contains("\"y\":20");

            String feature2 = reader.next();
            assertThat(feature2)
                    .isNotNull()
                    .contains("\"id\":2")
                    .contains("\"name\":\"Feature 2\"")
                    .contains("\"x\":30")
                    .contains("\"y\":40");

            assertThat(reader.next()).isNull();
        }
    }

    @Test
    void testEmptyFeatures() throws IOException {
        String json = """
                {
                    "features": []
                }
                """;

        try (EsriReader reader = new EsriReader(new ByteArrayInputStream(json.getBytes(UTF_8)))) {
            assertThat(reader.next()).isNull();
        }
    }

    @Test
    void testNoFeaturesArray() throws IOException {
        String json = """
                {
                    "someOtherField": []
                }
                """;

        try (EsriReader reader = new EsriReader(new ByteArrayInputStream(json.getBytes(UTF_8)))) {
            assertThat(reader.next()).isNull();
        }
    }

    @Test
    void testBytesReadCounter() throws IOException {
        String json = """
                {
                    "features": [
                        {
                            "attributes": {
                                "id": 1
                            }
                        }
                    ]
                }
                """;

        try (EsriReader reader = new EsriReader(new ByteArrayInputStream(json.getBytes(UTF_8)))) {
            reader.next();
            assertThat(reader.getBytesRead()).isPositive();
        }
    }

    @Test
    void testReadTimeNanos() throws IOException {
        String json = """
                {
                    "features": [
                        {
                            "attributes": {
                                "id": 1
                            }
                        }
                    ]
                }
                """;

        try (EsriReader reader = new EsriReader(new ByteArrayInputStream(json.getBytes(UTF_8)))) {
            reader.next();
            assertThat(reader.getReadTimeNanos()).isPositive();
        }
    }

    @Test
    void testLargeFile(@TempDir Path tempDir) throws IOException {
        Path jsonFile = tempDir.resolve("large.json");
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{\"features\":[");
        for (int i = 0; i < 1000; i++) {
            if (i > 0) {
                jsonBuilder.append(",");
            }
            jsonBuilder.append(String.format("""
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
        Files.writeString(jsonFile, jsonBuilder.toString());

        try (EsriReader reader = new EsriReader(Files.newInputStream(jsonFile))) {
            int count = 0;
            while (reader.next() != null) {
                count++;
            }
            assertThat(count).isEqualTo(1000);
        }
    }

    @Test
    void testClosedStatus() throws IOException {
        String json = """
                {
                    "features": []
                }
                """;

        EsriReader reader = new EsriReader(new ByteArrayInputStream(json.getBytes(UTF_8)));
        assertThat(reader.isClosed()).isFalse();

        reader.close();
        assertThat(reader.isClosed()).isTrue();
    }
}
