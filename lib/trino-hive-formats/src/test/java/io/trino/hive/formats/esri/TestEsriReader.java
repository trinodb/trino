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
import io.trino.spi.PageBuilder;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEsriReader
{
    @TempDir
    Path tempDir;

    private static final List<Column> TEST_COLUMNS = List.of(
            new Column("id", BIGINT, 0),
            new Column("name", VARCHAR, 1),
            new Column("geometry", VARBINARY, 2));

    private static PageBuilder createPageBuilder()
    {
        List<Type> types = TEST_COLUMNS.stream()
                .map(Column::type)
                .collect(toImmutableList());
        return new PageBuilder(types);
    }

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

        PageBuilder pageBuilder = createPageBuilder();
        EsriDeserializer deserializer = new EsriDeserializer(TEST_COLUMNS);

        try (EsriReader reader = new EsriReader(
                new ByteArrayInputStream(json.getBytes(UTF_8)),
                deserializer)) {

            assertThat(reader.next(pageBuilder)).isTrue();
            assertThat(pageBuilder.getPositionCount()).isEqualTo(1);

            assertThat(reader.next(pageBuilder)).isTrue();
            assertThat(pageBuilder.getPositionCount()).isEqualTo(2);

            assertThat(reader.next(pageBuilder)).isFalse();
        }
    }

    @Test
    void testEmptyFeatures() throws IOException {
        String json = """
                {
                    "features": []
                }
                """;

        PageBuilder pageBuilder = createPageBuilder();
        EsriDeserializer deserializer = new EsriDeserializer(TEST_COLUMNS);

        try (EsriReader reader = new EsriReader(
                new ByteArrayInputStream(json.getBytes(UTF_8)),
                deserializer)) {
            assertThat(reader.next(pageBuilder)).isFalse();
        }
    }

    @Test
    void testNoFeaturesArray() throws IOException {
        String json = """
                {
                    "someOtherField": []
                }
                """;

        PageBuilder pageBuilder = createPageBuilder();
        EsriDeserializer deserializer = new EsriDeserializer(TEST_COLUMNS);

        try (EsriReader reader = new EsriReader(
                new ByteArrayInputStream(json.getBytes(UTF_8)),
                deserializer)) {
            assertThat(reader.next(pageBuilder)).isFalse();
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

        PageBuilder pageBuilder = createPageBuilder();
        EsriDeserializer deserializer = new EsriDeserializer(TEST_COLUMNS);

        try (EsriReader reader = new EsriReader(
                new ByteArrayInputStream(json.getBytes(UTF_8)),
                deserializer)) {
            reader.next(pageBuilder);
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

        PageBuilder pageBuilder = createPageBuilder();
        EsriDeserializer deserializer = new EsriDeserializer(TEST_COLUMNS);

        try (EsriReader reader = new EsriReader(
                new ByteArrayInputStream(json.getBytes(UTF_8)),
                deserializer)) {
            reader.next(pageBuilder);
            assertThat(reader.getReadTimeNanos()).isPositive();
        }
    }

    @Test
    void testLargeFile() throws IOException {
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

        PageBuilder pageBuilder = createPageBuilder();
        EsriDeserializer deserializer = new EsriDeserializer(TEST_COLUMNS);

        try (EsriReader reader = new EsriReader(
                Files.newInputStream(jsonFile),
                deserializer)) {
            int count = 0;
            while (reader.next(pageBuilder)) {
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

        EsriDeserializer deserializer = new EsriDeserializer(TEST_COLUMNS);

        EsriReader reader = new EsriReader(
                new ByteArrayInputStream(json.getBytes(UTF_8)),
                deserializer);

        assertThat(reader.isClosed()).isFalse();
        reader.close();
        assertThat(reader.isClosed()).isTrue();
    }
}
