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
package io.trino.plugin.iceberg.cache;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergStaleManifestLength
{
    private static final String SCHEMA = "stale_len";

    /**
     * A manifest list may record a {@code manifest_length} smaller than the manifest's actual
     * size on storage when the manifest list was written by a buggy writer, such as iceberg-go
     * before <a href="https://github.com/apache/iceberg-go/pull/439">apache/iceberg-go#439</a>
     * (see <a href="https://github.com/apache/iceberg-go/issues/438">apache/iceberg-go#438</a>).
     * Such metadata is permanent until the manifests are rewritten, so reads of the table must
     * not be corrupted by it: trino itself always records exact lengths (asserted first), and
     * a table with undersized recorded lengths must return the same correct rows with the
     * metadata cache enabled (the default) as without it.
     */
    @Test
    void testUndersizedManifestLengthFromForeignWriter()
            throws Exception
    {
        Path metastoreDirectory = Files.createTempDirectory(SCHEMA);
        try {
            try (QueryRunner writer = queryRunner(metastoreDirectory, true)) {
                writer.execute("CREATE TABLE %s.t AS SELECT * FROM (VALUES 1, 2) AS v(x)".formatted(SCHEMA));
                writer.execute("INSERT INTO %s.t VALUES 3".formatted(SCHEMA));

                // Trino records the exact manifest sizes, so tables written by trino are not
                // affected: only foreign writers introduce undersized lengths
                for (MaterializedRow manifest : writer.execute("SELECT path, length FROM %s.\"t$manifests\"".formatted(SCHEMA)).getMaterializedRows()) {
                    Path manifestFile = Paths.get(URI.create((String) manifest.getField(0)));
                    assertThat((long) manifest.getField(1))
                            .as("recorded manifest_length of %s", manifestFile)
                            .isEqualTo(Files.size(manifestFile));
                }
            }

            // Rewrite the manifest lists the way a buggy foreign writer would have written
            // them: same manifests, but with a recorded length smaller than the actual size
            try (Stream<Path> files = Files.walk(metastoreDirectory)) {
                List<Path> manifestLists = files.filter(file -> file.getFileName().toString().startsWith("snap-")).toList();
                assertThat(manifestLists).isNotEmpty();
                for (Path manifestList : manifestLists) {
                    shrinkRecordedManifestLengths(manifestList, 40);
                }
            }

            // A fresh coordinator with default configuration, metadata caching included, must
            // read all rows: the recorded length must not truncate what is actually read
            try (QueryRunner reader = queryRunner(metastoreDirectory, true)) {
                assertThat(reader.execute("SELECT x FROM %s.t ORDER BY x".formatted(SCHEMA)).getOnlyColumnAsSet())
                        .containsExactlyInAnyOrder(1, 2, 3);
            }

            // Reference behavior with the metadata cache disabled
            try (QueryRunner reader = queryRunner(metastoreDirectory, false)) {
                assertThat(reader.execute("SELECT x FROM %s.t ORDER BY x".formatted(SCHEMA)).getOnlyColumnAsSet())
                        .containsExactlyInAnyOrder(1, 2, 3);
            }
        }
        finally {
            deleteRecursively(metastoreDirectory, ALLOW_INSECURE);
        }
    }

    private static QueryRunner queryRunner(Path metastoreDirectory, boolean metadataCacheEnabled)
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.<String, String>builder()
                        .put("iceberg.metadata-cache.enabled", String.valueOf(metadataCacheEnabled))
                        .put("hive.metastore.catalog.dir", metastoreDirectory.toUri().toString())
                        .put("fs.hadoop.enabled", "true")
                        .buildOrThrow())
                .setWorkerCount(0)
                .build();
        queryRunner.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
        return queryRunner;
    }

    private static void shrinkRecordedManifestLengths(Path manifestList, long shrinkBy)
            throws IOException
    {
        Schema schema;
        Map<String, byte[]> metadata = new LinkedHashMap<>();
        List<GenericRecord> entries = new ArrayList<>();
        try (DataFileReader<GenericRecord> reader = new DataFileReader<>(new SeekableFileInput(manifestList.toFile()), new GenericDatumReader<>())) {
            schema = reader.getSchema();
            for (String key : reader.getMetaKeys()) {
                if (!key.startsWith("avro.")) {
                    metadata.put(key, reader.getMeta(key));
                }
            }
            reader.forEach(entries::add);
        }
        assertThat(entries).isNotEmpty();

        ByteArrayOutputStream rewritten = new ByteArrayOutputStream();
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(schema))) {
            metadata.forEach(writer::setMeta);
            writer.create(schema, rewritten);
            for (GenericRecord entry : entries) {
                entry.put("manifest_length", (long) entry.get("manifest_length") - shrinkBy);
                writer.append(entry);
            }
        }
        Files.write(manifestList, rewritten.toByteArray());
        // The hadoop local file system keeps a checksum sidecar that no longer matches the
        // rewritten content
        Files.deleteIfExists(manifestList.resolveSibling("." + manifestList.getFileName() + ".crc"));
    }
}
