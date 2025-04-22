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
package io.trino.plugin.hive.crypto;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.crypto.DecryptionKeyRetriever;
import io.trino.parquet.crypto.FileDecryptionProperties;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.FileParquetDataSource;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.ColumnEncryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Files;
import java.util.Map;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * End‑to‑end PME flow: Parquet writer → Hive connector → Trino query.
 */
// ExampleParquetWriter is not thread-safe
@TestInstance(PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class TestHiveParquetEncryption
        extends AbstractTestQueryFramework
{
    private static final byte[] FOOTER_KEY = "footKeyIs16Byte?".getBytes(UTF_8);

    // Keys per column (different on purpose)
    private static final byte[] COLUMN_KEY_AGE = "colKeyIs16ByteA?".getBytes(UTF_8);
    private static final byte[] COLUMN_KEY_ID = "colKeyIs16ByteB?".getBytes(UTF_8);

    private static final ColumnPath AGE_PATH = ColumnPath.fromDotString("age");
    private static final ColumnPath ID_PATH = ColumnPath.fromDotString("id");

    /**
     * kept so we can reference the warehouse path later
     */
    private java.nio.file.Path warehouseDir;

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        warehouseDir = Files.createTempDirectory("pme_hive");

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", warehouseDir.toUri().toString())
                .put("pme.environment-key-retriever.enabled", "false")
                .buildOrThrow();

        // Bind retriever that knows ONLY the footer key + age column key
        return HiveQueryRunner.builder()
                .setHiveProperties(properties)
                .setDecryptionKeyRetriever(new TestingParquetEncryptionModule(FOOTER_KEY, Optional.of(COLUMN_KEY_AGE), Optional.empty()))
                .build();
    }

    @Test
    public void testEncryptedParquetRead()
            throws Exception
    {
        // 1) write encrypted file inside warehouse
        java.nio.file.Path dataDir = Files.createDirectories(warehouseDir.resolve("pme_data"));
        java.nio.file.Path parquetFile = dataDir.resolve("data.parquet");
        writeEncryptedFile(parquetFile);

        // 2) create external table
        String location = Location.of(String.valueOf(dataDir.toUri())).toString();
        assertUpdate("""
                CREATE TABLE enc_age(age INT)
                WITH (external_location = '%s', format = 'PARQUET')
                """.formatted(location));

        // 3) verify results
        MaterializedResult result = computeActual("SELECT COUNT(*), MIN(age), MAX(age) FROM enc_age");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(100L);                    // count
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(0);   // min
        assertThat(result.getMaterializedRows().get(0).getField(2)).isEqualTo(99);  // max
    }

    @Test
    public void testTwoColumnFileOnlyAgeKeyProvided()
            throws Exception
    {
        // 1) write a two-column file (both columns encrypted with different keys)
        java.nio.file.Path dataDir = Files.createDirectories(warehouseDir.resolve("pme_two_cols"));
        java.nio.file.Path parquetFile = dataDir.resolve("data.parquet");
        writeTwoColumnEncryptedFile(parquetFile);

        // 2) create external table with both columns
        String location = Location.of(String.valueOf(dataDir.toUri())).toString();
        assertUpdate("""
                CREATE TABLE enc_two(id INT, age INT)
                WITH (external_location = '%s', format = 'PARQUET')
                """.formatted(location));

        // 3) Selecting ONLY the accessible column (age) must succeed
        MaterializedResult ok = computeActual("SELECT COUNT(*), MIN(age), MAX(age) FROM enc_two");
        assertThat(ok.getMaterializedRows().get(0).getField(0)).isEqualTo(100L);
        assertThat(ok.getMaterializedRows().get(0).getField(1)).isEqualTo(0);
        assertThat(ok.getMaterializedRows().get(0).getField(2)).isEqualTo(99);

        // 4) Selecting the inaccessible column (id) must fail (no column key)
        //    We match on a broad error text to avoid coupling to a specific message.
        assertQueryFails("SELECT MIN(id) FROM enc_two", "(?s).*User does not have access to column.*");
    }

    @Test
    public void testEncryptedDictionaryPruningTwoColumns()
            throws Exception
    {
        // 1) write an encrypted, dictionary-encoded two-column file
        // Values are 0..10 except the “missing” ones; RG min/max is [0,10] so min/max alone can’t prune.
        java.nio.file.Path dataDir = Files.createDirectories(warehouseDir.resolve("pme_dict2_enc"));
        java.nio.file.Path parquetFile = dataDir.resolve("data.parquet");
        int missingAge = 7;
        int missingId = 3;
        writeTwoColumnEncryptedDictionaryFile(parquetFile, missingAge, missingId);

        // 2) create external table
        String location = Location.of(String.valueOf(dataDir.toUri())).toString();
        assertUpdate("""
                CREATE TABLE enc_dict2(id INT, age INT)
                WITH (external_location = '%s', format = 'PARQUET')
                """.formatted(location));

        // 3) Predicate on the accessible column (age) – dictionary (encrypted) gets read & prunes to zero
        assertThat(computeActual("SELECT count(*) FROM enc_dict2 WHERE age = " + missingAge).getOnlyValue())
                .isEqualTo(0L);

        // sanity: present value on age returns rows
        assertThat(((Number) computeActual("SELECT count(*) FROM enc_dict2 WHERE age = 5").getOnlyValue()).longValue())
                .isGreaterThan(0L);

        // 4) Predicate on the inaccessible column (id) – should fail (no column key)
        assertQueryFails("SELECT count(*) FROM enc_dict2 WHERE id = " + missingId, "(?s).*access.*column.*id.*");

        assertQuerySucceeds("DROP TABLE enc_dict2");
    }

    // ───────────────────────── writer ─────────────────────────
    private static void writeEncryptedFile(java.nio.file.Path path)
            throws Exception
    {
        var schema = MessageTypeParser.parseMessageType(
                "message doc { required int32 age; }");

        // This test purposely reuses one demo key.
        // With Parquet Modular Encryption (AES-GCM/CTR), reusing a key for lots of data
        // weakens security.
        ColumnEncryptionProperties columnProperties = ColumnEncryptionProperties.builder(AGE_PATH)
                .withKey(COLUMN_KEY_AGE)
                .build();

        FileEncryptionProperties encodingProperties = FileEncryptionProperties.builder(FOOTER_KEY)
                .withAlgorithm(ParquetCipher.AES_GCM_CTR_V1)
                .withEncryptedColumns(Map.of(AGE_PATH, columnProperties))
                .build();

        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new org.apache.hadoop.fs.Path(path.toString()))
                .withType(schema)
                .withConf(new Configuration())
                .withEncryption(encodingProperties)
                .withWriteMode(OVERWRITE)
                .build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            for (int i = 0; i < 100; i++) {
                writer.write(factory.newGroup().append("age", i));
            }
        }
    }

    private static void writeTwoColumnEncryptedFile(java.nio.file.Path path)
            throws Exception
    {
        var schema = MessageTypeParser.parseMessageType("message doc { required int32 age; required int32 id; }");

        ColumnEncryptionProperties idProperties = ColumnEncryptionProperties.builder(ID_PATH).withKey(COLUMN_KEY_ID).build();
        ColumnEncryptionProperties ageProperties = ColumnEncryptionProperties.builder(AGE_PATH).withKey(COLUMN_KEY_AGE).build();

        FileEncryptionProperties encodingProperties = FileEncryptionProperties.builder(FOOTER_KEY)
                .withAlgorithm(ParquetCipher.AES_GCM_CTR_V1)
                .withEncryptedColumns(Map.of(AGE_PATH, ageProperties, ID_PATH, idProperties))
                .build();

        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new org.apache.hadoop.fs.Path(path.toString()))
                .withType(schema)
                .withConf(new Configuration())
                .withEncryption(encodingProperties)
                .withWriteMode(OVERWRITE)
                .build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            for (int i = 0; i < 100; i++) {
                writer.write(factory.newGroup().append("id", 100 - i).append("age", i));
            }
        }
    }

    /**
     * Two-column writer:
     * - both columns encrypted (different keys),
     * - tiny page size to encourage dictionary encoding,
     * - each column skips one value in 0..10 so dictionary-based pruning can eliminate the row-group.
     * Reader in this test only has the AGE key (ID key is absent).
     */
    private static void writeTwoColumnEncryptedDictionaryFile(java.nio.file.Path path, int missingAge, int missingId)
            throws Exception
    {
        var schema = MessageTypeParser.parseMessageType("message doc { required int32 age; required int32 id; }");

        ColumnEncryptionProperties idProperties = ColumnEncryptionProperties.builder(ID_PATH).withKey(COLUMN_KEY_ID).build();
        ColumnEncryptionProperties ageProperties = ColumnEncryptionProperties.builder(AGE_PATH).withKey(COLUMN_KEY_AGE).build();

        FileEncryptionProperties encodingProperties = FileEncryptionProperties.builder(FOOTER_KEY)
                .withAlgorithm(ParquetCipher.AES_GCM_CTR_V1)
                .withEncryptedColumns(Map.of(AGE_PATH, ageProperties, ID_PATH, idProperties))
                .build();

        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new org.apache.hadoop.fs.Path(path.toString()))
                .withType(schema)
                .withConf(new Configuration())
                .withEncryption(encodingProperties)
                .withWriteMode(OVERWRITE)
                .withPageSize(1024) // small pages -> dictionary likely
                .build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            for (int i = 0; i < 5000; i++) {
                int id = i % 11;
                if (id == missingId) {
                    id = (id + 1) % 11;  // skip one value for id
                }
                int age = i % 11;
                if (age == missingAge) {
                    age = (age + 1) % 11;  // skip one value for age
                }
                writer.write(factory.newGroup().append("id", id).append("age", age));
            }
        }

        // Verify both columns actually have dictionary pages
        try (ParquetDataSource source = new FileParquetDataSource(path.toFile(), ParquetReaderOptions.defaultOptions())) {
            FileDecryptionProperties dec = FileDecryptionProperties.builder()
                    .withKeyRetriever(new TestHiveParquetEncryption.TestingParquetEncryptionModule(
                            FOOTER_KEY, Optional.of(COLUMN_KEY_AGE), Optional.of(COLUMN_KEY_ID)))
                    .build();
            ParquetMetadata metadata = MetadataReader.readFooter(source, Optional.empty(), Optional.empty(), Optional.of(dec));

            ColumnChunkMetadata ageChunk = metadata.getBlocks().getFirst().columns().stream()
                    .filter(column -> column.getPath().equals(AGE_PATH))
                    .findFirst().orElseThrow();
            ColumnChunkMetadata idChunk = metadata.getBlocks().getFirst().columns().stream()
                    .filter(column -> column.getPath().equals(ID_PATH))
                    .findFirst().orElseThrow();

            assertThat(ageChunk.getDictionaryPageOffset()).isGreaterThan(0);
            assertThat(idChunk.getDictionaryPageOffset()).isGreaterThan(0);
            assertThat(ageChunk.getEncodings()).anyMatch(org.apache.parquet.column.Encoding::usesDictionary);
            assertThat(idChunk.getEncodings()).anyMatch(org.apache.parquet.column.Encoding::usesDictionary);
        }
    }

    public static class TestingParquetEncryptionModule
            implements DecryptionKeyRetriever
    {
        private final byte[] footerKey;
        private final Optional<byte[]> ageKey;
        private final Optional<byte[]> idKey;

        public TestingParquetEncryptionModule(byte[] footerKey, Optional<byte[]> ageKey, Optional<byte[]> idKey)
        {
            this.footerKey = requireNonNull(footerKey, "footerKey is null");
            this.ageKey = requireNonNull(ageKey, "ageKey is null");
            this.idKey = requireNonNull(idKey, "idKey is null");
        }

        @Override
        public Optional<byte[]> getColumnKey(ColumnPath columnPath, Optional<byte[]> keyMetadata)
        {
            String path = columnPath.toDotString();
            if ("age".equals(path)) {
                return ageKey;
            }
            if ("id".equals(path)) {
                return idKey;
            }
            return Optional.empty();
        }

        @Override
        public Optional<byte[]> getFooterKey(Optional<byte[]> keyMetadata)
        {
            return Optional.of(footerKey);
        }
    }
}
