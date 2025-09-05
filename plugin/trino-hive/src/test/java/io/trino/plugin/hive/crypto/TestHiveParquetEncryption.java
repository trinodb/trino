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
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import io.trino.filesystem.Location;
import io.trino.parquet.crypto.DecryptionKeyRetriever;
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

import java.nio.file.Files;
import java.util.Map;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End‑to‑end PME flow: Parquet writer → Hive connector → Trino query.
 */
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
                .setHiveModule(new TestingParquetEncryptionModule(FOOTER_KEY, Optional.of(COLUMN_KEY_AGE), Optional.empty()))
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
    public void testTwoColumnFileOnlyAgeKeyProvided() throws Exception
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
        var schema = MessageTypeParser.parseMessageType("message doc { required int32 id; required int32 age; }");

        ColumnEncryptionProperties idProps = ColumnEncryptionProperties.builder(ID_PATH).withKey(COLUMN_KEY_ID).build();
        ColumnEncryptionProperties ageProps = ColumnEncryptionProperties.builder(AGE_PATH).withKey(COLUMN_KEY_AGE).build();

        FileEncryptionProperties fileEnc = FileEncryptionProperties.builder(FOOTER_KEY)
                .withAlgorithm(ParquetCipher.AES_GCM_CTR_V1)
                .withEncryptedColumns(Map.of(AGE_PATH, ageProps, ID_PATH, idProps))
                .build();

        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new org.apache.hadoop.fs.Path(path.toString()))
                .withType(schema)
                .withConf(new Configuration())
                .withEncryption(fileEnc)
                .withWriteMode(OVERWRITE)
                .build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            for (int i = 0; i < 100; i++) {
                writer.write(factory.newGroup().append("id", i).append("age", i));
            }
        }
    }

    public static class TestingParquetEncryptionModule
            implements Module, DecryptionKeyRetriever
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
        public void configure(Binder binder)
        {
            Multibinder<DecryptionKeyRetriever> retrieverBinder =
                    Multibinder.newSetBinder(binder, DecryptionKeyRetriever.class);
            retrieverBinder.addBinding().toInstance(this);
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
