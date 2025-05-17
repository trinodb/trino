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
    private static final byte[] COLUMN_KEY = "colKeyIs16ByteA?".getBytes(UTF_8);
    private static final ColumnPath AGE_PATH = ColumnPath.fromDotString("age");

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
                // disable env retriever; we inject keys via custom module
                .put("pme.environment-key-retriever.enabled", "false")
                .buildOrThrow();

        return HiveQueryRunner.builder()
                .setHiveProperties(properties)
                .setHiveModule(new TestingParquetEncryptionModule(FOOTER_KEY, COLUMN_KEY))
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

    // ───────────────────────── writer ─────────────────────────
    private static void writeEncryptedFile(java.nio.file.Path path)
            throws Exception
    {
        var schema = MessageTypeParser.parseMessageType(
                "message doc { required int32 age; }");

        /*
         *  ─────────────────────────  IMPORTANT  ─────────────────────────
         *
         *  **This unit-test reuses one synthetic 128-bit key for every file.**
         *
         *  That is fine for *tests*, but **DO NOT** follow this pattern in
         *  production: AES-GCM/CTR has a hard upper-bound (~2³² blocks) on the
         *  number of encryption operations that may be performed with the same
         *  key/non-repeating-nonce combination. Exceeding that limit
         *  catastrophically weakens the cipher.
         *
         *  In real deployments you should:
         *    • Generate a fresh data-encryption-key (DEK) per file *or* per
         *      small batch of files, **or**
         *    • Use different key-ids (KEKs) and supply a map
         *      <key-id → DEK-bytes> to the reader.
         *
         *  See “Apache Parquet Modular Encryption: security considerations”
         *  for details.
         *  ────────────────────────────────────────────────────────────────
         */
        ColumnEncryptionProperties columnProperties = ColumnEncryptionProperties.builder(AGE_PATH)
                .withKey(COLUMN_KEY)
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

    /**
     * Test‑only module: always returns the given footer / column key.
     */
    public static class TestingParquetEncryptionModule
            implements Module, DecryptionKeyRetriever
    {
        private final byte[] footerKey;
        private final byte[] columnKey;

        public TestingParquetEncryptionModule(byte[] footerKey, byte[] columnKey)
        {
            this.footerKey = requireNonNull(footerKey, "footerKey is null");
            this.columnKey = requireNonNull(columnKey, "columnKey is null");
        }

        @Override
        public void configure(Binder binder)
        {
            Multibinder<DecryptionKeyRetriever> retrieverBinder =
                    Multibinder.newSetBinder(binder, DecryptionKeyRetriever.class);
            retrieverBinder.addBinding().toInstance(new TestingParquetEncryptionModule(footerKey, columnKey));
        }

        @Override
        public Optional<byte[]> getColumnKey(ColumnPath columnPath, Optional<byte[]> keyMetadata)
        {
            return Optional.of(columnKey);
        }

        @Override
        public Optional<byte[]> getFooterKey(Optional<byte[]> keyMetadata)
        {
            return Optional.of(footerKey);
        }
    }
}
