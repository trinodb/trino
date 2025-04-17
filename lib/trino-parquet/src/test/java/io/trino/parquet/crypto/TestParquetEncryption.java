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
package io.trino.parquet.crypto;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.FileParquetDataSource;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.IntegerType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.crypto.ColumnEncryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit‑tests exercising Trino’s PME path.
 */
public class TestParquetEncryption
{
    // ──────────────────────────────────  CONSTANTS  ────────────────────────────────────
    private static final ColumnPath AGE_PATH = ColumnPath.fromDotString("age");
    private static final ColumnPath ID_PATH = ColumnPath.fromDotString("id");

    private static final byte[] KEY_AGE = "colKeyIs16ByteA?".getBytes(UTF_8);
    private static final byte[] KEY_ID = "colKeyIs16ByteB?".getBytes(UTF_8);
    private static final byte[] KEY_FOOT = "footKeyIs16Byte?".getBytes(UTF_8);

    // one‑column schema
    private static final MessageType AGE_SCHEMA =
            MessageTypeParser.parseMessageType("message doc { required int32 age; }");

    // two‑column schema
    private static final MessageType TWO_COL_SCHEMA = MessageTypeParser.parseMessageType(
            "message doc { required int32 id; required int32 age; }");

    // ────────────────────────────────  PARAMETERISED TESTS  ─────────────────────────────

    /**
     * Column encryption only – footer left in plaintext.
     */
    @ParameterizedTest(name = "colOnly_footerIntegrity={0}_compressed={1}")
    @CsvSource({
            "false,false",
            "false,true",
            "true,false",
            "true,true"
    })
    void colOnlyFooterPlaintext(boolean checkFooterIntegrity, boolean compressed)
            throws IOException
    {
        File file = Files.createTempFile("pme‑col‑only", ".parquet").toFile();
        file.deleteOnExit();

        writeSingleColumnFile(file, compressed, /*encryptFooter*/ false);

        List<Integer> values = readSingleColumnFile(
                file,
                new TestingKeyRetriever(checkFooterIntegrity ? Optional.of(KEY_FOOT) : Optional.empty(), Optional.of(KEY_AGE), Optional.empty()),
                checkFooterIntegrity);

        verifySequence(values, 100);
    }

    /**
     * Column+ footer encryption (same column as above).
     */
    @ParameterizedTest(name = "col+footer_compressed={0}")
    @CsvSource({"false", "true"})
    void colAndFooter(boolean compressed)
            throws IOException
    {
        File file = Files.createTempFile("pme‑col‑foot", ".parquet").toFile();
        file.deleteOnExit();

        writeSingleColumnFile(file, compressed, /*encryptFooter*/ true);

        List<Integer> values = readSingleColumnFile(
                file,
                new TestingKeyRetriever(Optional.of(KEY_FOOT), Optional.of(KEY_AGE), Optional.empty()),
                /*checkFooterIntegrity*/ true);

        verifySequence(values, 100);
    }

    // ───────────────────────────────────  EXTRA CASES  ──────────────────────────────────

    /**
     * Two columns; footer encrypted; one column plaintext, other encrypted.
     */
    @Test
    void mixedEncryptedAndPlaintextColumns()
            throws IOException
    {
        File file = Files.createTempFile("pme‑mixed‑cols", ".parquet").toFile();
        file.deleteOnExit();

        // age column encrypted, id column plaintext
        writeTwoColumnFile(file, /*encryptAge*/ true, /*encryptId*/ false);

        // Provide keys for both footer & encrypted age column
        Map<String, List<Integer>> data = readTwoColumnFile(file, new TestingKeyRetriever(Optional.of(KEY_FOOT), Optional.of(KEY_AGE), Optional.empty()));

        verifySequence(data.get("age"), 100);
        verifyReverseSequence(data.get("id"), 100);
    }

    /**
     * Two columns; different keys; refuse access to one column.
     */
    @Test
    void oneColumnAccessibleOtherInaccessible()
            throws IOException
    {
        File file = Files.createTempFile("pme‑locked‑col", ".parquet").toFile();
        file.deleteOnExit();

        // both columns encrypted with different keys
        writeTwoColumnFile(file, /*encryptAge*/ true, /*encryptId*/ true);

        // reader has footer key + KEY_AGE only, not KEY_ID
        TestingKeyRetriever retriever = new TestingKeyRetriever(Optional.of(KEY_FOOT), Optional.of(KEY_AGE), Optional.empty());

        List<Integer> values = readSingleColumnFile(file, retriever, true);

        verifySequence(values, 100);
    }

    /**
     * Single column encrypted, written with dictionary encoding.
     */
    @Test
    void dictionaryEncodedEncryptedColumn()
            throws IOException
    {
        File file = Files.createTempFile("pme‑dict", ".parquet").toFile();
        file.deleteOnExit();

        // create a very small domain so writer chooses dictionary
        writeSingleColumnFile(file, /*compressed*/ false, /*encryptFooter*/ true, /*columnEncryptedWithFooterKey*/ false,
                () -> List.of(1, 2, 3, 1, 2, 3));   // limited distincts

        List<Integer> values = readSingleColumnFile(
                file,
                new TestingKeyRetriever(Optional.of(KEY_FOOT), Optional.of(KEY_AGE), Optional.empty()),
                true);

        assertThat(new HashSet<>(values)).containsExactlyInAnyOrder(1, 2, 3);

        // ── extra assertion: verify a dictionary really exists ──
        try (ParquetDataSource source = new FileParquetDataSource(file, new ParquetReaderOptions())) {
            FileDecryptionProperties properties = FileDecryptionProperties.builder()
                    .withKeyRetriever(new TestingKeyRetriever(Optional.of(KEY_FOOT), Optional.of(KEY_AGE), Optional.empty()))
                    .build();

            ParquetMetadata metadata = MetadataReader.readFooter(source, Optional.empty(), Optional.of(properties));

            // first (and only) row‑group → column‑chunk for "age"
            ColumnChunkMetadata chunk =
                    metadata.getBlocks().getFirst().columns().stream()
                            .filter(column -> column.getPath().equals(AGE_PATH))
                            .findFirst()
                            .orElseThrow();

            // 1) dictionary page must be present
            assertThat(chunk.getDictionaryPageOffset()).isGreaterThan(0);

            // 2) PLAIN_DICTIONARY (or RLE_DICTIONARY in v2) must be in the encoding list
            assertThat(chunk.getEncodings()).anyMatch(Encoding::usesDictionary);

            // 3) (optional) Trino helper: all data pages are dictionary‑encoded
            assertThat(io.trino.parquet.ParquetReaderUtils.isOnlyDictionaryEncodingPages(chunk)).isTrue();
        }
    }

    /**
     * Single column encrypted with footer column.
     */
    @ParameterizedTest(name = "columnEncryptedWithFooterKey+footer_encrypted={0}")
    @CsvSource({"false", "true"})
    void columnEncryptedWithFooterKey(boolean encryptFooter)
            throws IOException
    {
        File file = Files.createTempFile("pme‑footerKey‑col", ".parquet").toFile();
        file.deleteOnExit();

        // write: column encrypted with footer key, footer encrypted as usual
        writeSingleColumnFile(file, /*compressed*/ false, encryptFooter, /*columnEncryptedWithFooterKey*/ true, () -> range(0, 100));

        // read: supply ONLY the footer key (no column key necessary)
        List<Integer> values = readSingleColumnFile(
                file,
                new TestingKeyRetriever(Optional.of(KEY_FOOT), Optional.empty(), Optional.empty()),
                /*checkFooterIntegrity*/ true);

        verifySequence(values, 100);   // data round‑trip OK
    }

    // ─────────────────────────────────  WRITER HELPERS  ─────────────────────────────────

    /**
     * Single‑column writer with knobs.
     */
    private static void writeSingleColumnFile(
            File target,
            boolean compressed,
            boolean encryptFooter)
            throws IOException
    {
        writeSingleColumnFile(target, compressed, encryptFooter, false, () -> range(0, 100));
    }

    /**
     * Overload that takes explicit values supplier (for dictionary case).
     */
    private static void writeSingleColumnFile(
            File target,
            boolean compressed,
            boolean encryptFooter,
            boolean columnEncryptedWithFooterKey,
            Supplier<List<Integer>> valuesSupplier)
            throws IOException
    {
        ColumnEncryptionProperties.Builder ageEncryptionBuilder = ColumnEncryptionProperties.builder(AGE_PATH);
        if (!columnEncryptedWithFooterKey) {
            ageEncryptionBuilder.withKey(KEY_AGE);
        }
        ColumnEncryptionProperties ageEncryption = ageEncryptionBuilder.build();

        FileEncryptionProperties.Builder fileEncryption = FileEncryptionProperties.builder(KEY_FOOT)
                .withAlgorithm(ParquetCipher.AES_GCM_CTR_V1)
                .withEncryptedColumns(ImmutableMap.of(AGE_PATH, ageEncryption));

        if (!encryptFooter) {
            fileEncryption.withPlaintextFooter();
        }

        ExampleParquetWriter.Builder builder = ExampleParquetWriter
                .builder(new Path(target.getAbsolutePath()))
                .withType(AGE_SCHEMA)
                .withConf(new Configuration())
                .withEncryption(fileEncryption.build())
                .withWriteMode(OVERWRITE)
                .withCompressionCodec(compressed ? SNAPPY : UNCOMPRESSED);

        try (ParquetWriter<Group> writer = builder.build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(AGE_SCHEMA);
            for (int value : valuesSupplier.get()) {
                writer.write(factory.newGroup().append("age", value));
            }
        }
    }

    /**
     * Two‑column writer.
     */
    private static void writeTwoColumnFile(
            File target,
            boolean encryptAge,
            boolean encryptId)
            throws IOException
    {
        ImmutableMap.Builder<ColumnPath, ColumnEncryptionProperties> columnMap = ImmutableMap.builder();
        if (encryptAge) {
            columnMap.put(AGE_PATH, ColumnEncryptionProperties.builder(AGE_PATH).withKey(KEY_AGE).build());
        }
        if (encryptId) {
            columnMap.put(ID_PATH, ColumnEncryptionProperties.builder(ID_PATH).withKey(KEY_ID).build());
        }

        FileEncryptionProperties fileEncryption = FileEncryptionProperties.builder(KEY_FOOT)
                .withAlgorithm(ParquetCipher.AES_GCM_CTR_V1)
                .withEncryptedColumns(columnMap.buildOrThrow())
                .build();

        ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(new Path(target.getAbsolutePath()))
                .withType(TWO_COL_SCHEMA)
                .withConf(new Configuration())
                .withEncryption(fileEncryption)
                .withWriteMode(OVERWRITE);

        try (ParquetWriter<Group> writer = builder.build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(TWO_COL_SCHEMA);
            for (int i = 0; i < 100; i++) {
                writer.write(factory.newGroup().append("id", 100 - i).append("age", i));
            }
        }
    }

    // ────────────────────────────────  READER HELPERS  ──────────────────────────────────

    /**
     * Reads the single‑column file and returns the “age” values.
     */
    private static List<Integer> readSingleColumnFile(
            File file,
            DecryptionKeyRetriever keyRetriever,
            boolean checkFooterIntegrity)
            throws IOException
    {
        ParquetDataSource source = new FileParquetDataSource(file, new ParquetReaderOptions());

        FileDecryptionProperties properties = FileDecryptionProperties
                .builder()
                .withKeyRetriever(keyRetriever)
                .withCheckFooterIntegrity(checkFooterIntegrity)
                .build();

        ParquetMetadata metadata = MetadataReader.readFooter(
                source, Optional.empty(), Optional.of(properties));

        ColumnDescriptor descriptor = new ColumnDescriptor(
                new String[] {"age"},
                Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("age"),
                0, 0);

        Map<List<String>, ColumnDescriptor> byPath = ImmutableMap.of(
                ImmutableList.of("age"), descriptor);

        TupleDomain<ColumnDescriptor> domain = TupleDomain.all();
        TupleDomainParquetPredicate predicate = new TupleDomainParquetPredicate(
                domain, ImmutableList.of(descriptor), DateTimeZone.UTC);

        List<RowGroupInfo> groups = getFilteredRowGroups(
                0, source.getEstimatedSize(),
                source, metadata,
                List.of(domain), List.of(predicate),
                byPath, DateTimeZone.UTC, 200, new ParquetReaderOptions());

        PrimitiveField field = new PrimitiveField(IntegerType.INTEGER, true, descriptor, 0);
        io.trino.parquet.Column column = new io.trino.parquet.Column("age", field);

        try (ParquetReader reader = new ParquetReader(
                Optional.ofNullable(metadata.getFileMetaData().getCreatedBy()),
                List.of(column),
                false,
                groups,
                source,
                DateTimeZone.UTC,
                newSimpleAggregatedMemoryContext(),
                new ParquetReaderOptions(),
                RuntimeException::new,
                Optional.of(predicate),
                Optional.empty(),
                metadata.getDecryptionContext())) {
            List<Integer> out = new ArrayList<>();
            SourcePage page;
            while ((page = reader.nextPage()) != null) {
                Block block = page.getBlock(0);
                IntArrayBlock ints = (IntArrayBlock) block.getUnderlyingValueBlock();
                for (int i = 0; i < ints.getPositionCount(); i++) {
                    out.add(block.isNull(i) ? null : ints.getInt(i));
                }
            }
            return out;
        }
        finally {
            source.close();
        }
    }

    /**
     * Reads both columns and returns a map “age” → values, “id → values.
     */
    private static Map<String, List<Integer>> readTwoColumnFile(
            File file, DecryptionKeyRetriever retriever)
            throws IOException
    {
        ParquetDataSource source = new FileParquetDataSource(file, new ParquetReaderOptions());

        FileDecryptionProperties properties = FileDecryptionProperties
                .builder().withKeyRetriever(retriever).withCheckFooterIntegrity(true).build();

        ParquetMetadata metadata = MetadataReader.readFooter(source, Optional.empty(), Optional.of(properties));

        ColumnDescriptor ageDescriptor = new ColumnDescriptor(
                new String[] {"age"},
                Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("age"), 0, 0);

        ColumnDescriptor idDescriptor = new ColumnDescriptor(
                new String[] {"id"},
                Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("id"), 0, 0);

        Map<List<String>, ColumnDescriptor> byPath = ImmutableMap.of(
                ImmutableList.of("age"), ageDescriptor,
                ImmutableList.of("id"), idDescriptor);

        TupleDomainParquetPredicate predicate = new TupleDomainParquetPredicate(
                TupleDomain.all(), ImmutableList.of(ageDescriptor, idDescriptor), DateTimeZone.UTC);

        List<RowGroupInfo> groups = getFilteredRowGroups(
                0, source.getEstimatedSize(), source, metadata,
                List.of(TupleDomain.all()), List.of(predicate),
                byPath, DateTimeZone.UTC, 200, new ParquetReaderOptions());

        PrimitiveField ageField = new PrimitiveField(IntegerType.INTEGER, true, ageDescriptor, 0);
        PrimitiveField idField = new PrimitiveField(IntegerType.INTEGER, true, idDescriptor, 1);

        List<io.trino.parquet.Column> columns = List.of(
                new io.trino.parquet.Column("age", ageField),
                new io.trino.parquet.Column("id", idField));

        try (ParquetReader reader = new ParquetReader(
                Optional.ofNullable(metadata.getFileMetaData().getCreatedBy()),
                columns,
                false, groups, source,
                DateTimeZone.UTC,
                newSimpleAggregatedMemoryContext(),
                new ParquetReaderOptions(),
                RuntimeException::new,
                Optional.of(predicate),
                Optional.empty(),
                metadata.getDecryptionContext())) {
            List<Integer> ages = new ArrayList<>();
            List<Integer> ids = new ArrayList<>();

            SourcePage page;
            while ((page = reader.nextPage()) != null) {
                for (int column = 0; column < 2; column++) {
                    Block block = page.getBlock(column);
                    IntArrayBlock ints = (IntArrayBlock) block.getUnderlyingValueBlock();
                    List<Integer> values = (column == 0) ? ages : ids;
                    for (int i = 0; i < ints.getPositionCount(); i++) {
                        values.add(block.isNull(i) ? null : ints.getInt(i));
                    }
                }
            }
            return Map.of("age", ages, "id", ids);
        }
        finally {
            source.close();
        }
    }

    // ───────────────────────────────────  UTILITIES  ───────────────────────────────────

    private static void verifySequence(List<Integer> actual, int n)
    {
        assertThat(actual).hasSize(n);
        for (int i = 0; i < n; i++) {
            assertThat(actual.get(i)).isEqualTo(i);
        }
    }

    private static void verifyReverseSequence(List<Integer> actual, int n)
    {
        assertThat(actual).hasSize(n);
        for (int i = 0; i < n; i++) {
            assertThat(actual.get(i)).isEqualTo(100 - i);
        }
    }

    private static List<Integer> range(int fromInclusive, int toExclusive)
    {
        List<Integer> out = new ArrayList<>(toExclusive - fromInclusive);
        for (int i = fromInclusive; i < toExclusive; i++) {
            out.add(i);
        }
        return out;
    }

    // small functional interface to feed custom value list
    @FunctionalInterface
    private interface Supplier<T>
    {
        T get();
    }

    // ──────────────────────────  SIMPLE KEY‑RETRIEVER IMPL  ─────────────────────────────

    public static final class TestingKeyRetriever
            implements DecryptionKeyRetriever
    {
        private final Optional<byte[]> footerKey;
        private final Optional<byte[]> ageColumnKey;
        private final Optional<byte[]> idColumnKey;

        public TestingKeyRetriever(Optional<byte[]> footerKey, Optional<byte[]> ageColumnKey, Optional<byte[]> idColumnKey)
        {
            this.footerKey = footerKey;
            this.ageColumnKey = ageColumnKey;
            this.idColumnKey = idColumnKey;
        }

        @Override
        public Optional<byte[]> getColumnKey(ColumnPath path, Optional<byte[]> keyMetadata)
        {
            if ("age".equals(path.toDotString())) {
                return ageColumnKey;
            }
            if ("id".equals(path.toDotString())) {
                return idColumnKey;
            }
            return Optional.empty();
        }

        @Override
        public Optional<byte[]> getFooterKey(Optional<byte[]> keyMetadata)
        {
            return footerKey;
        }
    }
}
