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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Supplier;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempFile;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.joda.time.DateTimeZone.UTC;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Unit‑tests exercising Trino’s PME path.
 */
// ExampleParquetWriter is not thread-safe
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public final class TestParquetEncryption
{
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
            "message doc { required int32 age; required int32 id; }");

    /**
     * Column encryption only – footer left in plaintext.
     */
    @ParameterizedTest(name = "checkFooterIntegrity={0}, compressed={1}")
    @CsvSource({
            "false,false",
            "false,true",
            "true,false",
            "true,true"
    })
    void columnOnlyFooterPlaintext(boolean checkFooterIntegrity, boolean compressed)
            throws IOException
    {
        File file = createTempFile("pme‑col‑only", ".parquet").toFile();
        file.deleteOnExit();

        writeSingleColumnFile(file, compressed, /*encryptFooter*/ false);

        List<Integer> values = readSingleColumnFile(
                file,
                new TestingKeyRetriever(checkFooterIntegrity ? Optional.of(KEY_FOOT) : Optional.empty(), Optional.of(KEY_AGE), Optional.empty()),
                checkFooterIntegrity,
                Optional.empty());

        verifySequence(values, 100);
    }

    /**
     * Column+ footer encryption (same column as above).
     */
    @ParameterizedTest(name = "compressed={0}")
    @CsvSource({"false", "true"})
    void columnAndFooter(boolean compressed)
            throws IOException
    {
        File file = createTempFile("pme‑col‑foot", ".parquet").toFile();
        file.deleteOnExit();

        writeSingleColumnFile(file, compressed, /*encryptFooter*/ true);

        List<Integer> values = readSingleColumnFile(
                file,
                new TestingKeyRetriever(Optional.of(KEY_FOOT), Optional.of(KEY_AGE), Optional.empty()),
                /*checkFooterIntegrity*/ true,
                Optional.empty());

        verifySequence(values, 100);
    }

    /**
     * Single column encrypted with footer column.
     */
    @ParameterizedTest(name = "encryptFooter={0}")
    @CsvSource({"false", "true"})
    void columnEncryptedWithFooterKey(boolean encryptFooter)
            throws IOException
    {
        File file = createTempFile("pme‑footerKey‑col", ".parquet").toFile();
        file.deleteOnExit();

        // write: column encrypted with footer key, footer encrypted as usual
        writeSingleColumnFile(file, /*compressed*/ false, encryptFooter, /*columnEncryptedWithFooterKey*/ true, Optional.empty(), () -> range(0, 100), OptionalInt.empty(), OptionalInt.empty());

        // read: supply ONLY the footer key (no column key necessary)
        List<Integer> values = readSingleColumnFile(
                file,
                new TestingKeyRetriever(Optional.of(KEY_FOOT), Optional.empty(), Optional.empty()),
                /*checkFooterIntegrity*/ true,
                Optional.empty());

        verifySequence(values, 100);   // data round‑trip OK
    }

    @ParameterizedTest(name = "supplyPrefix={0}")
    @CsvSource({"true", "false"})
    void aadPrefixRoundTrip(boolean supplyPrefix)
            throws IOException
    {
        File file = createTempFile("pme‑aad", ".parquet").toFile();
        file.deleteOnExit();

        byte[] prefix = "fileAAD‑prefix".getBytes(UTF_8);
        writeSingleColumnFile(file, /*compressed*/ true, /*encryptFooter*/ true, /*columnEncryptedWithFooterKey*/ false, Optional.of(prefix), () -> range(0, 100), OptionalInt.empty(), OptionalInt.empty());

        // build FileDecryptionProperties with / without the required prefix
        FileDecryptionProperties.Builder properties = FileDecryptionProperties.builder()
                .withKeyRetriever(new TestingKeyRetriever(Optional.of(KEY_FOOT), Optional.of(KEY_AGE), Optional.empty()));

        if (supplyPrefix) {
            properties.withAadPrefix(prefix);
            // correct prefix => read succeeds
            List<Integer> values = readSingleColumnFile(
                    file,
                    new TestingKeyRetriever(Optional.of(KEY_FOOT), Optional.of(KEY_AGE), Optional.empty()),
                    true,                                       // footer integrity
                    Optional.of(prefix));                       // pass prefix

            verifySequence(values, 100);
        }
        else {
            // missing / wrong prefix => should fail while reading footer
            assertThatThrownBy(() -> readSingleColumnFile(
                    file,
                    new TestingKeyRetriever(Optional.of(KEY_FOOT), Optional.of(KEY_AGE), Optional.empty()),
                    true,
                    Optional.empty()))                          // NO prefix
                    .isInstanceOfAny(ParquetCryptoException.class);
        }
    }

    @ParameterizedTest(name = "rows={0}, rowGroupSize={1}, pageSize={2}, encryptFooter={3}, compressed={4}")
    @CsvSource({
            // rows, rowGroupSizeBytes, pageSizeBytes, encryptFooter, compressed
            "2000, 1024, 128, true,  false",
            "1500, 2048, 256, false, true",
    })
    void multipleRowGroups(int rows, int rowGroupSize, int pageSize, boolean encryptFooter, boolean compressed)
            throws IOException
    {
        File file = createTempFile("pme-multirg", ".parquet").toFile();
        file.deleteOnExit();

        writeSingleColumnFile(
                file,
                compressed,
                encryptFooter,
                /* columnEncryptedWithFooterKey */ false,
                Optional.empty(),
                () -> range(0, rows),
                OptionalInt.of(rowGroupSize),
                OptionalInt.of(pageSize));

        // sanity: we really created >1 row group
        try (ParquetDataSource source = new FileParquetDataSource(file, ParquetReaderOptions.builder().build())) {
            FileDecryptionProperties properties = FileDecryptionProperties.builder()
                    .withKeyRetriever(new TestingKeyRetriever(
                            /* footer */ encryptFooter ? Optional.of(KEY_FOOT) : Optional.empty(),
                            /* column */ Optional.of(KEY_AGE),
                            Optional.empty()))
                    .withCheckFooterIntegrity(encryptFooter)
                    .build();
            ParquetMetadata metadata = MetadataReader.readFooter(source, Optional.empty(), Optional.empty(), Optional.of(properties));
            assertThat(metadata.getBlocks().size()).isGreaterThan(1);
        }

        List<Integer> values = readSingleColumnFile(
                file,
                new TestingKeyRetriever(encryptFooter ? Optional.of(KEY_FOOT) : Optional.empty(), Optional.of(KEY_AGE), Optional.empty()),
                encryptFooter,
                Optional.empty());

        verifySequence(values, rows);
    }

    /**
     * Two columns; footer encrypted; one column plaintext, other encrypted.
     */
    @Test
    void mixedEncryptedAndPlaintextColumns()
            throws IOException
    {
        File file = createTempFile("pme‑mixed‑cols", ".parquet").toFile();
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
        File file = createTempFile("pme‑locked‑col", ".parquet").toFile();
        file.deleteOnExit();

        // both columns encrypted with different keys
        writeTwoColumnFile(file, /*encryptAge*/ true, /*encryptId*/ true);

        // reader has footer key + KEY_AGE only, not KEY_ID
        TestingKeyRetriever retriever = new TestingKeyRetriever(Optional.of(KEY_FOOT), Optional.of(KEY_AGE), Optional.empty());

        List<Integer> values = readSingleColumnFile(file, retriever, true, Optional.empty());

        verifySequence(values, 100);
    }

    /**
     * Single column encrypted, written with dictionary encoding.
     */
    @Test
    void dictionaryEncodedEncryptedColumn()
            throws IOException
    {
        File file = createTempFile("pme‑dict", ".parquet").toFile();
        file.deleteOnExit();

        // create a very small domain so writer chooses dictionary
        writeSingleColumnFile(
                file,
                /*compressed*/ false,
                /*encryptFooter*/ true,
                /*columnEncryptedWithFooterKey*/ false,
                Optional.empty(),
                () -> List.of(1, 2, 3, 1, 2, 3),
                OptionalInt.empty(),
                OptionalInt.empty());   // limited distincts

        List<Integer> values = readSingleColumnFile(
                file,
                new TestingKeyRetriever(Optional.of(KEY_FOOT), Optional.of(KEY_AGE), Optional.empty()),
                true,
                Optional.empty());

        assertThat(new HashSet<>(values)).containsExactlyInAnyOrder(1, 2, 3);

        // ── extra assertion: verify a dictionary really exists ──
        try (ParquetDataSource source = new FileParquetDataSource(file, ParquetReaderOptions.builder().build())) {
            FileDecryptionProperties properties = FileDecryptionProperties.builder()
                    .withKeyRetriever(new TestingKeyRetriever(Optional.of(KEY_FOOT), Optional.of(KEY_AGE), Optional.empty()))
                    .build();

            ParquetMetadata metadata = MetadataReader.readFooter(source, Optional.empty(), Optional.empty(), Optional.of(properties));

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

    @Test
    void readFailsWithoutFooterKey()
            throws IOException
    {
        File file = createTempFile("pme-no-footer-key", ".parquet").toFile();
        file.deleteOnExit();

        // footer + column encrypted
        writeSingleColumnFile(file, /*compressed*/ false, /*encryptFooter*/ true);

        assertThatThrownBy(() -> readSingleColumnFile(
                file,
                new TestingKeyRetriever(/* footer */ Optional.empty(),
                        /* column */ Optional.of(KEY_AGE),
                        Optional.empty()),
                /*checkFooterIntegrity*/ true,
                Optional.empty()))
                .isInstanceOf(ParquetCryptoException.class);
    }

    /**
     * Footer key is present, but the column key is missing.
     * Footer decrypts; column decryption must fail.
     */
    @Test
    void readFailsWithoutColumnKey()
            throws IOException
    {
        File file = createTempFile("pme-no-column-key", ".parquet").toFile();
        file.deleteOnExit();

        // footer + column encrypted
        writeSingleColumnFile(file, /*compressed*/ false, /*encryptFooter*/ true);

        assertThatThrownBy(() -> readSingleColumnFile(
                file,
                new TestingKeyRetriever(/* footer */ Optional.of(KEY_FOOT),
                        /* column */ Optional.empty(),
                        Optional.empty()),
                /*checkFooterIntegrity*/ true,
                Optional.empty()))
                .isInstanceOf(ParquetCryptoException.class);
    }

    @Test
    void readFailsWithInvalidFooterKey()
            throws IOException
    {
        File file = createTempFile("pme-bad-footer", ".parquet").toFile();
        file.deleteOnExit();

        writeSingleColumnFile(file, /*compressed*/ false, /*encryptFooter*/ true);

        byte[] wrongFooter = "thisIsTheBadKey!".getBytes(UTF_8);   // 16 bytes != real key

        assertThatThrownBy(() -> readSingleColumnFile(
                file,
                new TestingKeyRetriever(Optional.of(wrongFooter), Optional.of(KEY_AGE), Optional.empty()),
                /*checkFooterIntegrity*/ true,
                Optional.empty()))
                .isInstanceOf(ParquetCryptoException.class);
    }

    /**
     * Footer key is correct but the column key is wrong.
     * Footer decrypts, column decryption must still fail.
     */
    @Test
    void readFailsWithInvalidColumnKey()
            throws IOException
    {
        File file = createTempFile("pme-bad-column", ".parquet").toFile();
        file.deleteOnExit();

        writeSingleColumnFile(file, /*compressed*/ false, /*encryptFooter*/ true);

        byte[] wrongColumn = "badColumnKey123".getBytes(UTF_8);  // 16 bytes

        assertThatThrownBy(() -> readSingleColumnFile(
                file,
                new TestingKeyRetriever(Optional.of(KEY_FOOT), Optional.of(wrongColumn), Optional.empty()),
                /*checkFooterIntegrity*/ true,
                Optional.empty()))
                .isInstanceOf(ParquetCryptoException.class);
    }

    @Test
    void encryptedDictionaryPruningTwoColumns()
            throws IOException
    {
        File file = createTempFile("pme-dict-2cols", ".parquet").toFile();
        file.deleteOnExit();

        int missingAge = 7;
        int missingId = 3;
        writeTwoColumnEncryptedDictionaryFile(file, missingAge, missingId);

        // Open with footer key + AGE key only (no ID key)
        try (ParquetDataSource source = new FileParquetDataSource(file, ParquetReaderOptions.builder().build())) {
            FileDecryptionProperties props = FileDecryptionProperties.builder()
                    .withKeyRetriever(new TestingKeyRetriever(
                            Optional.of(KEY_FOOT),      // footer
                            Optional.of(KEY_AGE),       // age column key
                            Optional.empty()))          // NO id key
                    .withCheckFooterIntegrity(true)
                    .build();

            ParquetMetadata metadata = MetadataReader.readFooter(
                    source, Optional.empty(), Optional.empty(), Optional.of(props));

            ColumnDescriptor age = new ColumnDescriptor(
                    new String[] {"age"},
                    Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("age"),
                    0, 0);

            ColumnDescriptor id = new ColumnDescriptor(
                    new String[] {"id"},
                    Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("id"),
                    0, 0);

            // ——— Predicate on accessible column (age = missingAge) → dictionary-based pruning to 0 ———
            TupleDomain<ColumnDescriptor> domainAge = TupleDomain.withColumnDomains(ImmutableMap.of(age, singleValue(INTEGER, (long) missingAge)));

            TupleDomainParquetPredicate predicateAge = new TupleDomainParquetPredicate(
                    domainAge, ImmutableList.of(age), UTC);

            List<RowGroupInfo> groupsAge = getFilteredRowGroups(
                    0,
                    source.getEstimatedSize(),
                    source,
                    metadata,
                    List.of(domainAge),
                    List.of(predicateAge),
                    ImmutableMap.of(ImmutableList.of("age"), age),
                    UTC,
                    200,
                    ParquetReaderOptions.builder().build());

            // No row-groups should pass after dictionary pruning
            assertThat(groupsAge).isEmpty();

            // ——— Predicate on inaccessible column (id = missingId) → should fail (no column key) ———
            TupleDomain<ColumnDescriptor> domainId = TupleDomain.withColumnDomains(ImmutableMap.of(id, singleValue(INTEGER, (long) missingId)));

            TupleDomainParquetPredicate predicateId = new TupleDomainParquetPredicate(domainId, ImmutableList.of(id), UTC);

            assertThatThrownBy(() -> getFilteredRowGroups(
                    0,
                    source.getEstimatedSize(),
                    source,
                    metadata,
                    List.of(domainId),
                    List.of(predicateId),
                    ImmutableMap.of(ImmutableList.of("id"), id),
                    UTC,
                    200,
                    ParquetReaderOptions.builder().build()))
                    // keep this broad; different layers may surface different messages
                    .hasMessageMatching("(?s).*access.*column.*id.*|.*decrypt.*id.*|.*key.*id.*");
        }
    }

    @Test
    void encryptedBloomFilterPruningTwoColumns()
            throws IOException
    {
        File file = createTempFile("pme-bloom-2cols", ".parquet").toFile();
        file.deleteOnExit();

        int missingAge = 7;
        int missingId = 3;
        writeTwoColumnEncryptedBloomFile(file, missingAge, missingId);
        assertBloomFiltersPresent(file);

        try (ParquetDataSource source = new FileParquetDataSource(file, ParquetReaderOptions.builder().build())) {
            // Footer + AGE key only (no ID key)
            FileDecryptionProperties props = FileDecryptionProperties.builder()
                    .withKeyRetriever(new TestingKeyRetriever(
                            Optional.of(KEY_FOOT),      // footer
                            Optional.of(KEY_AGE),       // age column key
                            Optional.empty()))          // NO id key
                    .withCheckFooterIntegrity(true)
                    .build();

            ParquetMetadata metadata = MetadataReader.readFooter(source, Optional.empty(), Optional.empty(), Optional.of(props));

            ColumnDescriptor age = new ColumnDescriptor(
                    new String[] {"age"},
                    Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("age"),
                    0, 0);

            ColumnDescriptor id = new ColumnDescriptor(
                    new String[] {"id"},
                    Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("id"),
                    0, 0);

            // --- Predicate on accessible column (age == missingAge) → Bloom filter should prune to 0
            TupleDomain<ColumnDescriptor> domainAge = TupleDomain.withColumnDomains(
                    ImmutableMap.of(age, singleValue(INTEGER, (long) missingAge)));
            TupleDomainParquetPredicate predicateAge = new TupleDomainParquetPredicate(domainAge, ImmutableList.of(age), UTC);

            List<RowGroupInfo> groupsAge = getFilteredRowGroups(
                    0,
                    source.getEstimatedSize(),
                    source,
                    metadata,
                    List.of(domainAge),
                    List.of(predicateAge),
                    ImmutableMap.of(ImmutableList.of("age"), age),
                    UTC,
                    200,
                    ParquetReaderOptions.builder().build());

            assertThat(groupsAge).isEmpty(); // pruned by encrypted Bloom filter

            // Sanity: present value should not prune to 0
            TupleDomain<ColumnDescriptor> domainAgePresent = TupleDomain.withColumnDomains(
                    ImmutableMap.of(age, singleValue(INTEGER, 5L)));
            TupleDomainParquetPredicate predicateAgePresent = new TupleDomainParquetPredicate(domainAgePresent, ImmutableList.of(age), UTC);
            List<RowGroupInfo> groupsAgePresent = getFilteredRowGroups(
                    0,
                    source.getEstimatedSize(),
                    source,
                    metadata,
                    List.of(domainAgePresent),
                    List.of(predicateAgePresent),
                    ImmutableMap.of(ImmutableList.of("age"), age),
                    UTC,
                    200,
                    ParquetReaderOptions.builder().build());
            assertThat(groupsAgePresent).isNotEmpty();

            // --- Predicate on inaccessible column (id == missingId) → should fail (no column key)
            TupleDomain<ColumnDescriptor> domainId = TupleDomain.withColumnDomains(
                    ImmutableMap.of(id, singleValue(INTEGER, (long) missingId)));
            TupleDomainParquetPredicate predicateId = new TupleDomainParquetPredicate(domainId, ImmutableList.of(id), UTC);

            assertThatThrownBy(() -> getFilteredRowGroups(
                    0,
                    source.getEstimatedSize(),
                    source,
                    metadata,
                    List.of(domainId),
                    List.of(predicateId),
                    ImmutableMap.of(ImmutableList.of("id"), id),
                    UTC,
                    200,
                    ParquetReaderOptions.builder().build()))
                    .hasMessageMatching("(?s).*access.*column.*id.*|.*decrypt.*id.*|.*key.*id.*");
        }
    }

    /**
     * Single‑column writer with knobs.
     */
    private static void writeSingleColumnFile(File target, boolean compressed, boolean encryptFooter)
            throws IOException
    {
        writeSingleColumnFile(target, compressed, encryptFooter, false, Optional.empty(), () -> range(0, 100), OptionalInt.empty(), OptionalInt.empty());
    }

    /**
     * Overload that takes explicit values supplier (for dictionary case).
     */
    private static void writeSingleColumnFile(
            File target,
            boolean compressed,
            boolean encryptFooter,
            boolean columnEncryptedWithFooterKey,
            Optional<byte[]> aadPrefix,
            Supplier<List<Integer>> valuesSupplier,
            OptionalInt rowGroupSizeBytes,
            OptionalInt pageSizeBytes)
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

        aadPrefix.ifPresent(prefix -> {
            fileEncryption.withAADPrefix(prefix);
            fileEncryption.withoutAADPrefixStorage();
        });

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

        rowGroupSizeBytes.ifPresent(builder::withRowGroupSize);
        // tiny page => even 100 ints give us ≥ 2 data pages
        builder.withPageSize(pageSizeBytes.orElse(64));

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
                .withWriteMode(OVERWRITE)
                // tiny page => even 100 ints give us ≥ 2 data pages
                .withPageSize(64);

        try (ParquetWriter<Group> writer = builder.build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(TWO_COL_SCHEMA);
            for (int i = 0; i < 100; i++) {
                writer.write(factory.newGroup().append("id", 100 - i).append("age", i));
            }
        }
    }

    private static void writeTwoColumnEncryptedDictionaryFile(File target, int missingAge, int missingId)
            throws IOException
    {
        FileEncryptionProperties fileEncryption = FileEncryptionProperties.builder(KEY_FOOT)
                .withAlgorithm(ParquetCipher.AES_GCM_CTR_V1)
                .withEncryptedColumns(ImmutableMap.of(
                        AGE_PATH, ColumnEncryptionProperties.builder(AGE_PATH).withKey(KEY_AGE).build(),
                        ID_PATH, ColumnEncryptionProperties.builder(ID_PATH).withKey(KEY_ID).build()))
                .build();

        ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(new Path(target.getAbsolutePath()))
                .withType(TWO_COL_SCHEMA)
                .withConf(new Configuration())
                .withEncryption(fileEncryption)
                .withWriteMode(OVERWRITE)
                .withPageSize(1024); // small pages → strong chance of dictionary encoding

        writeSampleData(builder, missingAge, missingId);
        assertDictionariesPresent(target);
    }

    private static void assertDictionariesPresent(File file)
            throws IOException
    {
        try (ParquetDataSource source = new FileParquetDataSource(file, ParquetReaderOptions.builder().build())) {
            FileDecryptionProperties properties = FileDecryptionProperties.builder()
                    // for metadata inspection we provide BOTH column keys + footer key
                    .withKeyRetriever(new TestingKeyRetriever(
                            Optional.of(KEY_FOOT),
                            Optional.of(KEY_AGE),
                            Optional.of(KEY_ID)))
                    .withCheckFooterIntegrity(true)
                    .build();

            ParquetMetadata metadata = MetadataReader.readFooter(source, Optional.empty(), Optional.empty(), Optional.of(properties));

            ColumnChunkMetadata age = metadata.getBlocks().getFirst().columns().stream()
                    .filter(context -> context.getPath().equals(AGE_PATH))
                    .findFirst().orElseThrow();

            ColumnChunkMetadata id = metadata.getBlocks().getFirst().columns().stream()
                    .filter(context -> context.getPath().equals(ID_PATH))
                    .findFirst().orElseThrow();

            // dictionary page present
            assertThat(age.getDictionaryPageOffset()).isGreaterThan(0);
            assertThat(id.getDictionaryPageOffset()).isGreaterThan(0);

            // column encodings include a dictionary encoding
            assertThat(age.getEncodings()).anyMatch(org.apache.parquet.column.Encoding::usesDictionary);
            assertThat(id.getEncodings()).anyMatch(org.apache.parquet.column.Encoding::usesDictionary);

            // (optional) every data page is dictionary-encoded
            assertThat(io.trino.parquet.ParquetReaderUtils.isOnlyDictionaryEncodingPages(age)).isTrue();
            assertThat(io.trino.parquet.ParquetReaderUtils.isOnlyDictionaryEncodingPages(id)).isTrue();
        }
    }

    private static void writeTwoColumnEncryptedBloomFile(File target, int missingAge, int missingId)
            throws IOException
    {
        // Encryption: both columns with different keys; encrypted footer
        FileEncryptionProperties fileEncryption = FileEncryptionProperties.builder(KEY_FOOT)
                .withAlgorithm(ParquetCipher.AES_GCM_CTR_V1)
                .withEncryptedColumns(ImmutableMap.of(
                        AGE_PATH, ColumnEncryptionProperties.builder(AGE_PATH).withKey(KEY_AGE).build(),
                        ID_PATH, ColumnEncryptionProperties.builder(ID_PATH).withKey(KEY_ID).build()))
                .build();

        // Enable bloom filters and select columns
        ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(new Path(target.getAbsolutePath()))
                .withType(TWO_COL_SCHEMA)
                .withEncryption(fileEncryption)
                .withWriteMode(OVERWRITE)
                .withPageSize(1024)
                // Bloom filters won't be used with dictionary encoding
                .withDictionaryEncoding(false)
                .withBloomFilterEnabled(true);

        writeSampleData(builder, missingAge, missingId);
        assertBloomFiltersPresent(target);
    }

    private static void writeSampleData(ExampleParquetWriter.Builder builder, int missingAge, int missingId)
            throws IOException
    {
        try (ParquetWriter<Group> writer = builder.build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(TWO_COL_SCHEMA);
            for (int i = 0; i < 5000; i++) {
                int id = i % 11;
                // ensure 'missingId' not present
                if (id == missingId) {
                    id = (id + 1) % 11;
                }
                int age = i % 11;
                // ensure 'missingAge' not present
                if (age == missingAge) {
                    age = (age + 1) % 11;
                }
                writer.write(factory.newGroup().append("id", id).append("age", age));
            }
        }
    }

    private static void assertBloomFiltersPresent(File file)
            throws IOException
    {
        try (ParquetDataSource source = new FileParquetDataSource(file, ParquetReaderOptions.builder().build())) {
            // Provide ALL keys for metadata inspection
            FileDecryptionProperties properties = FileDecryptionProperties.builder()
                    .withKeyRetriever(new TestingKeyRetriever(
                            Optional.of(KEY_FOOT),
                            Optional.of(KEY_AGE),
                            Optional.of(KEY_ID)))
                    .withCheckFooterIntegrity(true)
                    .build();

            ParquetMetadata metadata = MetadataReader.readFooter(source, Optional.empty(), Optional.empty(), Optional.of(properties));

            ColumnChunkMetadata age = metadata.getBlocks().getFirst().columns().stream()
                    .filter(context -> context.getPath().equals(AGE_PATH))
                    .findFirst().orElseThrow();

            ColumnChunkMetadata id = metadata.getBlocks().getFirst().columns().stream()
                    .filter(context -> context.getPath().equals(ID_PATH))
                    .findFirst().orElseThrow();

            // Bloom filter offsets must be present for both columns
            assertThat(age.getBloomFilterOffset()).isGreaterThan(0L);
            assertThat(id.getBloomFilterOffset()).isGreaterThan(0L);
        }
    }

    /**
     * Reads the single‑column file and returns the “age” values.
     */
    private static List<Integer> readSingleColumnFile(
            File file,
            DecryptionKeyRetriever keyRetriever,
            boolean checkFooterIntegrity,
            Optional<byte[]> aadPrefix)
            throws IOException
    {
        ParquetDataSource source = new FileParquetDataSource(file, ParquetReaderOptions.builder().build());

        FileDecryptionProperties.Builder properties = FileDecryptionProperties
                .builder()
                .withKeyRetriever(keyRetriever)
                .withCheckFooterIntegrity(checkFooterIntegrity);
        aadPrefix.ifPresent(properties::withAadPrefix);

        ParquetMetadata metadata = MetadataReader.readFooter(
                source, Optional.empty(), Optional.empty(), Optional.of(properties.build()));

        ColumnDescriptor descriptor = new ColumnDescriptor(
                new String[] {"age"},
                Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("age"),
                0, 0);

        Map<List<String>, ColumnDescriptor> byPath = ImmutableMap.of(
                ImmutableList.of("age"), descriptor);

        TupleDomain<ColumnDescriptor> domain = TupleDomain.all();
        TupleDomainParquetPredicate predicate = new TupleDomainParquetPredicate(
                domain, ImmutableList.of(descriptor), UTC);

        List<RowGroupInfo> groups = getFilteredRowGroups(
                0, source.getEstimatedSize(),
                source, metadata,
                List.of(domain), List.of(predicate),
                byPath, UTC, 200, ParquetReaderOptions.builder().build());

        PrimitiveField field = new PrimitiveField(INTEGER, true, descriptor, 0);
        io.trino.parquet.Column column = new io.trino.parquet.Column("age", field);

        try (ParquetReader reader = new ParquetReader(
                Optional.ofNullable(metadata.getFileMetaData().getCreatedBy()),
                List.of(column),
                false,
                groups,
                source,
                UTC,
                newSimpleAggregatedMemoryContext(),
                ParquetReaderOptions.builder().build(),
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
        ParquetDataSource source = new FileParquetDataSource(file, ParquetReaderOptions.builder().build());

        FileDecryptionProperties properties = FileDecryptionProperties
                .builder().withKeyRetriever(retriever).withCheckFooterIntegrity(true).build();

        ParquetMetadata metadata = MetadataReader.readFooter(source, Optional.empty(), Optional.empty(), Optional.of(properties));

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
                TupleDomain.all(), ImmutableList.of(ageDescriptor, idDescriptor), UTC);

        List<RowGroupInfo> groups = getFilteredRowGroups(
                0, source.getEstimatedSize(), source, metadata,
                List.of(TupleDomain.all()), List.of(predicate),
                byPath, UTC, 200, ParquetReaderOptions.builder().build());

        PrimitiveField ageField = new PrimitiveField(INTEGER, true, ageDescriptor, 0);
        PrimitiveField idField = new PrimitiveField(INTEGER, true, idDescriptor, 1);

        List<io.trino.parquet.Column> columns = List.of(
                new io.trino.parquet.Column("age", ageField),
                new io.trino.parquet.Column("id", idField));

        try (ParquetReader reader = new ParquetReader(
                Optional.ofNullable(metadata.getFileMetaData().getCreatedBy()),
                columns,
                false, groups, source,
                UTC,
                newSimpleAggregatedMemoryContext(),
                ParquetReaderOptions.builder().build(),
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

    private static final class TestingKeyRetriever
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
