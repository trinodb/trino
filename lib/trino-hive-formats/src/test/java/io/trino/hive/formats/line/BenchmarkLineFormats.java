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
package io.trino.hive.formats.line;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.hive.formats.encodings.text.TextEncodingOptions;
import io.trino.hive.formats.line.csv.CsvDeserializer;
import io.trino.hive.formats.line.simple.SimpleDeserializer;
import io.trino.hive.formats.line.text.TextLineReader;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.Type;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;

/**
 * Benchmarks the text line read path: newline scanning in {@link TextLineReader} and
 * field splitting plus value decoding in the LazySimpleSerDe-compatible deserializer.
 */
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Measurement(iterations = 15, time = 1)
@Warmup(iterations = 8, time = 1)
@Fork(2)
public class BenchmarkLineFormats
{
    private static final int LINE_COUNT = 200_000;
    private static final int BUFFER_SIZE = 8 * 1024;
    private static final int MAX_LINE_LENGTH = 8 * 1024;
    private static final int WIDE_TABLE_COLUMNS = 100;
    private static final int NARROW_PROJECTION = 3;

    public enum ColumnType
    {
        VARCHAR_TYPE(VARCHAR),
        BIGINT_TYPE(BIGINT),
        DOUBLE_TYPE(DOUBLE),
        DECIMAL_TYPE(createDecimalType(12, 2));

        private final Type type;

        ColumnType(Type type)
        {
            this.type = type;
        }

        Type getType()
        {
            return type;
        }

        String generateValue(Random random)
        {
            // Locale.ROOT is required: a locale with a comma decimal separator would emit values
            // that fail to parse, making the benchmark measure the exception path instead.
            return switch (this) {
                case VARCHAR_TYPE -> STATION_NAMES.get(random.nextInt(STATION_NAMES.size()));
                case BIGINT_TYPE -> Long.toString(random.nextLong(-1_000_000_000L, 1_000_000_000L));
                case DOUBLE_TYPE -> String.format(Locale.ROOT, "%.1f", random.nextDouble(-99.9, 99.9));
                case DECIMAL_TYPE -> String.format(Locale.ROOT, "%.2f", random.nextDouble(-9_999_999.99, 9_999_999.99));
            };
        }
    }

    private static final List<String> STATION_NAMES = ImmutableList.of(
            "Hamburg",
            "Bulawayo",
            "Palembang",
            "St. John's",
            "Cracow",
            "Bridgetown",
            "Istanbul",
            "Roseau",
            "Conakry",
            "Warsaw",
            "Chicago",
            "Mumbai",
            "Ouagadougou",
            "Reykjavik");

    /**
     * Number of columns in the generated file. Every column has the same type so that a
     * regression in one decoder is not masked by the cost of the others.
     */
    @Param("4")
    public int columnCount;

    @Param({"VARCHAR_TYPE", "BIGINT_TYPE", "DOUBLE_TYPE", "DECIMAL_TYPE"})
    public ColumnType columnType;

    private byte[] data;
    private List<Column> columns;
    private List<Type> types;
    private LineDeserializer deserializer;

    // CSV only supports varchar columns, so it reads the same values as text
    private byte[] csvData;
    private List<Type> csvTypes;
    private LineDeserializer csvDeserializer;

    // a wide table with only the leading columns projected, which is the common shape of a Hive
    // text table that is not read in full
    private byte[] wideData;
    private List<Type> narrowTypes;
    private LineDeserializer narrowDeserializer;

    // escaping enabled, over values that do not actually contain an escape, which is the common case
    private LineDeserializer escapedDeserializer;

    @Setup
    public void setup()
            throws IOException
    {
        byte separator = TextEncodingOptions.DEFAULT_SIMPLE_OPTIONS.getSeparators().getByte(0);

        columns = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            columns.add(new Column("column_" + i, columnType.getType(), i));
        }
        types = columns.stream()
                .map(Column::type)
                .collect(Collectors.toList());

        Random random = new Random(3846);
        StringBuilder builder = new StringBuilder();
        for (int line = 0; line < LINE_COUNT; line++) {
            for (int column = 0; column < columnCount; column++) {
                if (column > 0) {
                    builder.append((char) separator);
                }
                builder.append(columnType.generateValue(random));
            }
            builder.append('\n');
        }
        data = builder.toString().getBytes(StandardCharsets.UTF_8);

        deserializer = new SimpleDeserializer(columns, TextEncodingOptions.DEFAULT_SIMPLE_OPTIONS, columnCount);

        List<Column> csvColumns = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            csvColumns.add(new Column("column_" + i, VARCHAR, i));
        }
        csvTypes = csvColumns.stream()
                .map(Column::type)
                .collect(Collectors.toList());
        csvDeserializer = new CsvDeserializer(csvColumns, ',', '"', '\\');

        random = new Random(3846);
        StringBuilder csvBuilder = new StringBuilder();
        for (int line = 0; line < LINE_COUNT; line++) {
            for (int column = 0; column < columnCount; column++) {
                if (column > 0) {
                    csvBuilder.append(',');
                }
                csvBuilder.append(columnType.generateValue(random));
            }
            csvBuilder.append('\n');
        }
        csvData = csvBuilder.toString().getBytes(StandardCharsets.UTF_8);

        List<Column> narrowColumns = new ArrayList<>();
        for (int i = 0; i < NARROW_PROJECTION; i++) {
            narrowColumns.add(new Column("column_" + i, columnType.getType(), i));
        }
        narrowTypes = narrowColumns.stream()
                .map(Column::type)
                .collect(Collectors.toList());
        narrowDeserializer = new SimpleDeserializer(narrowColumns, TextEncodingOptions.DEFAULT_SIMPLE_OPTIONS, WIDE_TABLE_COLUMNS);

        random = new Random(3846);
        StringBuilder wideBuilder = new StringBuilder();
        for (int line = 0; line < LINE_COUNT; line++) {
            for (int column = 0; column < WIDE_TABLE_COLUMNS; column++) {
                if (column > 0) {
                    wideBuilder.append((char) separator);
                }
                wideBuilder.append(columnType.generateValue(random));
            }
            wideBuilder.append('\n');
        }
        wideData = wideBuilder.toString().getBytes(StandardCharsets.UTF_8);

        TextEncodingOptions escapedOptions = TextEncodingOptions.builder()
                .escapeByte((byte) '\\')
                .build();
        escapedDeserializer = new SimpleDeserializer(columns, escapedOptions, columnCount);
    }

    /**
     * Reads a table with escaping enabled. The values contain no escapes, which is the common case.
     */
    @Benchmark
    public List<Page> deserializeSimpleEscaped()
            throws IOException
    {
        TextLineReader reader = TextLineReader.createUncompressedReader(new ByteArrayInputStream(data), BUFFER_SIZE);
        LineBuffer lineBuffer = new LineBuffer(1024, MAX_LINE_LENGTH);
        PageBuilder pageBuilder = new PageBuilder(types);

        List<Page> pages = new ArrayList<>();
        while (reader.readLine(lineBuffer)) {
            escapedDeserializer.deserialize(lineBuffer, pageBuilder);
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        if (!pageBuilder.isEmpty()) {
            pages.add(pageBuilder.build());
        }
        return pages;
    }

    /**
     * Reads only the leading columns of a wide table, so the line does not need to be split past them.
     */
    @Benchmark
    public List<Page> deserializeSimpleNarrowProjection()
            throws IOException
    {
        TextLineReader reader = TextLineReader.createUncompressedReader(new ByteArrayInputStream(wideData), BUFFER_SIZE);
        LineBuffer lineBuffer = new LineBuffer(1024, MAX_LINE_LENGTH);
        PageBuilder pageBuilder = new PageBuilder(narrowTypes);

        List<Page> pages = new ArrayList<>();
        while (reader.readLine(lineBuffer)) {
            narrowDeserializer.deserialize(lineBuffer, pageBuilder);
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        if (!pageBuilder.isEmpty()) {
            pages.add(pageBuilder.build());
        }
        return pages;
    }

    /**
     * CSV read path: newline scan, field split, and value decode into pages.
     */
    @Benchmark
    public List<Page> deserializeCsv()
            throws IOException
    {
        TextLineReader reader = TextLineReader.createUncompressedReader(new ByteArrayInputStream(csvData), BUFFER_SIZE);
        LineBuffer lineBuffer = new LineBuffer(1024, MAX_LINE_LENGTH);
        PageBuilder pageBuilder = new PageBuilder(csvTypes);

        List<Page> pages = new ArrayList<>();
        while (reader.readLine(lineBuffer)) {
            csvDeserializer.deserialize(lineBuffer, pageBuilder);
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        if (!pageBuilder.isEmpty()) {
            pages.add(pageBuilder.build());
        }
        return pages;
    }

    /**
     * Isolates newline scanning: reads every line, decodes nothing.
     */
    @Benchmark
    public long readLines()
            throws IOException
    {
        TextLineReader reader = TextLineReader.createUncompressedReader(new ByteArrayInputStream(data), BUFFER_SIZE);
        LineBuffer lineBuffer = new LineBuffer(1024, MAX_LINE_LENGTH);
        long bytes = 0;
        while (reader.readLine(lineBuffer)) {
            bytes += lineBuffer.getLength();
        }
        return bytes;
    }

    /**
     * Full read path: newline scan, field split, and value decode into pages.
     */
    @Benchmark
    public List<Page> deserializeSimple()
            throws IOException
    {
        TextLineReader reader = TextLineReader.createUncompressedReader(new ByteArrayInputStream(data), BUFFER_SIZE);
        LineBuffer lineBuffer = new LineBuffer(1024, MAX_LINE_LENGTH);
        PageBuilder pageBuilder = new PageBuilder(types);

        List<Page> pages = new ArrayList<>();
        while (reader.readLine(lineBuffer)) {
            deserializer.deserialize(lineBuffer, pageBuilder);
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        if (!pageBuilder.isEmpty()) {
            pages.add(pageBuilder.build());
        }
        return pages;
    }

    /**
     * Verifies the generated data actually decodes. Without this, a data generation bug that makes
     * every value unparseable would silently turn the benchmark into a measurement of the
     * NumberFormatException path rather than of the decoder.
     */
    @VisibleForTesting
    void verifyDecodes()
    {
        try {
            setup();
            int positions = 0;
            for (Page page : deserializeSimple()) {
                positions += page.getPositionCount();
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    if (page.getBlock(channel).mayHaveNull()) {
                        throw new IllegalStateException("Generated %s data did not decode: column %s contains nulls".formatted(columnType, channel));
                    }
                }
            }
            if (positions != LINE_COUNT) {
                throw new IllegalStateException("Expected %s rows for %s, got %s".formatted(LINE_COUNT, columnType, positions));
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static void main()
            throws Exception
    {
        BenchmarkLineFormats benchmark = new BenchmarkLineFormats();
        benchmark.columnCount = 4;
        for (ColumnType columnType : ColumnType.values()) {
            benchmark.columnType = columnType;
            benchmark.verifyDecodes();
        }

        benchmark(BenchmarkLineFormats.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g"))
                .run();
    }
}
