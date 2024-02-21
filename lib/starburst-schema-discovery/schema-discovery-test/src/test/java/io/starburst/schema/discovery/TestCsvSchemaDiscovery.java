/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.starburst.schema.discovery.formats.csv.CsvFlags;
import io.starburst.schema.discovery.formats.csv.CsvOptions;
import io.starburst.schema.discovery.formats.csv.CsvSchemaDiscovery;
import io.starburst.schema.discovery.models.DiscoveredColumns;
import io.trino.plugin.hive.type.DecimalTypeInfo;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.starburst.schema.discovery.Util.toColumn;
import static io.starburst.schema.discovery.Util.withoutSamples;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_BOOLEAN;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_DATE;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_DOUBLE;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_INT;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_LONG;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_STRING;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_TIMESTAMP;
import static io.starburst.schema.discovery.internal.HiveTypes.arrayType;
import static io.starburst.schema.discovery.internal.HiveTypes.mapType;
import static io.starburst.schema.discovery.internal.TextFileLines.MAX_GUESS_BUFFER_SIZE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCsvSchemaDiscovery
{
    @Test
    public void testDecimal()
    {
        ImmutableMap<String, String> options = ImmutableMap.of(CsvOptions.SAMPLE_LINES_MODULO, "1");
        DiscoveredColumns schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/decimal.csv"), options);
        assertThat(withoutSamples(schemaColumns.columns())).containsExactly(toColumn("decimal", new DecimalTypeInfo(20, 0)),
                toColumn("long", HIVE_LONG),
                toColumn("double", HIVE_DOUBLE));
        assertThat(schemaColumns.flags()).isEmpty();
    }

    @Test
    public void testNumbers()
    {
        ImmutableMap<String, String> options = ImmutableMap.of(CsvOptions.SAMPLE_LINES_MODULO, "1",
                CsvOptions.NULL_VALUE, "--",
                CsvOptions.NAN_VALUE, "NAN",
                CsvOptions.POSITIVE_INF, "INF",
                CsvOptions.NEGATIVE_INF, "-INF");
        DiscoveredColumns schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/numbers.csv"), options);
        assertThat(withoutSamples(schemaColumns.columns())).containsExactly(toColumn("int", HIVE_INT),
                toColumn("long", HIVE_INT),  // this shows as long in the Spark source but I don't see how
                toColumn("float", HIVE_DOUBLE),
                toColumn("double", HIVE_DOUBLE));
    }

    @Test
    public void testCars()
    {
        ImmutableMap<String, String> options = ImmutableMap.of(CsvOptions.SAMPLE_LINES_MODULO, "1");
        DiscoveredColumns schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/cars.csv"), options);
        assertThat(withoutSamples(schemaColumns.columns())).containsExactly(toColumn("year", HIVE_INT),
                toColumn("make", HIVE_STRING),
                toColumn("model", HIVE_STRING),
                toColumn("comment", HIVE_STRING),
                toColumn("blank", HIVE_STRING));
        assertThat(schemaColumns.flags()).containsExactly(CsvFlags.HAS_QUOTED_FIELDS);
    }

    @Test
    public void testCars8859()
    {
        ImmutableMap<String, String> options = ImmutableMap.of(CsvOptions.SAMPLE_LINES_MODULO, "1",
                CsvOptions.CHARSET, "ISO-8859-1",
                CsvOptions.DELIMITER, "þ");
        DiscoveredColumns schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/cars-iso-8859-1.csv"), options);
        assertThat(withoutSamples(schemaColumns.columns())).containsExactly(toColumn("year", HIVE_INT),
                toColumn("make", HIVE_STRING),
                toColumn("model", HIVE_STRING),
                toColumn("comment", HIVE_STRING),
                toColumn("blank", HIVE_STRING));
        assertThat(schemaColumns.flags()).containsExactly(CsvFlags.HAS_QUOTED_FIELDS);
    }

    @Test
    public void testCarsAlt()
    {
        ImmutableMap<String, String> options = ImmutableMap.of(CsvOptions.SAMPLE_LINES_MODULO, "1",
                CsvOptions.QUOTE, "'",
                CsvOptions.DELIMITER, "|");
        DiscoveredColumns schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/cars-alternative.csv"), options);
        assertThat(withoutSamples(schemaColumns.columns())).containsExactly(toColumn("year", HIVE_INT),
                toColumn("make", HIVE_STRING),
                toColumn("model", HIVE_STRING),
                toColumn("comment", HIVE_STRING),
                toColumn("blank", HIVE_STRING));
    }

    @Test
    public void testCarsTabs()
    {
        ImmutableMap<String, String> options = ImmutableMap.of(CsvOptions.SAMPLE_LINES_MODULO, "1",
                CsvOptions.DELIMITER, "\t");
        DiscoveredColumns schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/cars.tsv"), options);
        assertThat(withoutSamples(schemaColumns.columns())).containsExactly(toColumn("year", HIVE_INT),
                toColumn("make", HIVE_STRING),
                toColumn("model", HIVE_STRING),
                toColumn("price", HIVE_DOUBLE),
                toColumn("comment", HIVE_STRING),
                toColumn("blank", HIVE_STRING));
        assertThat(schemaColumns.flags()).containsExactly(CsvFlags.HAS_QUOTED_FIELDS);
    }

    @Test
    public void testCarsBlankColumnName()
    {
        ImmutableMap<String, String> options = ImmutableMap.of(CsvOptions.SAMPLE_LINES_MODULO, "1");
        DiscoveredColumns schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/cars-blank-column-name.csv"), options);
        assertThat(withoutSamples(schemaColumns.columns())).containsExactly(toColumn("_", HIVE_INT),
                toColumn("_", HIVE_STRING),
                toColumn("make", HIVE_STRING),
                toColumn("customer", HIVE_STRING),
                toColumn("comment", HIVE_STRING));
    }

    @Test
    public void testEmpty()
    {
        DiscoveredColumns schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/empty.csv"), ImmutableMap.of());
        assertThat(schemaColumns.columns()).isEmpty();
    }

    @Test
    public void testEmptyNoHeaders()
    {
        ImmutableMap<String, String> options = ImmutableMap.of(CsvOptions.FIRST_LINE_IS_HEADERS, "false");
        DiscoveredColumns schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/empty.csv"), options);
        assertThat(schemaColumns.columns()).isEmpty();
    }

    @Test
    public void testMalformedRow()
    {
        ImmutableMap<String, String> options = ImmutableMap.of(CsvOptions.SAMPLE_LINES_MODULO, "1");
        DiscoveredColumns schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/malformed-row.csv"), options);
        assertThat(withoutSamples(schemaColumns.columns())).containsExactly(toColumn("fruit", HIVE_STRING),
                toColumn("color", HIVE_STRING),
                toColumn("price", HIVE_INT),
                toColumn("quantity", HIVE_INT));
    }

    @Test
    public void testBool()
    {
        ImmutableMap<String, String> options = ImmutableMap.of(CsvOptions.SAMPLE_LINES_MODULO, "1");
        DiscoveredColumns schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/bool.csv"), options);
        assertThat(withoutSamples(withoutSamples(schemaColumns.columns()))).containsExactly(toColumn("bool", HIVE_BOOLEAN));
        assertThat(schemaColumns.flags()).containsExactly(CsvFlags.HAS_QUOTED_FIELDS);
    }

    @Test
    public void testSamplingDifferences()
    {
        ImmutableMap<String, String> options = ImmutableMap.of(CsvOptions.SAMPLE_LINES_MODULO, "2");
        DiscoveredColumns schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/mixed-types.csv"), options);
        assertThat(withoutSamples(schemaColumns.columns())).containsExactly(toColumn("a", HIVE_INT));

        options = ImmutableMap.of(CsvOptions.SAMPLE_LINES_MODULO, "3");
        schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/mixed-types.csv"), options);
        assertThat(withoutSamples(schemaColumns.columns())).containsExactly(toColumn("a", HIVE_DOUBLE));

        options = ImmutableMap.of(CsvOptions.SAMPLE_LINES_MODULO, "1", CsvOptions.MAX_SAMPLE_LINES, "2");
        schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/mixed-types.csv"), options);
        assertThat(withoutSamples(schemaColumns.columns())).containsExactly(toColumn("a", HIVE_INT));

        options = ImmutableMap.of(CsvOptions.SAMPLE_LINES_MODULO, "1");
        schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/mixed-types.csv"), options);
        assertThat(withoutSamples(schemaColumns.columns())).containsExactly(toColumn("a", HIVE_DOUBLE));
    }

    @Test
    public void testSimpleSparse()
    {
        ImmutableMap<String, String> options = ImmutableMap.of(CsvOptions.SAMPLE_LINES_MODULO, "1");
        DiscoveredColumns schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/simple-sparse.csv"), options);
        assertThat(withoutSamples(schemaColumns.columns())).containsExactly(toColumn("A", HIVE_INT),
                toColumn("B", HIVE_INT),
                toColumn("C", HIVE_INT),
                toColumn("D", HIVE_INT));
    }

    @Test
    public void testWithEmptyQuotedValues()
    {
        ImmutableMap<String, String> options = ImmutableMap.of(CsvOptions.SAMPLE_LINES_MODULO, "1");
        DiscoveredColumns schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/with_empty_quoted_values.csv"), options);
        assertThat(withoutSamples(schemaColumns.columns())).containsExactly(toColumn("dispatching_base", HIVE_STRING),
                toColumn("pickup_date", HIVE_TIMESTAMP),
                toColumn("affiliated_base_num", HIVE_STRING),
                toColumn("location_id", HIVE_INT),
                toColumn("year_month", HIVE_STRING));
    }

    @Test
    public void testNoHeaders()
    {
        ImmutableMap<String, String> options = ImmutableMap.of(CsvOptions.SAMPLE_LINES_MODULO, "1",
                CsvOptions.FIRST_LINE_IS_HEADERS, "false");
        DiscoveredColumns schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/no-headers.csv"), options);
        assertThat(withoutSamples(schemaColumns.columns())).containsExactly(toColumn("COL1", HIVE_INT),
                toColumn("COL2", HIVE_INT),
                toColumn("COL3", HIVE_INT),
                toColumn("COL4", HIVE_INT));
    }

    @Test
    public void testComments()
    {
        ImmutableMap<String, String> options = ImmutableMap.of(CsvOptions.SAMPLE_LINES_MODULO, "1",
                CsvOptions.FIRST_LINE_IS_HEADERS, "false",
                CsvOptions.COMMENT, "~",
                CsvOptions.TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss");
        DiscoveredColumns schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/comments.csv"), options);
        assertThat(withoutSamples(schemaColumns.columns())).containsExactly(toColumn("COL1", HIVE_INT),
                toColumn("COL2", HIVE_INT),
                toColumn("COL3", HIVE_INT),
                toColumn("COL4", HIVE_INT),
                toColumn("COL5", HIVE_DOUBLE),
                toColumn("COL6", HIVE_TIMESTAMP));
        assertThat(schemaColumns.flags()).isEmpty();
    }

    @Test
    public void testCommentsWhitespaces()
    {
        ImmutableMap<String, String> options = ImmutableMap.of(CsvOptions.SAMPLE_LINES_MODULO, "1",
                CsvOptions.IGNORE_LEADING_WHITE_SPACE, "true",
                CsvOptions.IGNORE_TRAILING_WHITE_SPACE, "true");
        DiscoveredColumns schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/comments-whitespaces.csv"), options);
        assertThat(withoutSamples(schemaColumns.columns())).containsExactly(toColumn("colA", HIVE_STRING));
        assertThat(schemaColumns.flags()).isEmpty();
    }

    @Test
    public void testCtrlA()
    {
        Map<String, String> options = new HashMap<>(CsvOptions.controlA());
        options.put(CsvOptions.SAMPLE_LINES_MODULO, "1");
        options.put(CsvOptions.FIRST_LINE_IS_HEADERS, "false");
        DiscoveredColumns schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/ctrl-a.csv"), options);
        assertThat(withoutSamples(schemaColumns.columns())).containsExactly(toColumn("COL1", HIVE_STRING),
                toColumn("COL2", HIVE_INT),
                toColumn("COL3", HIVE_INT),
                toColumn("COL4", HIVE_INT),
                toColumn("COL5", HIVE_INT),
                toColumn("COL6", HIVE_DOUBLE),
                toColumn("COL7", HIVE_DOUBLE),
                toColumn("COL8", HIVE_BOOLEAN),
                toColumn("COL9", HIVE_TIMESTAMP),
                toColumn("COL10", HIVE_STRING),
                toColumn("COL11", HIVE_DATE),
                toColumn("COL12", HIVE_STRING),
                toColumn("COL13", HIVE_STRING),
                toColumn("COL14", mapType(HIVE_STRING, HIVE_STRING)),
                toColumn("COL15", arrayType(HIVE_STRING)),
                toColumn("COL16", HIVE_TIMESTAMP),
                toColumn("COL17", arrayType(mapType(HIVE_STRING, HIVE_DOUBLE))),
                toColumn("COL18", arrayType(HIVE_STRING)),
                toColumn("COL19", mapType(HIVE_INT, arrayType(mapType(HIVE_STRING, HIVE_DOUBLE)))));
    }

    @Test
    public void testCsvWithBigLines()
            throws Exception
    {
        // first check if our 2 lines in sample is in fact bigger than guess buffer, so if buffer ever changes, we need to adjust test data
        List<String> bigCsv = Resources.readLines(Resources.getResource("csv/big_lines.csv"), UTF_8);
        assertThat(
                bigCsv.stream().map(csv -> csv.getBytes(UTF_8).length).limit(2).reduce(Integer::sum))
                .isPresent()
                .get()
                .matches(csvSizeBytes -> csvSizeBytes > MAX_GUESS_BUFFER_SIZE, "Bigger than MAX_GUESS_BUFFER_SIZE: [%d]".formatted(MAX_GUESS_BUFFER_SIZE));

        ImmutableMap<String, String> options = ImmutableMap.of(CsvOptions.SAMPLE_LINES_MODULO, "1");
        DiscoveredColumns schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/big_lines.csv"), options);
        assertThat(schemaColumns.columns()).hasSize(58);
    }

    @Test
    public void testSingleBigLineCsv()
            throws Exception
    {
        // first check if our big line in sample is in fact bigger than guess buffer, so if buffer ever changes, we need to adjust test data
        List<String> bigCsv = Resources.readLines(Resources.getResource("csv/big_single_line.csv"), UTF_8);
        assertThat(
                bigCsv.stream().map(csv -> csv.getBytes(UTF_8).length).limit(1).reduce(Integer::sum))
                .isPresent()
                .get()
                .matches(csvSizeBytes -> csvSizeBytes > MAX_GUESS_BUFFER_SIZE, "Bigger than MAX_GUESS_BUFFER_SIZE: [%d]".formatted(MAX_GUESS_BUFFER_SIZE));

        ImmutableMap<String, String> options = ImmutableMap.of(CsvOptions.SAMPLE_LINES_MODULO, "1");
        DiscoveredColumns schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/big_single_line.csv"), options);
        assertThat(schemaColumns.columns())
                .hasSize(182)
                .allMatch(column -> column.type().typeInfo().equals(HIVE_STRING));
    }

    @Test
    public void testSingleBigLineCsvSkippingHeaders()
            throws Exception
    {
        // first check if our big line in sample is in fact bigger than guess buffer, so if buffer ever changes, we need to adjust test data
        List<String> bigCsv = Resources.readLines(Resources.getResource("csv/big_single_line.csv"), UTF_8);
        assertThat(
                bigCsv.stream().map(csv -> csv.getBytes(UTF_8).length).limit(1).reduce(Integer::sum))
                .isPresent()
                .get()
                .matches(csvSizeBytes -> csvSizeBytes > MAX_GUESS_BUFFER_SIZE, "Bigger than MAX_GUESS_BUFFER_SIZE: [%d]".formatted(MAX_GUESS_BUFFER_SIZE));

        ImmutableMap<String, String> options = ImmutableMap.of(CsvOptions.SAMPLE_LINES_MODULO, "1", CsvOptions.FIRST_LINE_IS_HEADERS, "false");
        DiscoveredColumns schemaColumns = CsvSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("csv/big_single_line.csv"), options);
        assertThat(schemaColumns.columns())
                .hasSize(182)
                .allMatch(column -> column.type().typeInfo().equals(HIVE_INT) || column.type().typeInfo().equals(HIVE_LONG));
    }
}
