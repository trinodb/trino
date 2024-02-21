/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.formats.csv;

import com.google.common.collect.ImmutableList;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import io.starburst.schema.discovery.internal.LineStreamSampler;
import io.starburst.schema.discovery.io.DiscoveryInput;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;

class CsvStreamSampler
        implements AutoCloseable
{
    private final List<String> columnNames;
    private final CsvParser csvParser;
    private final LineStreamSampler<List<String>> lineStreamSampler;
    private final CsvOptions options;
    private final boolean fieldsAreQuoted;
    private String[] firstLine;
    private String[] examples;

    CsvStreamSampler(CsvOptions options, DiscoveryInput in)
    {
        this.options = options;

        CsvParserSettings csvSettings = new CsvParserSettings();
        CsvFormat format = csvSettings.getFormat();
        if (options.lineSeparator().isEmpty()) {
            csvSettings.setLineSeparatorDetectionEnabled(true);
        }
        else {
            csvSettings.setLineSeparatorDetectionEnabled(false);
            format.setLineSeparator(options.lineSeparator());
        }
        csvSettings.setHeaderExtractionEnabled(options.firstLineIsHeaders());
        csvSettings.setNullValue(options.nullValue());
        csvSettings.setIgnoreLeadingWhitespaces(options.ignoreLeadingWhiteSpace());
        csvSettings.setIgnoreTrailingWhitespaces(options.ignoreTrailingWhiteSpace());
        csvSettings.setEmptyValue("");
        format.setDelimiter(options.delimiter());
        format.setQuote(options.quote());
        format.setQuoteEscape(options.escape());
        format.setComment(options.comment());

        csvParser = new CsvParser(csvSettings);
        fieldsAreQuoted = CheckCsvFormat.hasQuotedFields(in, format);
        csvParser.beginParsing(in.asInputStream(), options.charset());

        if (options.firstLineIsHeaders()) {
            columnNames = cleanHeaders(csvParser.getRecordMetadata().headers());
            firstLine = null;
        }
        else {
            firstLine = csvParser.parseNext();
            columnNames = headersFromFirstLine(firstLine);
        }
        lineStreamSampler = new LineStreamSampler<>(options.maxSampleLines(), options.sampleLinesModulo(), this::nextLine);
    }

    @Override
    public void close()
    {
        csvParser.stopParsing();
    }

    boolean fieldsAreQuoted()
    {
        return fieldsAreQuoted;
    }

    List<String> getColumnNames()
    {
        return columnNames;
    }

    List<String> examples()
    {
        return (examples != null) ? ImmutableList.copyOf(examples) : ImmutableList.of();
    }

    Stream<List<String>> stream()
    {
        return lineStreamSampler.stream();
    }

    private Optional<List<String>> nextLine()
    {
        String[] line;
        if (firstLine != null) {
            line = firstLine;
            firstLine = null;
        }
        else {
            line = csvParser.parseNext();
            if (examples == null) {
                examples = line;
            }
        }
        if (line == null) {
            return Optional.empty();
        }
        return Optional.of(Arrays.asList(line));    // Arrays.asList allows null values;
    }

    private List<String> cleanHeaders(String[] headers)
    {
        if ((headers == null) || (headers.length == 0)) {
            return ImmutableList.of();
        }
        return Arrays.stream(headers).map(s -> ((s != null) && !s.isEmpty() ? s : "_")).collect(toImmutableList());
    }

    private List<String> headersFromFirstLine(String[] firstLine)
    {
        ImmutableList.Builder<String> builder = ImmutableList.<String>builder();
        if (firstLine != null) {
            for (int i = 1; i <= firstLine.length; ++i) {
                builder.add(String.format(options.generatedHeadersFormat(), i));
            }
        }
        return builder.build();
    }
}
