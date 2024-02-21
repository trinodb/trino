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

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableMap;
import com.univocity.parsers.csv.CsvFormat;
import io.starburst.schema.discovery.internal.FormatGuess;
import io.starburst.schema.discovery.internal.TextFileLines;
import io.starburst.schema.discovery.io.DiscoveryInput;

import java.util.Map;
import java.util.Optional;

import static io.starburst.schema.discovery.formats.csv.CsvOptions.COMMA_DELIMITER;
import static io.starburst.schema.discovery.internal.TextFileLines.TestLinesMode.ALL_MATCH;
import static io.starburst.schema.discovery.internal.TextFileLines.TestLinesMode.ANY_MATCH;

class CheckCsvFormat
{
    private static final Map<String, FormatGuess> DELIMITED = ImmutableMap.<String, FormatGuess>builder()
            .put(COMMA_DELIMITER, match(CsvOptions.standard()))
            .put("\1", match(CsvOptions.controlA()))
            .put("\t", match(CsvOptions.tab()))
            .put("|", match(CsvOptions.pipe()))
            .buildOrThrow();

    static boolean hasQuotedFields(DiscoveryInput in, CsvFormat format)
    {
        return TextFileLines.getTestBytes(in).stream()
                .allMatch(bytes -> TextFileLines.testLines(bytes, ANY_MATCH, line -> hasQuotedFields(line, format)));
    }

    static Optional<FormatGuess> checkFormatMatch(DiscoveryInput inputStream)
    {
        Optional<byte[]> testBytes = TextFileLines.getTestBytes(inputStream);
        if (testBytes.isPresent()) {
            for (Map.Entry<String, FormatGuess> entry : DELIMITED.entrySet()) {
                if (TextFileLines.testLines(testBytes.get(), ALL_MATCH, s -> isDelimitedOrComment(entry.getKey(), s))) {
                    return Optional.of(entry.getValue());
                }
            }
            if (TextFileLines.testLines(testBytes.get(), ALL_MATCH, CheckCsvFormat::allAscii)) {
                return Optional.of(DELIMITED.get(COMMA_DELIMITER)); // it's all ASCII or spaces - probably a TEXT file of some kind
            }
        }
        return Optional.empty();
    }

    static Optional<FormatGuess> checkFormatMatch(byte[] bytes)
    {
        for (Map.Entry<String, FormatGuess> entry : DELIMITED.entrySet()) {
            if (TextFileLines.testLines(bytes, ALL_MATCH, s -> isDelimitedOrComment(entry.getKey(), s))) {
                return Optional.of(entry.getValue());
            }
        }
        return Optional.empty();
    }

    private static boolean allAscii(String s)
    {
        return CharMatcher.ascii().matchesAllOf(s);
    }

    private static boolean hasQuotedFields(String line, CsvFormat format)
    {
        boolean fieldStarted = false;
        boolean hasQuoted = false;
        boolean inQuote = false;
        boolean escapeNext = false;
        boolean commentChecked = false;
        for (char c : line.toCharArray()) {
            if (!commentChecked) {
                commentChecked = true;
                if (c == format.getComment()) {
                    break;
                }
            }

            if (escapeNext) {
                if (c == format.getQuote()) {
                    escapeNext = false;
                    continue;
                }
                inQuote = false;
                hasQuoted = true;
                break;
            }
            if (inQuote) {
                if (c == format.getQuoteEscape()) {
                    escapeNext = true;
                }
                else if (c == format.getQuote()) {
                    inQuote = false;
                    hasQuoted = true;
                    break;
                }
            }
            else if (fieldStarted) {
                if (c == format.getDelimiter()) {
                    fieldStarted = false;
                }
            }
            else {
                if (c == format.getQuote()) {
                    inQuote = true;
                }
                else {
                    fieldStarted = true;
                }
            }
        }

        if (inQuote && escapeNext && (format.getQuote() == format.getQuoteEscape())) {
            return true;    // quote-quote is an escaped quote but we saw a single quote at the end of line
        }

        return hasQuoted;
    }

    private static boolean isDelimitedOrComment(String delimiter, String s)
    {
        return s.contains(delimiter) || s.startsWith("#");
    }

    private static FormatGuess match(Map<String, String> options)
    {
        return new FormatGuess()
        {
            @Override
            public Map<String, String> options()
            {
                return options;
            }

            @Override
            public Confidence confidence()
            {
                return Confidence.LOW;
            }
        };
    }

    private CheckCsvFormat()
    {
    }
}
