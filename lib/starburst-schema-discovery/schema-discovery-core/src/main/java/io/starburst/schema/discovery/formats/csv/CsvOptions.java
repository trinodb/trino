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

import com.google.common.collect.ImmutableMap;
import io.starburst.schema.discovery.infer.OptionDescription;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.starburst.schema.discovery.options.OptionsMap;

import java.util.Map;

public class CsvOptions
        extends GeneralOptions
{
    @OptionDescription("If \"true\" treat first line as columns names")
    public static final String FIRST_LINE_IS_HEADERS = "headers";

    @OptionDescription("Pattern to use if column names are auto-generated")
    public static final String GENERATED_HEADERS_FORMAT = "generatedHeadersFormat";

    @OptionDescription("Text file delimiter")
    public static final String DELIMITER = "delimiter";

    @OptionDescription("Text file quote")
    public static final String QUOTE = "quote";

    @OptionDescription("Text file escape")
    public static final String ESCAPE = "escape";

    @OptionDescription("Text file comment")
    public static final String COMMENT = "comment";

    @OptionDescription("Text file null value")
    public static final String NULL_VALUE = "nullValue";

    @OptionDescription("If \"true\" ignore leading white space")
    public static final String IGNORE_LEADING_WHITE_SPACE = "ignoreLeadingWhiteSpace";

    @OptionDescription("If \"true\" ignore trailing white space")
    public static final String IGNORE_TRAILING_WHITE_SPACE = "ignoreTrailingWhiteSpace";

    @OptionDescription("If \"true\" try to discover arrays, structs and maps")
    public static final String CHECK_COMPLEX_HADOOP_TYPES = "complexHadoop";

    @OptionDescription("Text line separator")
    public static final String LINE_SEPARATOR = "lineSeparator";

    public static final String COMMA_DELIMITER = ",";

    public static Map<String, String> standard()
    {
        Map<String, String> options = withDefaults().put(DELIMITER, COMMA_DELIMITER)
                .put(NULL_VALUE, "")
                .put(CHECK_COMPLEX_HADOOP_TYPES, "false")
                .buildOrThrow();
        return GeneralOptions.combinedDefaults(options);
    }

    public static Map<String, String> controlA()
    {
        Map<String, String> options = withDefaults().put(DELIMITER, "\u0001")
                .put(NULL_VALUE, "\\N")
                .put(CHECK_COMPLEX_HADOOP_TYPES, "true")
                .buildOrThrow();
        return GeneralOptions.combinedDefaults(options);
    }

    public static Map<String, String> tab()
    {
        Map<String, String> options = withDefaults().put(DELIMITER, "\t")
                .put(NULL_VALUE, "")
                .put(CHECK_COMPLEX_HADOOP_TYPES, "false")
                .buildOrThrow();
        return GeneralOptions.combinedDefaults(options);
    }

    public static Map<String, String> pipe()
    {
        Map<String, String> options = withDefaults().put(DELIMITER, "|")
                .put(NULL_VALUE, "")
                .put(CHECK_COMPLEX_HADOOP_TYPES, "false")
                .buildOrThrow();
        return GeneralOptions.combinedDefaults(options);
    }

    private static ImmutableMap.Builder<String, String> withDefaults()
    {
        return ImmutableMap.<String, String>builder()
                .put(LINE_SEPARATOR, "")
                .put(FIRST_LINE_IS_HEADERS, "true")
                .put(QUOTE, "\"")
                .put(ESCAPE, "\\")
                .put(COMMENT, "#")
                .put(IGNORE_LEADING_WHITE_SPACE, "false")
                .put(IGNORE_TRAILING_WHITE_SPACE, "false")
                .put(GENERATED_HEADERS_FORMAT, "COL%d");
    }

    public CsvOptions(OptionsMap options)
    {
        super(options, standard());
    }

    public String lineSeparator()
    {
        return options.get(LINE_SEPARATOR);
    }

    public boolean firstLineIsHeaders()
    {
        return options.bool(FIRST_LINE_IS_HEADERS);
    }

    public String delimiter()
    {
        return options.get(DELIMITER);
    }

    public char quote()
    {
        return options.firstChar(QUOTE);
    }

    public char escape()
    {
        return options.firstChar(ESCAPE);
    }

    public char comment()
    {
        return options.firstChar(COMMENT);
    }

    public String nullValue()
    {
        return options.get(NULL_VALUE);
    }

    public boolean ignoreLeadingWhiteSpace()
    {
        return options.bool(IGNORE_LEADING_WHITE_SPACE);
    }

    public boolean ignoreTrailingWhiteSpace()
    {
        return options.bool(IGNORE_TRAILING_WHITE_SPACE);
    }

    public boolean checkComplexHadoopTypes()
    {
        return options.bool(CHECK_COMPLEX_HADOOP_TYPES);
    }

    public String generatedHeadersFormat()
    {
        return options.get(GENERATED_HEADERS_FORMAT);
    }
}
