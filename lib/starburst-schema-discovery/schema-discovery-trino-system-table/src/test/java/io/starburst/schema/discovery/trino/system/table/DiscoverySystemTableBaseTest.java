/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.trino.system.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.starburst.schema.discovery.SchemaDiscoveryController;
import io.starburst.schema.discovery.SchemaExplorer;
import io.starburst.schema.discovery.formats.csv.CsvOptions;
import io.starburst.schema.discovery.options.CommaDelimitedOptionsParser;
import io.starburst.schema.discovery.options.GeneralOptions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static io.starburst.schema.discovery.formats.csv.CsvOptions.DELIMITER;
import static io.starburst.schema.discovery.generation.Dialect.TRINO;
import static org.assertj.core.api.Assertions.assertThat;

public class DiscoverySystemTableBaseTest
{
    static final String OPTIONS_WITHOUT_DELIMITER = """
            dateFormat=yyyy-MM-dd,timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],headers=true,quote=\"""";

    static final String OPTIONS_WITH_COMMA_DELIMITER_START = """
            delimiter=,,dateFormat=yyyy-MM-dd,quote=",timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],headers=true""";

    static final String OPTIONS_WITH_COMMA_DELIMITER_MIDDLE = """
            dateFormat=yyyy-MM-dd,timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],delimiter=,,headers=true,quote=\"""";

    static final String OPTIONS_WITH_COMMA_DELIMITER_LAST = """
            dateFormat=yyyy-MM-dd,timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],headers=true,quote=",delimiter=,""";

    static final String OPTIONS_WITH_SPACE_DELIMITER_START = """
            delimiter= ,dateFormat=yyyy-MM-dd,quote=",timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],headers=true""";

    static final String OPTIONS_WITH_SPACE_DELIMITER_MIDDLE = """
            dateFormat=yyyy-MM-dd,timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],quote=",delimiter= ,headers=true""";

    static final String OPTIONS_WITH_SPACE_DELIMITER_LAST = "dateFormat=yyyy-MM-dd,timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],headers=true,quote=\",delimiter= ";

    static final String OPTIONS_WITH_TAB_DELIMITER_START = """
            delimiter=\t,dateFormat=yyyy-MM-dd,quote=",timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],headers=true""";

    static final String OPTIONS_WITH_TAB_DELIMITER_MIDDLE = """
            dateFormat=yyyy-MM-dd,timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],quote=",delimiter=\t,headers=true""";

    static final String OPTIONS_WITH_TAB_DELIMITER_LAST = "dateFormat=yyyy-MM-dd,timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],headers=true,quote=\",delimiter=\t";

    static final String OPTIONS_WITH_HORIZONTAL_DELIMITER_START = """
            delimiter=|,dateFormat=yyyy-MM-dd,quote=",timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],headers=true""";

    static final String OPTIONS_WITH_HORIZONTAL_DELIMITER_MIDDLE = """
            dateFormat=yyyy-MM-dd,timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],quote=",delimiter=|,headers=true""";

    static final String OPTIONS_WITH_HORIZONTAL_DELIMITER_LAST = "dateFormat=yyyy-MM-dd,timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],headers=true,quote=\",delimiter=|";

    static final String OPTIONS_WITH_TABLE_COMMA_DELIMITER_START = """
            s1.t1.delimiter=,,dateFormat=yyyy-MM-dd,timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],quote=",headers=true""";

    static final String OPTIONS_WITH_TABLE_COMMA_DELIMITER_LAST = """
            dateFormat=yyyy-MM-dd,timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],headers=true,quote=",s1.t1.delimiter=,""";
    static final String OPTIONS_WITH_TABLE_COMMA_DELIMITER_MIDDLE = """
            dateFormat=yyyy-MM-dd,timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],s1.t1.delimiter=,,quote=",headers=true""";
    static final String OPTIONS_WITH_PATTERNS_INCLUDING_COMMA_MIDDLE = """
            dateFormat=yyyy-MM-dd,excludePatterns=**/csv/{coalesce,coalesce-mismatch}/*,timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],s1.t1.delimiter=,,quote=",headers=true""";
    static final String OPTIONS_WITH_PATTERNS_INCLUDING_COMMA_START = """
            excludePatterns=**/csv/{coalesce,coalesce-mismatch}/*,dateFormat=yyyy-MM-dd,timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],s1.t1.delimiter=,,quote=",headers=true""";
    static final String OPTIONS_WITH_PATTERNS_INCLUDING_COMMA_LAST = """
            dateFormat=yyyy-MM-dd,timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],s1.t1.delimiter=,,quote=",headers=true,excludePatterns=**/csv/{coalesce,coalesce-mismatch}/*""";

    static final Map<String, String> SHARED_OPTIONS = ImmutableMap.of(
            "dateFormat", "yyyy-MM-dd",
            "timestampFormat", "yyyy-MM-dd HH:mm:ss[.SSSSSSS]",
            "headers", "true",
            "quote", "\"");

    @Test
    public void testBuildOptions()
    {
        testOptionsString(OPTIONS_WITHOUT_DELIMITER, Map.entry(DELIMITER, ","));

        testOptionsString(OPTIONS_WITH_COMMA_DELIMITER_LAST, Map.entry(DELIMITER, ","));
        testOptionsString(OPTIONS_WITH_COMMA_DELIMITER_MIDDLE, Map.entry(DELIMITER, ","));
        testOptionsString(OPTIONS_WITH_COMMA_DELIMITER_START, Map.entry(DELIMITER, ","));

        testOptionsString(OPTIONS_WITH_SPACE_DELIMITER_START, Map.entry(DELIMITER, " "));
        testOptionsString(OPTIONS_WITH_SPACE_DELIMITER_MIDDLE, Map.entry(DELIMITER, " "));
        testOptionsString(OPTIONS_WITH_SPACE_DELIMITER_LAST, Map.entry(DELIMITER, " "));

        testOptionsString(OPTIONS_WITH_TAB_DELIMITER_START, Map.entry(DELIMITER, "\t"));
        testOptionsString(OPTIONS_WITH_TAB_DELIMITER_MIDDLE, Map.entry(DELIMITER, "\t"));
        testOptionsString(OPTIONS_WITH_TAB_DELIMITER_LAST, Map.entry(DELIMITER, "\t"));

        testOptionsString(OPTIONS_WITH_HORIZONTAL_DELIMITER_START, Map.entry(DELIMITER, "|"));
        testOptionsString(OPTIONS_WITH_HORIZONTAL_DELIMITER_MIDDLE, Map.entry(DELIMITER, "|"));
        testOptionsString(OPTIONS_WITH_HORIZONTAL_DELIMITER_LAST, Map.entry(DELIMITER, "|"));

        testOptionsString(OPTIONS_WITH_TABLE_COMMA_DELIMITER_LAST, Map.entry("s1.t1." + DELIMITER, ","));
        testOptionsString(OPTIONS_WITH_TABLE_COMMA_DELIMITER_MIDDLE, Map.entry("s1.t1." + DELIMITER, ","));
        testOptionsString(OPTIONS_WITH_TABLE_COMMA_DELIMITER_START, Map.entry("s1.t1." + DELIMITER, ","));

        testOptionsString(OPTIONS_WITH_PATTERNS_INCLUDING_COMMA_START, ImmutableMap.of(
                "s1.t1." + DELIMITER, ",",
                "excludePatterns", "**/csv/{coalesce,coalesce-mismatch}/*"));
        testOptionsString(OPTIONS_WITH_PATTERNS_INCLUDING_COMMA_MIDDLE, ImmutableMap.of(
                "s1.t1." + DELIMITER, ",",
                "excludePatterns", "**/csv/{coalesce,coalesce-mismatch}/*"));
        testOptionsString(OPTIONS_WITH_PATTERNS_INCLUDING_COMMA_LAST, ImmutableMap.of(
                "s1.t1." + DELIMITER, ",",
                "excludePatterns", "**/csv/{coalesce,coalesce-mismatch}/*"));
    }

    private void testOptionsString(String optionsString, Map.Entry<String, String> expectedEntry)
    {
        testOptionsString(optionsString, ImmutableMap.ofEntries(expectedEntry));
    }

    private void testOptionsString(String optionsString, Map<String, String> expectedMap)
    {
        Map<String, String> expected = new HashMap<>(SHARED_OPTIONS);
        expected.putAll(CsvOptions.standard());
        expected.putAll(expectedMap);

        SchemaExplorer schemaExplorer = new SchemaExplorer(
                new SchemaDiscoveryController(__ -> null, __ -> null, (ignore1, ignore2, ignore3, ignore4) -> null, TRINO),
                new ObjectMapper(),
                new CommaDelimitedOptionsParser(ImmutableList.of(GeneralOptions.class, CsvOptions.class)));
        Map<String, String> actual = schemaExplorer.buildOptions(optionsString);

        assertThat(actual).isEqualTo(expected);
    }
}
