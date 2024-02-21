/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.options;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.starburst.schema.discovery.formats.csv.CsvOptions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class CommaDelimitedOptionsParserTest
{
    static final String OPTIONS_WITHOUT_DELIMITER = """
            dateFormat=yyyy-MM-dd,timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],headers=true,quote=\"""";

    static final String OPTIONS_WITH_COMMA_DELIMITER_START = """
            delimiter=,,dateFormat=yyyy-MM-dd,quote=",timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],headers=true""";

    static final String OPTIONS_WITH_COMMA_DELIMITER_MIDDLE = """
            dateFormat=yyyy-MM-dd,delimiter=,,timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],headers=true,quote=\"""";

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
    static final String OPTIONS_COMPLEX = """
            dateFormat=yyyy-MM-dd,excludePatterns=**/csv/{coalesce,coalesce-mismatch}/*,timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],s1.t1.delimiter=,,quote=",headers=true,s2.table_2.dateFormat=dd-MM-yyyy,schema_3.table-3.excludePatterns=**/csv/{coalesce,coalesce-mismatch}/*|**/orc/{coalesce,coalesce-mismatch}/*,schema_3.tablejson.forceTableFormat=JSON""";
    static final String OPTIONS_WITH_MULTIPLE_OVERWRITES = """
            s1.t1.delimiter=,,delimiter=,,timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],dateFormat=yyyy-MM-dd,s1.t2.quote=",headers=false,s1.t1.dateFormat=dd-MM-yyyy,quote=',s1.t1.headers=true,s1.t2.delimiter=,,s1.t1.quote=`,s1.t2.headers=true""";

    static final String OPTIONS_WITH_SCHEMA_NAME_AS_OPTION_NAME = "timestampFormat=yyyy-MM-dd HH:mm:ss[.SSSSSSS],delimiter.cars_vertical.delimiter=|,dateFormat=yyyy-MM-dd,dateFormat.dateFormat.dateFormat=dd-MM-yyyy,headers=true,quote=\"";

    static final Map<String, String> SHARED_OPTIONS = ImmutableMap.of(
            "dateFormat", "yyyy-MM-dd",
            "timestampFormat", "yyyy-MM-dd HH:mm:ss[.SSSSSSS]",
            "headers", "true",
            "quote", "\"");

    @Test
    public void testCase1()
    {
        testParsing(OPTIONS_WITHOUT_DELIMITER, ImmutableMap.of());
    }

    @Test
    public void testCase2()
    {
        ImmutableMap<String, String> expected = ImmutableMap.of(
                "delimiter", ",");
        testParsing(OPTIONS_WITH_COMMA_DELIMITER_START, expected);
    }

    @Test
    public void testCase3()
    {
        ImmutableMap<String, String> expected = ImmutableMap.of(
                "delimiter", ",");
        testParsing(OPTIONS_WITH_COMMA_DELIMITER_MIDDLE, expected);
    }

    @Test
    public void testCase4()
    {
        ImmutableMap<String, String> expected = ImmutableMap.of(
                "delimiter", ",");
        testParsing(OPTIONS_WITH_COMMA_DELIMITER_LAST, expected);
    }

    @Test
    public void testCase5()
    {
        ImmutableMap<String, String> expected = ImmutableMap.of(
                "delimiter", " ");
        testParsing(OPTIONS_WITH_SPACE_DELIMITER_START, expected);
    }

    @Test
    public void testCase6()
    {
        ImmutableMap<String, String> expected = ImmutableMap.of(
                "delimiter", " ");
        testParsing(OPTIONS_WITH_SPACE_DELIMITER_MIDDLE, expected);
    }

    @Test
    public void testCase7()
    {
        ImmutableMap<String, String> expected = ImmutableMap.of(
                "delimiter", " ");
        testParsing(OPTIONS_WITH_SPACE_DELIMITER_LAST, expected);
    }

    @Test
    public void testCase8()
    {
        ImmutableMap<String, String> expected = ImmutableMap.of(
                "delimiter", "\t");
        testParsing(OPTIONS_WITH_TAB_DELIMITER_START, expected);
    }

    @Test
    public void testCase9()
    {
        ImmutableMap<String, String> expected = ImmutableMap.of(
                "delimiter", "\t");
        testParsing(OPTIONS_WITH_TAB_DELIMITER_MIDDLE, expected);
    }

    @Test
    public void testCase10()
    {
        ImmutableMap<String, String> expected = ImmutableMap.of(
                "delimiter", "\t");
        testParsing(OPTIONS_WITH_TAB_DELIMITER_LAST, expected);
    }

    @Test
    public void testCase11()
    {
        ImmutableMap<String, String> expected = ImmutableMap.of(
                "delimiter", "|");
        testParsing(OPTIONS_WITH_HORIZONTAL_DELIMITER_START, expected);
    }

    @Test
    public void testCase12()
    {
        ImmutableMap<String, String> expected = ImmutableMap.of(
                "delimiter", "|");
        testParsing(OPTIONS_WITH_HORIZONTAL_DELIMITER_MIDDLE, expected);
    }

    @Test
    public void testCase13()
    {
        ImmutableMap<String, String> expected = ImmutableMap.of(
                "delimiter", "|");
        testParsing(OPTIONS_WITH_HORIZONTAL_DELIMITER_LAST, expected);
    }

    @Test
    public void testCase14()
    {
        ImmutableMap<String, String> expected = ImmutableMap.of(
                "s1.t1.delimiter", ",");
        testParsing(OPTIONS_WITH_TABLE_COMMA_DELIMITER_START, expected);
    }

    @Test
    public void testCase15()
    {
        ImmutableMap<String, String> expected = ImmutableMap.of(
                "s1.t1.delimiter", ",");
        testParsing(OPTIONS_WITH_TABLE_COMMA_DELIMITER_LAST, expected);
    }

    @Test
    public void testCase16()
    {
        ImmutableMap<String, String> expected = ImmutableMap.of(
                "s1.t1.delimiter", ",");
        testParsing(OPTIONS_WITH_TABLE_COMMA_DELIMITER_MIDDLE, expected);
    }

    @Test
    public void testCase17()
    {
        ImmutableMap<String, String> expected = ImmutableMap.of(
                "s1.t1.delimiter", ",",
                "excludePatterns", "**/csv/{coalesce,coalesce-mismatch}/*");
        testParsing(OPTIONS_WITH_PATTERNS_INCLUDING_COMMA_MIDDLE, expected);
    }

    @Test
    public void testCase18()
    {
        ImmutableMap<String, String> expected = ImmutableMap.of(
                "s1.t1.delimiter", ",",
                "excludePatterns", "**/csv/{coalesce,coalesce-mismatch}/*");
        testParsing(OPTIONS_WITH_PATTERNS_INCLUDING_COMMA_START, expected);
    }

    @Test
    public void testCase19()
    {
        ImmutableMap<String, String> expected = ImmutableMap.of(
                "s1.t1.delimiter", ",",
                "excludePatterns", "**/csv/{coalesce,coalesce-mismatch}/*");
        testParsing(OPTIONS_WITH_PATTERNS_INCLUDING_COMMA_LAST, expected);
    }

    @Test
    public void testCase20()
    {
        ImmutableMap<String, String> expected = ImmutableMap.of(
                "s1.t1.delimiter", ",",
                "excludePatterns", "**/csv/{coalesce,coalesce-mismatch}/*",
                "s2.table_2.dateFormat", "dd-MM-yyyy",
                "schema_3.table-3.excludePatterns", "**/csv/{coalesce,coalesce-mismatch}/*|**/orc/{coalesce,coalesce-mismatch}/*",
                "schema_3.tablejson.forceTableFormat", "JSON");
        testParsing(OPTIONS_COMPLEX, expected);
    }

    @Test
    public void testCase21()
    {
        ImmutableMap<String, String> expected = ImmutableMap.<String, String>builder()
                .put("s1.t1.delimiter", ",")
                .put("delimiter", ",")
                .put("dateFormat", "yyyy-MM-dd")
                .put("s1.t2.quote", "\"")
                .put("headers", "false")
                .put("s1.t1.dateFormat", "dd-MM-yyyy")
                .put("quote", "'")
                .put("s1.t1.headers", "true")
                .put("s1.t2.delimiter", ",")
                .put("s1.t1.quote", "`")
                .put("s1.t2.headers", "true")
                .buildOrThrow();
        testParsing(OPTIONS_WITH_MULTIPLE_OVERWRITES, expected);
    }

    @Test
    public void testCase22()
    {
        Map<String, String> expected = ImmutableMap.of(
                "delimiter.cars_vertical.delimiter", "|",
                "dateFormat.dateFormat.dateFormat", "dd-MM-yyyy");
        testParsing(OPTIONS_WITH_SCHEMA_NAME_AS_OPTION_NAME, expected);
    }

    public void testParsing(String optionsString, Map<String, String> expectedWithoutShared)
    {
        CommaDelimitedOptionsParser parser = new CommaDelimitedOptionsParser(ImmutableList.of(GeneralOptions.class, CsvOptions.class));
        Map<String, String> expected = ImmutableMap.<String, String>builder()
                .putAll(SHARED_OPTIONS)
                .putAll(expectedWithoutShared)
                .buildKeepingLast();

        assertThat(parser.parse(optionsString))
                .containsExactlyInAnyOrderEntriesOf(expected);
    }
}
