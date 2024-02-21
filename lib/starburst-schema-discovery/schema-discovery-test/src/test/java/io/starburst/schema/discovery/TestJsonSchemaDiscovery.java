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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.starburst.schema.discovery.formats.json.JsonSchemaDiscovery;
import io.starburst.schema.discovery.internal.HiveType;
import io.starburst.schema.discovery.models.DiscoveredColumns;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.trino.plugin.hive.type.DecimalTypeInfo;
import io.trino.plugin.hive.type.TypeInfo;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.starburst.schema.discovery.Util.toColumn;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_BOOLEAN;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_DOUBLE;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_INT;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_LONG;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_STRING;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_TIMESTAMP;
import static io.starburst.schema.discovery.internal.HiveTypes.STRING_TYPE;
import static io.starburst.schema.discovery.internal.HiveTypes.arrayType;
import static io.starburst.schema.discovery.internal.HiveTypes.structType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJsonSchemaDiscovery
{
    @Test
    public void testWithMapFields()
    {
        ImmutableMap<String, String> options = ImmutableMap.of(GeneralOptions.SAMPLE_LINES_MODULO, "1");
        DiscoveredColumns schemaColumns = JsonSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("json/with-map-fields.json"), options);
        TypeInfo internalFields = structType(ImmutableList.of("startTime", "endTime"), ImmutableList.of(HIVE_INT, HIVE_INT));
        TypeInfo fields = structType(ImmutableList.of("a", "b"), ImmutableList.of(internalFields, internalFields));
        assertThat(schemaColumns.columns()).containsExactly(toColumn("id", HIVE_INT).withSampleValue("1"), toColumn("intervals", fields));
    }

    @Test
    public void testWithArrayFields()
    {
        ImmutableMap<String, String> options = ImmutableMap.of(GeneralOptions.SAMPLE_LINES_MODULO, "1");
        DiscoveredColumns schemaColumns = JsonSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("json/with-array-fields.json"), options);
        TypeInfo internalFields = structType(ImmutableList.of("startTime", "endTime"), ImmutableList.of(HIVE_INT, HIVE_INT));
        assertThat(schemaColumns.columns()).containsExactly(toColumn("id", HIVE_INT).withSampleValue("1"),
                toColumn("intervals", arrayType(internalFields)),
                toColumn("ints", arrayType(HIVE_INT)));
    }

    @Test
    public void testDifferentNestedJsonFields()
    {
        Map<String, String> options = ImmutableMap.of(GeneralOptions.SAMPLE_LINES_MODULO, "1");
        DiscoveredColumns schemaColumns = JsonSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("json/not_always_matching_fields.json"), options);
        TypeInfo internalFields = structType(ImmutableList.of("endTime", "startTime"), ImmutableList.of(HIVE_INT, HIVE_INT));
        assertThat(schemaColumns.columns()).containsExactly(toColumn("id", HIVE_INT).withSampleValue("1"),
                toColumn("intervals", structType(ImmutableList.of("a", "b"), ImmutableList.of(internalFields, internalFields))));
    }

    @Test
    public void testTypes()
    {
        ImmutableMap<String, String> optionsInferDecimal = ImmutableMap.of(GeneralOptions.SAMPLE_LINES_MODULO, "1", GeneralOptions.INFER_JSON_STRING_TO_DECIMAL, "true");
        DiscoveredColumns schemaColumns = JsonSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("json/types.json"), optionsInferDecimal);
        assertThat(schemaColumns.columns()).containsExactly(
                toColumn("bool", HIVE_BOOLEAN).withSampleValue("true"),
                toColumn("int", HIVE_INT).withSampleValue("1234"),
                toColumn("long", HIVE_LONG).withSampleValue("21474836478"),
                toColumn("double", HIVE_DOUBLE).withSampleValue("10.1"),
                toColumn("bigdecimal", new DecimalTypeInfo(22, 11)).withSampleValue("21474836478.21474836478"),
                toColumn("string_bool", HIVE_BOOLEAN).withSampleValue("true"),
                // when it comes to string to concrete type casting in trino, primitive numeric types expect json to have pure numeric value, but decimals work with string numbers
                toColumn("string_int", new DecimalTypeInfo(3, 0)).withSampleValue("123"),
                toColumn("string_long", new DecimalTypeInfo(11, 0)).withSampleValue("21474836478"),
                toColumn("string_double", new DecimalTypeInfo(4, 2)).withSampleValue("10.10"),
                toColumn("string_bigdecimal", new DecimalTypeInfo(22, 11)).withSampleValue("21474836478.21474836478"),
                toColumn("date", HIVE_TIMESTAMP).withSampleValue("2015-08-20 15:57:00"),
                toColumn("text", HIVE_STRING).withSampleValue("blah blah blah"),
                toColumn("stringarray", arrayType(STRING_TYPE)));

        ImmutableMap<String, String> optionsNoDecimal = ImmutableMap.of(GeneralOptions.SAMPLE_LINES_MODULO, "1");
        DiscoveredColumns schemaColumnsNoDecimal = JsonSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("json/types.json"), optionsNoDecimal);
        assertThat(schemaColumnsNoDecimal.columns()).containsExactly(
                toColumn("bool", HIVE_BOOLEAN).withSampleValue("true"),
                toColumn("int", HIVE_INT).withSampleValue("1234"),
                toColumn("long", HIVE_LONG).withSampleValue("21474836478"),
                toColumn("double", HIVE_DOUBLE).withSampleValue("10.1"),
                toColumn("bigdecimal", new DecimalTypeInfo(22, 11)).withSampleValue("21474836478.21474836478"),
                toColumn("string_bool", HIVE_BOOLEAN).withSampleValue("true"),
                // string decimals should be strings by default
                toColumn("string_int", HIVE_STRING).withSampleValue("123"),
                toColumn("string_long", HIVE_STRING).withSampleValue("21474836478"),
                toColumn("string_double", HIVE_STRING).withSampleValue("10.10"),
                toColumn("string_bigdecimal", HIVE_STRING).withSampleValue("21474836478.21474836478"),
                toColumn("date", HIVE_TIMESTAMP).withSampleValue("2015-08-20 15:57:00"),
                toColumn("text", HIVE_STRING).withSampleValue("blah blah blah"),
                toColumn("stringarray", arrayType(STRING_TYPE)));
    }

    @Test
    public void testInternalWithMixedFieldsRow()
    {
        ImmutableMap<String, String> options = ImmutableMap.of(GeneralOptions.SAMPLE_LINES_MODULO, "1");
        DiscoveredColumns schemaColumns = JsonSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("json/internal_row.json"), options);

        assertThat(schemaColumns.columns())
                .containsExactly(
                        toColumn("timestamp", HIVE_STRING).withSampleValue("2023-01-01T12:34:56Z"),
                        toColumn("version", new DecimalTypeInfo(1, 0)).withSampleValue("1"),
                        toColumn("dimensions", structType(ImmutableList.of("name", "address", "height", "age"), ImmutableList.of(HIVE_STRING, HIVE_STRING, HIVE_STRING, HIVE_STRING))),
                        toColumn("name", HIVE_STRING),
                        toColumn("isalive", HIVE_BOOLEAN),
                        toColumn("age", HIVE_INT),
                        toColumn("address", structType(ImmutableList.of("city", "postalCode", "streetAddress", "state", "flat", "street", "country"), ImmutableList.of(HIVE_STRING, HIVE_STRING, HIVE_STRING, HIVE_STRING, HIVE_STRING, HIVE_STRING, HIVE_STRING))),
                        toColumn("dob", HIVE_STRING));
    }

    @Test
    public void testNumberStringsWithEdgeCaseEndings()
    {
        // each line, line by line
        ImmutableList.of("1", "2", "3", "4", "5", "6").forEach(linesModulo -> {
            ImmutableMap<String, String> options = ImmutableMap.of(GeneralOptions.SAMPLE_LINES_MODULO, linesModulo, GeneralOptions.MAX_SAMPLE_LINES, "1");
            DiscoveredColumns schemaColumns = JsonSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("json/with_edge_case_non_number_strings.json"), options);
            assertThat(schemaColumns.columns()).isNotEmpty();
            assertThat(schemaColumns.columns().get(0).type()).isEqualTo(new HiveType(HIVE_STRING));
        });

        // whole json at once
        ImmutableMap<String, String> options = ImmutableMap.of(GeneralOptions.SAMPLE_LINES_MODULO, "1");
        DiscoveredColumns schemaColumns = JsonSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("json/with_edge_case_non_number_strings.json"), options);
        assertThat(schemaColumns.columns()).isNotEmpty();
        assertThat(schemaColumns.columns().get(0).type()).isEqualTo(new HiveType(HIVE_STRING));
    }

    @Test
    public void testNumberThenString()
    {
        // first line should have int
        ImmutableMap<String, String> options = ImmutableMap.of(GeneralOptions.SAMPLE_LINES_MODULO, "1", GeneralOptions.MAX_SAMPLE_LINES, "1");
        DiscoveredColumns schemaColumns = JsonSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("json/number_then_string.json"), options);
        assertThat(schemaColumns.columns()).isNotEmpty();
        assertThat(schemaColumns.columns().get(0).type()).isEqualTo(new HiveType(HIVE_INT));

        // second line should be string so int + string = string
        ImmutableMap<String, String> optionsFull = ImmutableMap.of(GeneralOptions.SAMPLE_LINES_MODULO, "1");
        DiscoveredColumns schemaColumnsFull = JsonSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("json/number_then_string.json"), optionsFull);
        assertThat(schemaColumnsFull.columns()).isNotEmpty();
        assertThat(schemaColumnsFull.columns().get(0).type()).isEqualTo(new HiveType(HIVE_STRING));
    }
}
