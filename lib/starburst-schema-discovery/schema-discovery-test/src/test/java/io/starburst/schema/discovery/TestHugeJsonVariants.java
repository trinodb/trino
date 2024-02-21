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
import com.google.common.io.Resources;
import io.starburst.schema.discovery.formats.json.JsonSchemaDiscovery;
import io.starburst.schema.discovery.models.DiscoveredColumns;
import io.starburst.schema.discovery.options.GeneralOptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static io.starburst.schema.discovery.Util.toColumn;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_DATE;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_INT;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_STRING;
import static io.starburst.schema.discovery.internal.HiveTypes.arrayType;
import static io.starburst.schema.discovery.internal.HiveTypes.structType;
import static io.starburst.schema.discovery.internal.TextFileLines.MAX_GUESS_BUFFER_SIZE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHugeJsonVariants
{
    private static final Map<String, String> OPTIONS = ImmutableMap.of(GeneralOptions.SAMPLE_LINES_MODULO, "1", GeneralOptions.INFER_JSON_STRING_TO_DECIMAL, "false");

    @Test
    public void testHugeJson()
            throws IOException
    {
        // first check if our sample is in fact bigger than guess buffer, so if buffer ever changes, we need to adjust test data
        List<String> bigJsons = Resources.readLines(Resources.getResource("json/huge.json"), UTF_8);
        assertThat(
                bigJsons.stream().map(json -> json.getBytes(UTF_8).length))
                .allMatch(jsonSizeBytes -> jsonSizeBytes > MAX_GUESS_BUFFER_SIZE, "Bigger than MAX_GUESS_BUFFER_SIZE: [%d]".formatted(MAX_GUESS_BUFFER_SIZE));

        DiscoveredColumns schemaColumns = JsonSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("json/huge.json"), OPTIONS);
        assertHugeJsonColumns(schemaColumns);
    }

    @Test
    public void testHugeJsonCompressedGzip()
    {
        DiscoveredColumns schemaColumns = JsonSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("json/compressed/huge.json.gz"), OPTIONS);
        assertHugeJsonColumns(schemaColumns);
    }

    @Test
    public void testHugeJsonCompressedZstd()
    {
        DiscoveredColumns schemaColumns = JsonSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("json/compressed/huge.json.zst"), OPTIONS);
        assertHugeJsonColumns(schemaColumns);
    }

    @Test
    public void testHugeJsonCompressedLz4()
    {
        DiscoveredColumns schemaColumns = JsonSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("json/compressed/huge.json.lz4"), OPTIONS);
        assertHugeJsonColumns(schemaColumns);
    }

    @Test
    public void testHugeJsonCompressedSnappy()
            throws IOException
    {
        DiscoveredColumns schemaColumns = JsonSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("json/compressed/huge.json.snappy"), OPTIONS);
        assertHugeJsonColumns(schemaColumns);
    }

    @Test
    public void testHugeJsonCompressedDeflate()
            throws IOException
    {
        DiscoveredColumns schemaColumns = JsonSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("json/compressed/huge.json.deflate"), OPTIONS);
        assertHugeJsonColumns(schemaColumns);
    }

    @Test
    public void testHugeJsonCompressedBzip2()
            throws IOException
    {
        DiscoveredColumns schemaColumns = JsonSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("json/compressed/huge.json.bz2"), OPTIONS);
        assertHugeJsonColumns(schemaColumns);
    }

    @Test
    public void testHugeJsonCompressedLzo()
            throws IOException
    {
        DiscoveredColumns schemaColumns = JsonSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("json/compressed/huge.json.lzo"), OPTIONS);
        assertHugeJsonColumns(schemaColumns);
    }

    @Test
    public void testHugeJsonCompressedLzop()
            throws IOException
    {
        DiscoveredColumns schemaColumns = JsonSchemaDiscovery.INSTANCE.discoverColumns(Util.testFile("json/compressed/huge.json.lzo_deflate"), OPTIONS);
        assertHugeJsonColumns(schemaColumns);
    }

    private static void assertHugeJsonColumns(DiscoveredColumns schemaColumns)
    {
        assertThat(schemaColumns.columns()).containsExactly(
                toColumn("id", HIVE_INT).withSampleValue("1"),
                toColumn("first_name", HIVE_STRING).withSampleValue("John"),
                toColumn("last_name", HIVE_STRING).withSampleValue("Doe"),
                toColumn("email", HIVE_STRING).withSampleValue("johndoe@example.com"),
                toColumn("address", structType(ImmutableList.of("street", "city", "state", "zip_code"), ImmutableList.of(HIVE_STRING, HIVE_STRING, HIVE_STRING, HIVE_STRING))),
                toColumn("phone_numbers", arrayType(structType(ImmutableList.of("type", "number"), ImmutableList.of(HIVE_STRING, HIVE_STRING)))),
                toColumn("birth_date", HIVE_DATE).withSampleValue("1970-01-01"),
                toColumn("notes", HIVE_STRING).withSampleValue("This is a sample JSON object with a size greater than 4096 bytes. This text is repeated multiple times to make sure that the size of the JSON object is greater than 4096 bytes. This is a sample JSON object with a size greater than 4096 bytes. This text is repeated multiple times to make sure that the size of the JSON object is greater than 4096 bytes. This is a sample JSON object with a size greater than 4096 bytes. This text is repeated multiple times to make sure that the size of the JSON object is greater than 4096 bytes. This is a sample JSON object with a size greater than 4096 bytes. This text is repeated multiple times to make sure that the size of the JSON object is greater than 4096 bytes. This is a sample JSON object with a size greater than 4096 bytes. This text is repeated multiple times to make sure that the size of the JSON object is greater than 4096 bytes."));
    }
}
