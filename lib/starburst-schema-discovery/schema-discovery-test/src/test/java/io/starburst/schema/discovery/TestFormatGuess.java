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

import io.starburst.schema.discovery.formats.csv.CsvOptions;
import io.starburst.schema.discovery.formats.csv.CsvSchemaDiscovery;
import io.starburst.schema.discovery.formats.json.JsonSchemaDiscovery;
import io.starburst.schema.discovery.internal.FormatGuess;
import io.starburst.schema.discovery.options.OptionsMap;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.starburst.schema.discovery.Util.orcSchemaDiscovery;
import static io.starburst.schema.discovery.Util.parquetSchemaDiscovery;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFormatGuess
{
    @Test
    public void testTextFiles()
    {
        Optional<FormatGuess> match = CsvSchemaDiscovery.INSTANCE.checkFormatMatch(Util.testFile("csv/decimal.csv"));
        assertThat(match).isNotEmpty();
        assertThat(new CsvOptions(new OptionsMap(match.get().options())).delimiter()).isEqualTo(",");

        match = CsvSchemaDiscovery.INSTANCE.checkFormatMatch(Util.testFile("csv/cars.tsv"));
        assertThat(match).isNotEmpty();
        assertThat(new CsvOptions(new OptionsMap(match.get().options())).delimiter()).isEqualTo("\t");

        match = CsvSchemaDiscovery.INSTANCE.checkFormatMatch(Util.testFile("csv/ctrl-a.csv"));
        assertThat(match).isNotEmpty();
        assertThat(new CsvOptions(new OptionsMap(match.get().options())).delimiter()).isEqualTo("\1");

        match = CsvSchemaDiscovery.INSTANCE.checkFormatMatch(Util.testFile("json/with-array-fields.json"));
        assertThat(match).isNotEmpty();
        assertThat(match.get().confidence()).isEqualTo(FormatGuess.Confidence.LOW);
        assertThat(new CsvOptions(new OptionsMap(match.get().options())).delimiter()).isEqualTo(",");

        match = CsvSchemaDiscovery.INSTANCE.checkFormatMatch(Util.testFile("orc/users.orc"));
        assertThat(match).isEmpty();
    }

    @Test
    public void testJsonFiles()
    {
        Optional<FormatGuess> match = JsonSchemaDiscovery.INSTANCE.checkFormatMatch(Util.testFile("json/with-array-fields.json"));
        assertThat(match).isNotEmpty();
        match = JsonSchemaDiscovery.INSTANCE.checkFormatMatch(Util.testFile("json/with-map-fields.json"));
        assertThat(match).isNotEmpty();
        match = JsonSchemaDiscovery.INSTANCE.checkFormatMatch(Util.testFile("json/huge.json"));
        assertThat(match).isNotEmpty();
        match = JsonSchemaDiscovery.INSTANCE.checkFormatMatch(Util.testFile("json/types.json"));
        assertThat(match).isNotEmpty();

        match = JsonSchemaDiscovery.INSTANCE.checkFormatMatch(Util.testFile("csv/decimal.csv"));
        assertThat(match).isEmpty();

        match = JsonSchemaDiscovery.INSTANCE.checkFormatMatch(Util.testFile("csv/cars.tsv"));
        assertThat(match).isEmpty();

        match = JsonSchemaDiscovery.INSTANCE.checkFormatMatch(Util.testFile("orc/users.orc"));
        assertThat(match).isEmpty();
    }

    @Test
    public void testOrcFiles()
    {
        Optional<FormatGuess> match = orcSchemaDiscovery.checkFormatMatch(Util.testFile("orc/users.orc"));
        assertThat(match).isNotEmpty();
        match = orcSchemaDiscovery.checkFormatMatch(Util.testFile("orc/from-trino.orc"));
        assertThat(match).isNotEmpty();

        match = orcSchemaDiscovery.checkFormatMatch(Util.testFile("json/types.json"));
        assertThat(match).isEmpty();
        match = orcSchemaDiscovery.checkFormatMatch(Util.testFile("csv/decimal.csv"));
        assertThat(match).isEmpty();
    }

    @Test
    public void testParquetFiles()
    {
        Optional<FormatGuess> match = parquetSchemaDiscovery.checkFormatMatch(Util.testFile("parquet/proto-struct-with-array-many.parquet"));
        assertThat(match).isNotEmpty();
        match = parquetSchemaDiscovery.checkFormatMatch(Util.testFile("parquet/from-trino.parquet"));
        assertThat(match).isNotEmpty();

        match = parquetSchemaDiscovery.checkFormatMatch(Util.testFile("json/types.json"));
        assertThat(match).isEmpty();
        match = parquetSchemaDiscovery.checkFormatMatch(Util.testFile("csv/decimal.csv"));
        assertThat(match).isEmpty();
    }
}
