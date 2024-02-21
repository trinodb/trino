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
import io.starburst.schema.discovery.models.DiscoveredSchema;
import io.starburst.schema.discovery.models.TableFormat;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.starburst.schema.discovery.options.OptionsMap;
import io.starburst.schema.discovery.processor.Processor;
import io.trino.filesystem.Location;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.Executors;

import static io.starburst.schema.discovery.Util.schemaDiscoveryInstances;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeSchemaDiscovery
{
    private static final OptionsMap OPTIONS = new OptionsMap(ImmutableMap.of(GeneralOptions.SAMPLE_FILES_PER_TABLE_MODULO, "1"));

    @Test
    public void testDeltaLakeSingleTable()
    {
        Location directory = Util.testFilePath("deltalake/table1");
        Processor processor = new Processor(schemaDiscoveryInstances, Util.fileSystem(), directory, OPTIONS, Executors.newCachedThreadPool());
        processor.startRootProcessing();
        assertThat(processor)
                .succeedsWithin(Duration.ofSeconds(1))
                .matches(discovered -> discovered.rootPath().path().endsWith("deltalake/table1/")
                                       && discovered.errors().isEmpty()
                                       && discovered.tables().size() == 1)
                .extracting(discovered -> discovered.tables().get(0))
                .matches(table -> table.valid() && table.path().path().endsWith("deltalake/table1/") && table.format() == TableFormat.DELTA_LAKE);
    }

    @Test
    public void testDeltaLakeTablesParent()
    {
        Location directory = Util.testFilePath("deltalake");
        Processor processor = new Processor(schemaDiscoveryInstances, Util.fileSystem(), directory, OPTIONS, Executors.newCachedThreadPool());
        processor.startRootProcessing();
        assertThat(processor)
                .succeedsWithin(Duration.ofSeconds(1))
                .matches(discovered -> discovered.rootPath().path().endsWith("deltalake/")
                                       && discovered.errors().isEmpty()
                                       && discovered.tables().size() == 2)
                .extracting(DiscoveredSchema::tables)
                .matches(tables ->
                        tables.get(0).valid() && tables.get(0).path().path().endsWith("deltalake/table2/") && tables.get(0).format() == TableFormat.DELTA_LAKE &&
                        tables.get(1).valid() && tables.get(1).path().path().endsWith("deltalake/table1/") && tables.get(1).format() == TableFormat.DELTA_LAKE);
    }

    @Test
    public void testModuloRecursiveDeltaLake()
    {
        OptionsMap optionsMap = new OptionsMap(ImmutableMap.of(GeneralOptions.SAMPLE_FILES_PER_TABLE_MODULO, "8", GeneralOptions.MAX_SAMPLE_FILES_PER_TABLE, "1", GeneralOptions.DISCOVERY_MODE, "recursive_directories"));
        Location directory = Util.testFilePath("deltalake");
        Processor processor = new Processor(schemaDiscoveryInstances, Util.fileSystem(), directory, optionsMap, Executors.newCachedThreadPool());
        processor.startRootProcessing();
        assertThat(processor)
                .succeedsWithin(Duration.ofSeconds(1))
                .matches(discovered -> discovered.rootPath().path().endsWith("deltalake/")
                                       && discovered.errors().isEmpty()
                                       && discovered.tables().size() == 2)
                .extracting(DiscoveredSchema::tables)
                .matches(tables -> tables.stream().allMatch(table ->
                        table.valid() &&
                        (table.path().path().endsWith("deltalake/table1/") || table.path().path().endsWith("deltalake/table2/")) &&
                        table.format() == TableFormat.DELTA_LAKE));
    }
}
