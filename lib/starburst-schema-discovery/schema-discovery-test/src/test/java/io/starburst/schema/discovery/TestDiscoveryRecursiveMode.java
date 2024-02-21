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
import com.google.common.util.concurrent.ListenableFuture;
import io.starburst.schema.discovery.TableChanges.TableName;
import io.starburst.schema.discovery.formats.csv.CsvOptions;
import io.starburst.schema.discovery.internal.Column;
import io.starburst.schema.discovery.internal.HiveType;
import io.starburst.schema.discovery.internal.HiveTypes;
import io.starburst.schema.discovery.io.DiscoveryTrinoFileSystem;
import io.starburst.schema.discovery.models.DiscoveredColumns;
import io.starburst.schema.discovery.models.DiscoveredPartitions;
import io.starburst.schema.discovery.models.DiscoveredSchema;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.TableFormat;
import io.starburst.schema.discovery.options.DiscoveryMode;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.starburst.schema.discovery.options.OptionsMap;
import io.starburst.schema.discovery.request.GuessRequest;
import io.trino.filesystem.Location;
import org.junit.jupiter.api.Test;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.starburst.schema.discovery.Util.orcDataSourceFactory;
import static io.starburst.schema.discovery.Util.parquetDataSourceFactory;
import static io.starburst.schema.discovery.generation.Dialect.TRINO;
import static io.starburst.schema.discovery.io.LocationUtils.uriFromLocation;
import static io.starburst.schema.discovery.models.LowerCaseString.toLowerCase;
import static io.starburst.schema.discovery.models.SlashEndedPath.ensureEndsWithSlash;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDiscoveryRecursiveMode
{
    private static final Map<String, String> OPTIONS = ImmutableMap.<String, String>builder()
            .putAll(CsvOptions.standard())
            .put(GeneralOptions.SAMPLE_FILES_PER_TABLE_MODULO, "2")
            .put(GeneralOptions.DISCOVERY_MODE, "recursive_directories")
            .put(GeneralOptions.MAX_SAMPLE_FILES_PER_TABLE, "2")
            .buildKeepingLast();

    @Test
    public void testRecursiveDirectories()
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO, directExecutor());
        Location directory = Util.testFilePath("recursive");
        ListenableFuture<DiscoveredSchema> discoveryFuture = controller.guess(new GuessRequest(uriFromLocation(directory), OPTIONS));
        DiscoveredSchema discoveredSchema = discoveryFuture.get(5, TimeUnit.SECONDS);

        Map<String, String> expectedOptions = ImmutableMap.<String, String>builder()
                .putAll(new OptionsMap(OPTIONS).unwrap())
                .put(GeneralOptions.DISCOVERY_MODE.toLowerCase(Locale.ENGLISH), DiscoveryMode.RECURSIVE_DIRECTORIES.name().toLowerCase(Locale.ENGLISH))
                .buildKeepingLast();

        DiscoveredTable expectedDiscoveredTable1 = new DiscoveredTable(
                true,
                ensureEndsWithSlash(directory.appendSuffix("/rtable1")),
                new TableName(Optional.empty(), toLowerCase("rtable1")),
                TableFormat.CSV,
                expectedOptions,
                new DiscoveredColumns(
                        // samples from simple2024_2.csv, should skip first simple2024.csv and move to second file in this dir
                        ImmutableList.of(new Column(toLowerCase("name"), new HiveType(HiveTypes.HIVE_STRING), Optional.of("john")),
                                new Column(toLowerCase("age"), new HiveType(HiveTypes.HIVE_INT), Optional.of("5")),
                                new Column(toLowerCase("lastname"), new HiveType(HiveTypes.HIVE_STRING), Optional.of("locke"))),
                        ImmutableList.of()),
                DiscoveredPartitions.EMPTY_DISCOVERED_PARTITIONS,
                ImmutableList.of());

        DiscoveredTable expectedDiscoveredTable2 = new DiscoveredTable(
                true,
                ensureEndsWithSlash(directory.appendSuffix("/rtable2")),
                new TableName(Optional.empty(), toLowerCase("rtable2")),
                TableFormat.CSV,
                expectedOptions,
                new DiscoveredColumns(
                        // samples from simple2025.csv, skips columns from simple2024.csv
                        ImmutableList.of(new Column(toLowerCase("name"), new HiveType(HiveTypes.HIVE_STRING), Optional.of("john")),
                                new Column(toLowerCase("age"), new HiveType(HiveTypes.HIVE_INT), Optional.of("5"))),
                        ImmutableList.of()),
                DiscoveredPartitions.EMPTY_DISCOVERED_PARTITIONS,
                ImmutableList.of());

        assertThat(discoveredSchema.tables())
                .hasSize(2)
                .containsExactlyInAnyOrder(expectedDiscoveredTable1, expectedDiscoveredTable2);
    }

    @Test
    public void testNestedPartitionsWithRecursiveMode()
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO, directExecutor());
        Location directory = Util.testFilePath("csv/nested_partitions");
        ListenableFuture<DiscoveredSchema> discoveryFuture = controller.guess(new GuessRequest(uriFromLocation(directory), OPTIONS));
        DiscoveredSchema discoveredSchema = discoveryFuture.get(5, TimeUnit.SECONDS);

        Map<String, String> expectedOptions = ImmutableMap.<String, String>builder()
                .putAll(new OptionsMap(OPTIONS).unwrap())
                .put(GeneralOptions.DISCOVERY_MODE.toLowerCase(Locale.ENGLISH), DiscoveryMode.RECURSIVE_DIRECTORIES.name().toLowerCase(Locale.ENGLISH))
                .buildKeepingLast();

        DiscoveredTable expectedDiscoveredTable = new DiscoveredTable(
                true,
                ensureEndsWithSlash(directory.appendSuffix("/npschema")),
                new TableName(Optional.empty(), toLowerCase("npschema")),
                TableFormat.CSV,
                expectedOptions,
                new DiscoveredColumns(
                        ImmutableList.of(new Column(toLowerCase("test"), new HiveType(HiveTypes.HIVE_STRING), Optional.of("test"))),
                        ImmutableList.of()),
                DiscoveredPartitions.EMPTY_DISCOVERED_PARTITIONS,
                ImmutableList.of());

        assertThat(discoveredSchema.tables())
                .hasSize(1)
                .containsExactly(expectedDiscoveredTable);
    }

    @Test
    public void testNestedPartitionsWithRecursiveMode2()
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO, directExecutor());
        Location directory = Util.testFilePath("csv/nested_partitions/npschema");
        ListenableFuture<DiscoveredSchema> discoveryFuture = controller.guess(new GuessRequest(uriFromLocation(directory), OPTIONS));
        DiscoveredSchema discoveredSchema = discoveryFuture.get(5, TimeUnit.SECONDS);

        Map<String, String> expectedOptions = ImmutableMap.<String, String>builder()
                .putAll(new OptionsMap(OPTIONS).unwrap())
                .put(GeneralOptions.DISCOVERY_MODE.toLowerCase(Locale.ENGLISH), DiscoveryMode.RECURSIVE_DIRECTORIES.name().toLowerCase(Locale.ENGLISH))
                .buildKeepingLast();

        DiscoveredTable expectedDiscoveredTable = new DiscoveredTable(
                true,
                ensureEndsWithSlash(directory.appendSuffix("/nptable")),
                new TableName(Optional.empty(), toLowerCase("nptable")),
                TableFormat.CSV,
                expectedOptions,
                new DiscoveredColumns(
                        ImmutableList.of(new Column(toLowerCase("test"), new HiveType(HiveTypes.HIVE_STRING), Optional.of("test"))),
                        ImmutableList.of()),
                DiscoveredPartitions.EMPTY_DISCOVERED_PARTITIONS,
                ImmutableList.of());

        assertThat(discoveredSchema.tables())
                .hasSize(1)
                .containsExactly(expectedDiscoveredTable);
    }

    @Test
    public void testNestedPartitionsWithRecursiveMode3()
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO, directExecutor());
        Location directory = Util.testFilePath("csv/nested_partitions/npschema/nptable");
        ListenableFuture<DiscoveredSchema> discoveryFuture = controller.guess(new GuessRequest(uriFromLocation(directory), OPTIONS));
        DiscoveredSchema discoveredSchema = discoveryFuture.get(5, TimeUnit.SECONDS);

        Map<String, String> expectedOptions = ImmutableMap.<String, String>builder()
                .putAll(new OptionsMap(OPTIONS).unwrap())
                .put(GeneralOptions.DISCOVERY_MODE.toLowerCase(Locale.ENGLISH), DiscoveryMode.RECURSIVE_DIRECTORIES.name().toLowerCase(Locale.ENGLISH))
                .buildKeepingLast();

        DiscoveredTable expectedDiscoveredTable = new DiscoveredTable(
                true,
                ensureEndsWithSlash(directory),
                new TableName(Optional.empty(), toLowerCase("nptable")),
                TableFormat.CSV,
                expectedOptions,
                new DiscoveredColumns(
                        ImmutableList.of(new Column(toLowerCase("test"), new HiveType(HiveTypes.HIVE_STRING), Optional.of("test"))),
                        ImmutableList.of()),
                DiscoveredPartitions.EMPTY_DISCOVERED_PARTITIONS,
                ImmutableList.of());

        assertThat(discoveredSchema.tables())
                .hasSize(1)
                .containsExactly(expectedDiscoveredTable);
    }

    @Test
    public void testRecursiveDirectoryShallow()
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO, directExecutor());
        Location directory = Util.testFilePath("recursive");
        ListenableFuture<DiscoveredSchema> discoveryFuture = controller.discoverTablesShallow(new GuessRequest(uriFromLocation(directory), OPTIONS));
        DiscoveredSchema discoveredSchema = discoveryFuture.get(5, TimeUnit.SECONDS);

        Map<String, String> expectedOptions = ImmutableMap.<String, String>builder()
                .putAll(new OptionsMap(OPTIONS).unwrap())
                .put(GeneralOptions.DISCOVERY_MODE.toLowerCase(Locale.ENGLISH), DiscoveryMode.RECURSIVE_DIRECTORIES.name().toLowerCase(Locale.ENGLISH))
                .buildKeepingLast();

        DiscoveredTable expectedDiscoveredTable1 = new DiscoveredTable(
                false,
                ensureEndsWithSlash(directory.appendSuffix("/rtable1")),
                new TableName(Optional.empty(), toLowerCase("rtable1")),
                TableFormat.ERROR,
                expectedOptions,
                DiscoveredColumns.EMPTY_DISCOVERED_COLUMNS,
                DiscoveredPartitions.EMPTY_DISCOVERED_PARTITIONS,
                ImmutableList.of());

        DiscoveredTable expectedDiscoveredTable2 = new DiscoveredTable(
                false,
                ensureEndsWithSlash(directory.appendSuffix("/rtable2")),
                new TableName(Optional.empty(), toLowerCase("rtable2")),
                TableFormat.ERROR,
                expectedOptions,
                DiscoveredColumns.EMPTY_DISCOVERED_COLUMNS,
                DiscoveredPartitions.EMPTY_DISCOVERED_PARTITIONS,
                ImmutableList.of());

        assertThat(discoveredSchema.tables())
                .hasSize(2)
                .map(DiscoveredTable::skipErrors)
                .containsExactlyInAnyOrder(expectedDiscoveredTable1, expectedDiscoveredTable2);
    }

    @Test
    public void testNestedPartitionsShallow()
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO, directExecutor());
        Location directory = Util.testFilePath("csv/nested_partitions/npschema");
        ListenableFuture<DiscoveredSchema> discoveryFuture = controller.discoverTablesShallow(new GuessRequest(uriFromLocation(directory), OPTIONS));
        DiscoveredSchema discoveredSchema = discoveryFuture.get(5, TimeUnit.SECONDS);

        Map<String, String> expectedOptions = ImmutableMap.<String, String>builder()
                .putAll(new OptionsMap(OPTIONS).unwrap())
                .put(GeneralOptions.DISCOVERY_MODE.toLowerCase(Locale.ENGLISH), DiscoveryMode.RECURSIVE_DIRECTORIES.name().toLowerCase(Locale.ENGLISH))
                .buildKeepingLast();

        DiscoveredTable expectedDiscoveredTable = new DiscoveredTable(
                false,
                ensureEndsWithSlash(directory.appendSuffix("/nptable")),
                new TableName(Optional.empty(), toLowerCase("nptable")),
                TableFormat.ERROR,
                expectedOptions,
                DiscoveredColumns.EMPTY_DISCOVERED_COLUMNS,
                DiscoveredPartitions.EMPTY_DISCOVERED_PARTITIONS,
                ImmutableList.of());

        assertThat(discoveredSchema.tables())
                .hasSize(1)
                .map(DiscoveredTable::skipErrors)
                .containsExactly(expectedDiscoveredTable);
    }
}
