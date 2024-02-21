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
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.starburst.schema.discovery.TableChanges.TableName;
import io.starburst.schema.discovery.formats.csv.CsvOptions;
import io.starburst.schema.discovery.infer.InferredPartitionProjection;
import io.starburst.schema.discovery.internal.Column;
import io.starburst.schema.discovery.internal.HiveType;
import io.starburst.schema.discovery.internal.HiveTypes;
import io.starburst.schema.discovery.io.DiscoveryTrinoFileSystem;
import io.starburst.schema.discovery.models.DiscoveredColumns;
import io.starburst.schema.discovery.models.DiscoveredPartitionValues;
import io.starburst.schema.discovery.models.DiscoveredPartitions;
import io.starburst.schema.discovery.models.DiscoveredSchema;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.TableFormat;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.starburst.schema.discovery.options.OptionsMap;
import io.starburst.schema.discovery.processor.Processor;
import io.starburst.schema.discovery.request.GuessRequest;
import io.trino.filesystem.Location;
import io.trino.plugin.hive.projection.ProjectionType;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.starburst.schema.discovery.Util.orcDataSourceFactory;
import static io.starburst.schema.discovery.Util.parquetDataSourceFactory;
import static io.starburst.schema.discovery.Util.schemaDiscoveryInstances;
import static io.starburst.schema.discovery.generation.Dialect.TRINO;
import static io.starburst.schema.discovery.io.LocationUtils.uriFromLocation;
import static io.starburst.schema.discovery.models.LowerCaseString.toLowerCase;
import static io.starburst.schema.discovery.models.SlashEndedPath.ensureEndsWithSlash;
import static io.starburst.schema.discovery.options.GeneralOptions.INCLUDE_PATTERNS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSchemaDiscoveryController
{
    @Test
    public void testPartitioned()
            throws ExecutionException, InterruptedException
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO);
        Location directory = Util.testFilePath("csv/partitioned");
        ListenableFuture<DiscoveredSchema> future = controller.guess(new GuessRequest(uriFromLocation(directory), CsvOptions.standard()));
        DiscoveredSchema discoveredTables = future.get();

        ImmutableList<Column> dsColumn = ImmutableList.of(new Column(toLowerCase("ds"), new HiveType(HiveTypes.HIVE_DATE)));
        ImmutableList<Column> testColumn = ImmutableList.of(new Column(toLowerCase("test"), new HiveType(HiveTypes.HIVE_STRING), Optional.of("test")));
        DiscoveredPartitions partitions = new DiscoveredPartitions(dsColumn, ImmutableList.of(new DiscoveredPartitionValues(ensureEndsWithSlash(directory.appendPath("ds=2012-12-29")), ImmutableMap.of(toLowerCase("ds"), "2012-12-29")),
                new DiscoveredPartitionValues(ensureEndsWithSlash(directory.appendPath("ds=2012-12-30")), ImmutableMap.of(toLowerCase("ds"), "2012-12-30"))));
        DiscoveredColumns columns = new DiscoveredColumns(testColumn, ImmutableSet.of());
        Map<String, String> standardOptionsUnwrapped = new OptionsMap(CsvOptions.standard()).unwrap();
        DiscoveredTable expectedDiscoveredTable = new DiscoveredTable(true,
                ensureEndsWithSlash(uriFromLocation(directory)),
                new TableName(Optional.empty(), toLowerCase("partitioned")),
                TableFormat.CSV,
                standardOptionsUnwrapped,
                columns,
                partitions,
                ImmutableList.of());

        assertThat(discoveredTables.rootPath().path()).isEqualTo(directory + "/");
        assertThat(discoveredTables.errors()).isEmpty();
        assertThat(discoveredTables.tables()).hasSize(1).containsExactly(expectedDiscoveredTable);
    }

    @Test
    public void testNestedPartitioned()
            throws ExecutionException, InterruptedException
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO);
        Location directory = Util.testFilePath("csv/nested_partitions");
        ListenableFuture<DiscoveredSchema> future = controller.guess(new GuessRequest(URI.create(directory.toString()), CsvOptions.standard()));
        DiscoveredSchema discoveredTables = future.get();

        ImmutableList<Column> partitionColumns = ImmutableList.of(
                new Column(toLowerCase("ds"), new HiveType(HiveTypes.HIVE_DATE)),
                new Column(toLowerCase("hour"), new HiveType(HiveTypes.HIVE_INT)),
                new Column(toLowerCase("minute"), new HiveType(HiveTypes.HIVE_INT)));
        ImmutableList<Column> testColumn = ImmutableList.of(new Column(toLowerCase("test"), new HiveType(HiveTypes.HIVE_STRING), Optional.of("test")));
        DiscoveredPartitions partitions = new DiscoveredPartitions(
                partitionColumns,
                ImmutableList.of(
                        new DiscoveredPartitionValues(ensureEndsWithSlash(directory.appendPath("npschema/nptable/ds=2012-12-29/hour=14/minute=10")), ImmutableMap.of(toLowerCase("ds"), "2012-12-29", toLowerCase("hour"), "14", toLowerCase("minute"), "10")),
                        new DiscoveredPartitionValues(ensureEndsWithSlash(directory.appendPath("npschema/nptable/ds=2012-12-30/hour=15/minute=35")), ImmutableMap.of(toLowerCase("ds"), "2012-12-30", toLowerCase("hour"), "15", toLowerCase("minute"), "35"))),
                ImmutableMap.of(
                        toLowerCase("ds"), new InferredPartitionProjection(true, ProjectionType.INJECTED),
                        toLowerCase("hour"), new InferredPartitionProjection(true, ProjectionType.INTEGER),
                        toLowerCase("minute"), new InferredPartitionProjection(true, ProjectionType.INTEGER)));
        DiscoveredColumns columns = new DiscoveredColumns(testColumn, ImmutableSet.of());
        Map<String, String> standardOptionsUnwrapped = new OptionsMap(CsvOptions.standard()).unwrap();
        DiscoveredTable expectedTable = new DiscoveredTable(true,
                ensureEndsWithSlash(directory.appendSuffix("/npschema").appendSuffix("/nptable")),
                new TableName(Optional.of(toLowerCase("npschema")), toLowerCase("nptable")),
                TableFormat.CSV,
                standardOptionsUnwrapped,
                columns,
                partitions,
                ImmutableList.of());

        assertThat(discoveredTables.rootPath().path()).isEqualTo(directory + "/");
        assertThat(discoveredTables.errors()).isEmpty();
        assertThat(discoveredTables.tables())
                .hasSize(1)
                .containsExactly(expectedTable);
    }

    @Test
    public void testTableOptionOverrides()
            throws ExecutionException, InterruptedException
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO);
        Location directory = Util.testFilePath("schema");
        DiscoveredSchema discoveredTables = controller.guess(new GuessRequest(uriFromLocation(directory), ImmutableMap.of())).get();
        assertThat(discoveredTables.tables()).extracting(DiscoveredTable::format).containsExactlyInAnyOrder(TableFormat.CSV, TableFormat.PARQUET);
        assertThat(discoveredTables.tables()).extracting(DiscoveredTable::columns).anyMatch(t -> t.columns().size() == 1);
        assertThat(discoveredTables.tables()).extracting(DiscoveredTable::columns).anyMatch(t -> t.columns().size() == 19);
        Map<String, String> options = ImmutableMap.of(
                "test.iso." + GeneralOptions.CHARSET, "ISO-8859-1",
                "test.iso." + CsvOptions.DELIMITER, "þ",
                "test.iso." + GeneralOptions.FORCED_FORMAT, TableFormat.CSV.name());
        discoveredTables = controller.guess(new GuessRequest(uriFromLocation(directory), options)).get();
        assertThat(discoveredTables.tables()).extracting(DiscoveredTable::format).containsExactlyInAnyOrder(TableFormat.CSV, TableFormat.PARQUET);
        assertThat(discoveredTables.tables()).extracting(DiscoveredTable::columns).anyMatch(t -> t.columns().size() == 5);
        assertThat(discoveredTables.tables()).extracting(DiscoveredTable::columns).anyMatch(t -> t.columns().size() == 19);
    }

    @Test
    public void testShallowDiscoveryPartitions()
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO);
        Map<String, String> includeOnlyNestedOptions = ImmutableMap.of(
                INCLUDE_PATTERNS, "**/csv/*/{nptable}/*");
        Location directory = Util.testFilePath("csv/nested_partitions");

        DiscoveredSchema discoveredShallowSchema = controller.discoverTablesShallow(new GuessRequest(uriFromLocation(directory), includeOnlyNestedOptions)).get();

        assertThat(discoveredShallowSchema.tables())
                .hasSize(1)
                .anyMatch(t -> !t.discoveredPartitions().columns().isEmpty())
                .allMatch(t -> t.columns().columns().isEmpty())
                .allMatch(t -> !t.valid() && t.format() == TableFormat.ERROR);
    }

    @Test
    public void testShallowDiscovery()
            throws ExecutionException, InterruptedException
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO);
        Map<String, String> includeOnlyNestedOptions = ImmutableMap.of(
                INCLUDE_PATTERNS, "**/csv/top-schema/{under-top-schema-1,under-top-schema-2}/*");
        Location directory = Util.testFilePath("csv/top-schema");

        DiscoveredSchema discoveredShallowSchema = controller.discoverTablesShallow(new GuessRequest(uriFromLocation(directory), includeOnlyNestedOptions)).get();

        assertThat(discoveredShallowSchema.tables())
                .hasSize(2)
                .anyMatch(t -> t.discoveredPartitions().columns().isEmpty())
                .allMatch(t -> t.columns().columns().isEmpty())
                .allMatch(t -> !t.valid() && t.format() == TableFormat.ERROR);
    }

    @Test
    public void testShallowDiscoveryNestedPartitions()
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO);
        Location directory = Util.testFilePath("csv/nested_partitions");

        DiscoveredSchema discoveredShallowSchema = controller.discoverTablesShallow(new GuessRequest(uriFromLocation(directory), ImmutableMap.of())).get();

        assertThat(discoveredShallowSchema.tables())
                .hasSize(1)
                .anyMatch(t -> !t.discoveredPartitions().columns().isEmpty())
                .allMatch(t -> t.columns().columns().isEmpty())
                .allMatch(t -> !t.valid() && t.format() == TableFormat.ERROR);
    }

    // this test was observed to be flaky, let it run multiple times as its fast anyway
    @RepeatedTest(value = 100)
    public void testColumnsErrorShouldBeScopedToTable()
            throws ExecutionException, InterruptedException
    {
        // make it discover correct format, but fail on processing file later (simulate corrupted data)
        DiscoveryTrinoFileSystem fileSystem = new DiscoveryTrinoFileSystem(new ErroringTrinoFileSystem("csv/partitioned", "000000_0", 9000, 3));
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO);
        Location directory = Util.testFilePath("csv/partitioned");
        ListenableFuture<DiscoveredSchema> future = controller.guess(new GuessRequest(uriFromLocation(directory), CsvOptions.standard()));
        DiscoveredSchema discoveredTables = future.get();

        assertThat(discoveredTables.errors()).isEmpty();
        assertThat(discoveredTables.tables()).hasSize(1);
        assertThat(discoveredTables.tables().get(0).errors()).hasSize(4);
        assertThat(discoveredTables.tables().get(0).errors())
                .anyMatch(e -> e.contains("ds=2012-12-29/000000_0") && e.contains("Underlying input stream returned zero bytes"));
        assertThat(discoveredTables.tables().get(0).errors())
                .anyMatch(e -> e.contains("ds=2012-12-30/000000_0") && e.contains("Underlying input stream returned zero bytes"));
        assertThat(discoveredTables.tables().get(0).errors())
                .anyMatch(e -> e.contains("Error while discovering schema in format: [CSV]"));
        assertThat(discoveredTables.tables().get(0).errors())
                .last().matches(e -> e.contains("no valid columns were found"));
    }

    @Test
    public void testNestedIncludePatterns()
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO);
        Map<String, String> includeOnlyNestedOptions = ImmutableMap.of(
                INCLUDE_PATTERNS, "**/csv/{top-schema/under-top-schema-1/child-schema-1}*");
        Location directory = Util.testFilePath("csv/top-schema");
        DiscoveredSchema discoveredSchema = controller.guess(new GuessRequest(uriFromLocation(directory), includeOnlyNestedOptions)).get();

        assertThat(discoveredSchema.tables())
                .hasSize(1)
                .anyMatch(t -> t.discoveredPartitions().columns().isEmpty())
                .allMatch(t -> !t.columns().columns().isEmpty())
                .allMatch(t -> t.valid() && t.format() == TableFormat.CSV);
    }

    @Test
    public void testLimitsAsync()
            throws Exception
    {
        // SAMPLE_FILES_PER_TABLE_MODULO(1) - all files
        limitsTest(3, 3, 1, 3, 6, 9);
        limitsTest(1, 3, 1, 3);
        limitsTest(3, 1, 1, 1, 4, 7);
        limitsTest(2, 1, 1, 1, 4);
        limitsTest(2, 2, 1, 2, 5);

        // SAMPLE_FILES_PER_TABLE_MODULO(3) - only first files
        limitsTest(3, 3, 3, 1, 4, 7);
        limitsTest(1, 3, 3, 1);
        limitsTest(3, 1, 3, 1, 4, 7);
        limitsTest(2, 1, 3, 1, 4);
        limitsTest(2, 2, 3, 1, 4);
    }

    private void limitsTest(int tableQty, int filePerTableQty, int filesPerTableModulo, int... expectedColumnQtys)
            throws Exception
    {
        assertThat(tableQty).isEqualTo(expectedColumnQtys.length);

        HashMap<String, String> options = new HashMap<String, String>();
        options.put(GeneralOptions.MAX_SAMPLE_TABLES, Integer.toString(tableQty));
        options.put(GeneralOptions.MAX_SAMPLE_FILES_PER_TABLE, Integer.toString(filePerTableQty));
        options.put(GeneralOptions.SAMPLE_FILES_PER_TABLE_MODULO, Integer.toString(filesPerTableModulo));

        Location directory = Util.testFilePath("limits");
        Processor processor = new Processor(schemaDiscoveryInstances, Util.fileSystem(), directory, new OptionsMap(options), directExecutor());
        processor.startRootProcessing();
        List<DiscoveredTable> discoveredTables = processor.get(5, TimeUnit.SECONDS).tables();
        assertThat(discoveredTables.size()).isEqualTo(tableQty);
        for (int i = 0; i < expectedColumnQtys.length; ++i) {
            DiscoveredTable discovered = discoveredTables.get(i);
            assertThat(discovered.valid()).isTrue();
            assertThat(discovered.columns().columns()).hasSize(expectedColumnQtys[i]);
        }
    }
}
