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
import io.starburst.schema.discovery.formats.csv.CsvFlags;
import io.starburst.schema.discovery.formats.csv.CsvOptions;
import io.starburst.schema.discovery.internal.Column;
import io.starburst.schema.discovery.internal.HiveType;
import io.starburst.schema.discovery.internal.HiveTypes;
import io.starburst.schema.discovery.io.DiscoveryTrinoFileSystem;
import io.starburst.schema.discovery.models.DiscoveredColumns;
import io.starburst.schema.discovery.models.DiscoveredSchema;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.GeneratedOperations;
import io.starburst.schema.discovery.models.Operation;
import io.starburst.schema.discovery.models.TableFormat;
import io.starburst.schema.discovery.options.DiscoveryMode;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.starburst.schema.discovery.options.OptionsMap;
import io.starburst.schema.discovery.processor.Processor;
import io.starburst.schema.discovery.request.DiscoverRequest;
import io.starburst.schema.discovery.request.GenerateOperationsRequest;
import io.starburst.schema.discovery.request.GenerateOptions;
import io.starburst.schema.discovery.request.GuessRequest;
import io.trino.filesystem.Location;
import io.trino.plugin.hive.type.DecimalTypeInfo;
import io.trino.plugin.hive.type.TypeInfo;
import io.trino.spi.TrinoException;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.starburst.schema.discovery.SchemaDiscoveryErrorCode.LOCATION_DOES_NOT_EXISTS;
import static io.starburst.schema.discovery.Util.orcDataSourceFactory;
import static io.starburst.schema.discovery.Util.parquetDataSourceFactory;
import static io.starburst.schema.discovery.Util.schemaDiscoveryInstances;
import static io.starburst.schema.discovery.generation.Dialect.TRINO;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_DATE;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_INT;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_LONG;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_STRING;
import static io.starburst.schema.discovery.internal.HiveTypes.STRING_TYPE;
import static io.starburst.schema.discovery.internal.HiveTypes.adjustType;
import static io.starburst.schema.discovery.internal.HiveTypes.arrayType;
import static io.starburst.schema.discovery.internal.HiveTypes.structType;
import static io.starburst.schema.discovery.io.LocationUtils.uriFromLocation;
import static io.starburst.schema.discovery.models.LowerCaseString.toLowerCase;
import static io.starburst.schema.discovery.models.TableFormat.ERROR;
import static io.starburst.schema.discovery.models.TableFormat.JSON;
import static io.starburst.schema.discovery.options.GeneralOptions.DISCOVERY_MODE;
import static io.starburst.schema.discovery.options.GeneralOptions.HIVE_SKIP_HIDDEN_FILES;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.function.Predicate.not;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSchemaFromDirectory
{
    private static final OptionsMap OPTIONS = new OptionsMap(ImmutableMap.of(GeneralOptions.SAMPLE_FILES_PER_TABLE_MODULO, "1"));
    private static final Map<String, String> CSV_OPTIONS;

    static {
        HashMap<String, String> work = new HashMap<>(CsvOptions.standard());
        work.put(GeneralOptions.SAMPLE_FILES_PER_TABLE_MODULO, "1");
        CSV_OPTIONS = ImmutableMap.copyOf(work);
    }

    @Test
    public void testGuessSync()
            throws Exception
    {
        doGuessTest(directExecutor());
    }

    @Test
    public void testCoalesceMismatch()
            throws Exception
    {
        Location directory = Util.testFilePath("csv/coalesce-mismatch");
        Processor processor = new Processor(schemaDiscoveryInstances, Util.fileSystem(), directory, OPTIONS, directExecutor());
        processor.startRootProcessing();
        DiscoveredSchema discoveredTableSet = processor.get(5, TimeUnit.SECONDS);
        assertThat(discoveredTableSet.errors()).hasSize(0);
        assertThat(discoveredTableSet.tables().get(0).errors()).anyMatch(e -> e.contains("do not match with each other"));
        assertThat(discoveredTableSet.tables().get(0).errors()).anyMatch(e -> e.contains("mismatch"));
        assertThat(discoveredTableSet.tables().get(0).errors()).anyMatch(e -> e.contains("invalid"));
    }

    @Test
    public void testDiscoverSync()
            throws Exception
    {
        doDiscoverTest(directExecutor());
    }

    @Test
    public void testShallowDiscoverySync()
            throws Exception
    {
        doShallowDiscoveryTest(directExecutor());
    }

    @Test
    public void testErroredRescanSync()
            throws Exception
    {
        doErroredRescanTest(directExecutor());
    }

    @Test
    public void testValidRescanSync()
            throws Exception
    {
        doValidRescanTest(directExecutor());
    }

    @Test
    public void testGuessAsyncUnlimitedPool()
            throws Exception
    {
        doGuessTest(newCachedThreadPool());
    }

    @Test
    public void testDiscoverAsyncUnlimitedPool()
            throws Exception
    {
        doDiscoverTest(newCachedThreadPool());
    }

    @Test
    public void testShallowDiscoveryAsyncUnlimitedPool()
            throws Exception
    {
        doShallowDiscoveryTest(newCachedThreadPool());
    }

    @Test
    public void testErroredRescanAsyncUnlimitedPool()
            throws Exception
    {
        doErroredRescanTest(newCachedThreadPool());
    }

    @Test
    public void testValidRescanAsyncUnlimitedPool()
            throws Exception
    {
        doValidRescanTest(newCachedThreadPool());
    }

    @Test
    public void testGuessAsyncSingleThreaded()
            throws Exception
    {
        doGuessTest(Executors.newFixedThreadPool(1));
    }

    @Test
    public void testDiscoverAsyncSingleThreaded()
            throws Exception
    {
        doDiscoverTest(Executors.newFixedThreadPool(1));
    }

    @Test
    public void testShallowDiscoveryAsyncSingleThreaded()
            throws Exception
    {
        doShallowDiscoveryTest(Executors.newFixedThreadPool(1));
    }

    @Test
    public void testErroredRescanAsyncSingleThreaded()
            throws Exception
    {
        doErroredRescanTest(Executors.newFixedThreadPool(1));
    }

    @Test
    public void testValidRescanAsyncSingleThreaded()
            throws Exception
    {
        doValidRescanTest(Executors.newFixedThreadPool(1));
    }

    @Test
    public void testGuessAsyncEmpty()
            throws Exception
    {
        testEmptyAsync((__, processor) -> processor.startRootProcessing());
    }

    @Test
    public void testDiscoverAsyncEmpty()
            throws Exception
    {
        testEmptyAsync((file, processor) -> {
            Location filePath = Location.of("local://" + file.getAbsolutePath());
            processor.startSubPathProcessing(filePath, TableFormat.CSV, CSV_OPTIONS);
        });
    }

    @Test
    public void testShallowDiscoveryAsyncEmpty()
            throws Exception
    {
        testEmptyAsync((__, processor) -> processor.startShallowProcessing());
    }

    @Test
    public void testGuessAsyncWithFirstException()
            throws Exception
    {
        ErroringTrinoFileSystem erroringFileSystem = new ErroringTrinoFileSystem("limits/t1", "a.csv", 4);
        Processor processor = new Processor(schemaDiscoveryInstances, new DiscoveryTrinoFileSystem(erroringFileSystem), erroringFileSystem.directory(), OPTIONS, newCachedThreadPool());
        processor.startRootProcessing();
        DiscoveredSchema discoveredSchema = processor.get(5, TimeUnit.SECONDS);
        assertContainsBoomError(discoveredSchema);
    }

    @Test
    public void testDiscoverAsyncWithFirstException()
            throws Exception
    {
        ErroringTrinoFileSystem erroringFileSystem = new ErroringTrinoFileSystem("limits/t1", "a.csv", 4);
        Processor processor = new Processor(schemaDiscoveryInstances, new DiscoveryTrinoFileSystem(erroringFileSystem), erroringFileSystem.directory(), OPTIONS, newCachedThreadPool());
        processor.startSubPathProcessing(erroringFileSystem.directory(), TableFormat.CSV, CSV_OPTIONS);
        DiscoveredSchema discoveredSchema = processor.get(5, TimeUnit.SECONDS);
        assertContainsBoomError(discoveredSchema);
    }

    @Test
    public void testShallowDiscoveryAsyncWithFirstException()
    {
        ErroringTrinoFileSystem erroringFileSystem = new ErroringTrinoFileSystem("limits/t1", "limits/t1", 4, true);
        Processor processor = new Processor(schemaDiscoveryInstances, new DiscoveryTrinoFileSystem(erroringFileSystem), erroringFileSystem.directory(), OPTIONS, newCachedThreadPool());
        processor.startShallowProcessing();
        assertThatThrownBy(() -> processor.get(5, TimeUnit.SECONDS)).rootCause().hasMessage("listDirectories fail");
    }

    @Test
    public void testGuessAsyncWithSecondaryException()
            throws Exception
    {
        ErroringTrinoFileSystem erroringFileSystem = new ErroringTrinoFileSystem("limits/t1", "b.csv", 4);
        Processor processor = new Processor(schemaDiscoveryInstances, new DiscoveryTrinoFileSystem(erroringFileSystem), erroringFileSystem.directory(), OPTIONS, newCachedThreadPool());
        processor.startRootProcessing();
        DiscoveredSchema discoveredSchema = processor.get(5, TimeUnit.SECONDS);
        assertContainsBoomError(discoveredSchema);
    }

    @Test
    public void testDiscoverAsyncWithSecondaryException()
            throws Exception
    {
        ErroringTrinoFileSystem erroringFileSystem = new ErroringTrinoFileSystem("limits/t1", "b.csv", 4);
        Processor processor = new Processor(schemaDiscoveryInstances, new DiscoveryTrinoFileSystem(erroringFileSystem), erroringFileSystem.directory(), OPTIONS, newCachedThreadPool());
        processor.startSubPathProcessing(erroringFileSystem.directory(), TableFormat.CSV, CSV_OPTIONS);
        DiscoveredSchema discoveredSchema = processor.get(5, TimeUnit.SECONDS);
        assertContainsBoomError(discoveredSchema);
    }

    @Test
    public void testShallowDiscoveryAsyncWithSecondaryException()
    {
        ErroringTrinoFileSystem erroringFileSystem = new ErroringTrinoFileSystem("limits/t1", "b.csv", 4, true);
        Processor processor = new Processor(schemaDiscoveryInstances, new DiscoveryTrinoFileSystem(erroringFileSystem), erroringFileSystem.directory(), OPTIONS, newCachedThreadPool());
        processor.startShallowProcessing();
        assertThatThrownBy(() -> processor.get(5, TimeUnit.SECONDS)).rootCause().hasMessage("listDirectories fail");
    }

    @Test
    public void testGuessAsyncWithUnrecognizedFirstStream()
            throws Exception
    {
        ErroringTrinoFileSystem erroringFileSystem = new ErroringTrinoFileSystem("bogus-first", "a.txt", Integer.MAX_VALUE);
        Processor processor = new Processor(schemaDiscoveryInstances, new DiscoveryTrinoFileSystem(erroringFileSystem), erroringFileSystem.directory(), OPTIONS, newCachedThreadPool());
        processor.startRootProcessing();
        DiscoveredSchema discoveredTableSet = processor.get(5, TimeUnit.SECONDS);
        assertThat(discoveredTableSet.tables()).hasSize(1);
        assertThat(discoveredTableSet.tables().get(0).valid()).isFalse();
    }

    @Test
    public void testLocationNotFound()
    {
        Processor processor = new Processor(schemaDiscoveryInstances, Util.fileSystem(), Location.of("local:///definitely-not-existing-path"), OPTIONS, newCachedThreadPool());
        processor.startRootProcessing();
        assertThatThrownBy(() -> processor.get(5, TimeUnit.SECONDS))
                .rootCause()
                .asInstanceOf(InstanceOfAssertFactories.type(TrinoException.class))
                .matches(e -> e.getErrorCode().getName().equals(LOCATION_DOES_NOT_EXISTS.name()));
    }

    @Test
    public void testDatesSingleCol()
    {
        Location directory = Util.testFilePath("csv/dates_single_col");
        Processor processor = new Processor(schemaDiscoveryInstances, Util.fileSystem(), directory, OPTIONS, newCachedThreadPool());
        processor.startRootProcessing();

        assertThat(processor)
                .succeedsWithin(Duration.ofMillis(500))
                .matches(discovered -> discovered.errors().isEmpty() && discovered.tables().size() == 1 && discovered.rootPath().path().endsWith("csv/dates_single_col/"))
                .extracting(discovered -> discovered.tables().get(0))
                .matches(table -> table.valid() && table.format() == TableFormat.CSV && table.columns().columns().size() == 1 &&
                                  table.columns().columns().get(0).type().equals(new HiveType(STRING_TYPE)));
    }

    /**
     * This test should prove that we are filtering out values that schema discovery saw as either 'null' or '{}',
     * as there is no safe corresponding type in trino for them
     */
    @Test
    public void testJsonWithEmptyValues()
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO, newCachedThreadPool());
        Location directory = Util.testFilePath("json/with_empty");
        ListenableFuture<DiscoveredSchema> discoveryFuture = controller.discover(new DiscoverRequest(uriFromLocation(directory), uriFromLocation(directory), TableFormat.JSON, CSV_OPTIONS));
        DiscoveredSchema discoveredTableSet = discoveryFuture.get(5, TimeUnit.SECONDS);
        assertThat(discoveredTableSet.tables().get(0).columns().columns())
                .containsExactly(jsonWithEmptyValuesOnlyColumns());

        GeneratedOperations generatedOperations = controller.generateOperations(new GenerateOperationsRequest(discoveredTableSet, new GenerateOptions("schema123", 5, true, Optional.of("catalog123"))));
        assertThat(generatedOperations.operations())
                .element(0).isInstanceOf(Operation.CreateSchema.class);
        assertThat(generatedOperations.operations())
                .element(1).isInstanceOf(Operation.CreateTable.class);
    }

    @Test
    public void testNonOverlappingJsons()
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO, newCachedThreadPool());
        Location directory = Util.testFilePath("json/not_overlapping");
        ListenableFuture<DiscoveredSchema> discoveryFuture = controller.discover(new DiscoverRequest(uriFromLocation(directory), uriFromLocation(directory), TableFormat.JSON, CSV_OPTIONS));
        DiscoveredSchema discoveredTableSet = discoveryFuture.get(5, TimeUnit.SECONDS);

        TypeInfo filterSetTypeInfo = structType(ImmutableList.of("items"),
                ImmutableList.of(arrayType(structType(ImmutableList.of("name", "valueSet"), ImmutableList.of(HIVE_STRING, structType(ImmutableList.of("items"), ImmutableList.of(arrayType(structType(ImmutableList.of("value"), ImmutableList.of(HIVE_STRING))))))))));
        TypeInfo requestParametersTypeInfo = structType(
                ImmutableList.of("pageSize", "maxResults", "filterSet", "roleArn", "roleSessionName", "durationSeconds", "maxRecords"),
                ImmutableList.of(HIVE_INT, HIVE_INT, filterSetTypeInfo, HIVE_STRING, HIVE_STRING, HIVE_INT, HIVE_INT));
        TypeInfo tlsVersionTypeInfo = structType(
                ImmutableList.of("tlsVersion", "cipherSuite", "clientProvidedHostHeader"),
                ImmutableList.of(HIVE_STRING, HIVE_STRING, HIVE_STRING));
        TypeInfo mergedFields = arrayType(structType(
                ImmutableList.of("eventVersion", "eventTime", "requestParameters", "tlsDetails", "apiVersion"),
                ImmutableList.of(HIVE_STRING, HIVE_STRING, requestParametersTypeInfo, tlsVersionTypeInfo, HIVE_DATE)));

        assertThat(discoveredTableSet.tables().get(0).columns().columns().get(0))
                .isEqualTo(Util.column("records", mergedFields));
    }

    @Test
    public void testMixedFormatsShouldHaveErroredTable()
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO, newCachedThreadPool());
        Location directory = Util.testFilePath("mixed/mix_recursive_formats_fail");
        Map<String, String> options = ImmutableMap.<String, String>builder()
                .putAll(CSV_OPTIONS)
                .put(DISCOVERY_MODE, DiscoveryMode.RECURSIVE_DIRECTORIES.name())
                .buildKeepingLast();
        ListenableFuture<DiscoveredSchema> discoveryFuture = controller.guess(new GuessRequest(uriFromLocation(directory), options));
        DiscoveredSchema discoveredTableSet = discoveryFuture.get(5, TimeUnit.SECONDS);

        assertThat(discoveredTableSet.tables()).hasSize(1);
        assertThat(discoveredTableSet.errors()).hasSize(0);

        DiscoveredTable discoveredTable = discoveredTableSet.tables().get(0);
        assertThat(discoveredTable.valid()).isFalse();
        assertThat(discoveredTable.format()).isEqualTo(ERROR);
        assertThat(discoveredTable.errors()).hasSize(1);
        assertThat(discoveredTable.errors().get(0)).startsWith("Mismatched table formats, found: [JSON] and [ORC], at:");
    }

    @Test
    public void testParquetDifferentColumns()
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO, newCachedThreadPool());
        Location directory = Util.testFilePath("parquet_tables/columns_subset");
        ListenableFuture<DiscoveredSchema> discoveryFuture = controller.guess(new GuessRequest(uriFromLocation(directory), CSV_OPTIONS));
        DiscoveredSchema discoveredTableSet = discoveryFuture.get(5, TimeUnit.SECONDS);

        ImmutableList<Column> expectedMergedColumns = ImmutableList.of(
                new Column(toLowerCase("col1"), new HiveType(HIVE_STRING)),
                new Column(toLowerCase("col2"), new HiveType(HIVE_STRING)),
                new Column(toLowerCase("col3"), new HiveType(HIVE_STRING)),
                new Column(toLowerCase("col4"), new HiveType(HIVE_STRING)),
                new Column(toLowerCase("col5"), new HiveType(HIVE_LONG)),
                new Column(toLowerCase("col6"), new HiveType(HIVE_STRING)));

        assertThat(discoveredTableSet.tables())
                .hasSize(1)
                .allMatch(DiscoveredTable::valid);
        assertThat(discoveredTableSet.tables().get(0).columns().columns())
                .containsOnlyOnceElementsOf(expectedMergedColumns);
    }

    @Test
    public void testParquetWithEmptyFileInFolder()
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO, newCachedThreadPool());
        Location directory = Util.testFilePath("parquet_tables/with_empty_file");
        ListenableFuture<DiscoveredSchema> discoveryFuture = controller.guess(new GuessRequest(uriFromLocation(directory), CSV_OPTIONS));
        DiscoveredSchema discoveredTableSet = discoveryFuture.get(5, TimeUnit.SECONDS);

        assertThat(discoveredTableSet.tables())
                .hasSize(1)
                .allMatch(DiscoveredTable::valid);
    }

    @Test
    public void testParquetWithNoSkipHiddenFileInFolder()
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO, newCachedThreadPool());
        Location directory = Util.testFilePath("parquet_tables/with_hidden_file");
        ListenableFuture<DiscoveredSchema> discoveryFuture = controller.guess(new GuessRequest(uriFromLocation(directory), CSV_OPTIONS));
        DiscoveredSchema discoveredTableSet = discoveryFuture.get(5, TimeUnit.SECONDS);

        assertThat(discoveredTableSet.tables())
                .hasSize(1)
                .allMatch(not(DiscoveredTable::valid));
    }

    @Test
    public void testParquetWithSkipHiddenFileInFolder()
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO, newCachedThreadPool());
        Location directory = Util.testFilePath("parquet_tables/with_hidden_file");
        ListenableFuture<DiscoveredSchema> discoveryFuture = controller.guess(new GuessRequest(
                uriFromLocation(directory),
                ImmutableMap.<String, String>builder().putAll(CSV_OPTIONS).put(HIVE_SKIP_HIDDEN_FILES, "true").buildKeepingLast()));
        DiscoveredSchema discoveredTableSet = discoveryFuture.get(5, TimeUnit.SECONDS);

        assertThat(discoveredTableSet.tables())
                .hasSize(1)
                .allMatch(DiscoveredTable::valid);
    }

    @Test
    public void testDiscoveryDirectlyOnBucket()
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = new DiscoveryTrinoFileSystem(new ConstantTrinoFileSystem());
        Processor processor = new Processor(schemaDiscoveryInstances, fileSystem, ConstantTrinoFileSystem.FOLDER_PATH, OPTIONS, directExecutor());
        processor.startRootProcessing();
        List<DiscoveredTable> tables = processor.get(5, TimeUnit.SECONDS).tables();
        assertThat(tables).hasSize(1);
        assertThat(tables)
                .allMatch(DiscoveredTable::valid)
                .allMatch(t -> t.format() == JSON)
                .allMatch(t -> t.tableName().equals(new TableName(Optional.empty(), toLowerCase("dummy"))))
                .allMatch(t -> t.columns().equals(new DiscoveredColumns(ImmutableList.of(new Column(toLowerCase("key"), new HiveType(STRING_TYPE), Optional.of("value"))), ImmutableList.of())));
    }

    private static Column jsonWithEmptyValuesOnlyColumns()
    {
        return Util.column("_ab_additional_properties", HiveTypes.structType(
                ImmutableList.of("context",
                        "event", "referer", "session", "username"),
                ImmutableList.of(HiveTypes.structType(ImmutableList.of("course_id", "enterprise_uuid", "org_id", "path"), ImmutableList.of(STRING_TYPE, STRING_TYPE, STRING_TYPE, STRING_TYPE)),
                        STRING_TYPE, STRING_TYPE, STRING_TYPE, STRING_TYPE)));
    }

    private void testEmptyAsync(BiConsumer<File, Processor> proc)
            throws Exception
    {
        File tempFile = Files.createTempDirectory("xxx").toFile();
        try {
            tempFile.deleteOnExit();
            Location tempFilePath = Location.of("local://" + tempFile.getAbsolutePath());
            Processor processor = new Processor(schemaDiscoveryInstances, Util.fileSystem(), tempFilePath, OPTIONS, newCachedThreadPool());
            proc.accept(tempFile, processor);
            List<DiscoveredTable> tables = processor.get(5, TimeUnit.SECONDS).tables();
            assertThat(tables).isEmpty();
        }
        finally {
            //noinspection ResultOfMethodCallIgnored
            tempFile.delete();
        }
    }

    private void doGuessTest(Executor executor)
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO, executor);
        Location directory = Util.testFilePath("csv/coalesce");
        ListenableFuture<DiscoveredSchema> discoveryFuture = controller.guess(new GuessRequest(uriFromLocation(directory), CSV_OPTIONS));
        DiscoveredSchema discoveredTableSet = discoveryFuture.get(5, TimeUnit.SECONDS);
        validateDiscoveredTables(discoveredTableSet);
    }

    private void doErroredRescanTest(Executor executor)
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO, executor);
        Location directory = Util.testFilePath("csv");
        Location fileDirectory = Util.testFilePath("csv/coalesce");
        ListenableFuture<DiscoveredSchema> discoveryFuture = controller.discover(new DiscoverRequest(uriFromLocation(directory), uriFromLocation(fileDirectory), TableFormat.ORC, CSV_OPTIONS));
        DiscoveredSchema discoveredTableSet = discoveryFuture.get(5, TimeUnit.SECONDS);
        assertThat(discoveredTableSet.tables())
                .hasSize(1)
                .first()
                .matches(t -> !t.valid())
                .matches(t -> t.errors().size() == 5)
                .matches(t -> t.columns().equals(DiscoveredColumns.EMPTY_DISCOVERED_COLUMNS));
    }

    private void doValidRescanTest(Executor executor)
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO, executor);
        Location directory = Util.testFilePath("csv");
        Location fileDirectory = Util.testFilePath("csv/coalesce");
        ListenableFuture<DiscoveredSchema> discoveryFuture = controller.discover(new DiscoverRequest(uriFromLocation(directory), uriFromLocation(fileDirectory), TableFormat.CSV, CSV_OPTIONS));
        DiscoveredSchema discoveredTableSet = discoveryFuture.get(5, TimeUnit.SECONDS);
        validateDiscoveredTables(discoveredTableSet);
    }

    private void doDiscoverTest(Executor executor)
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO, executor);
        Location directory = Util.testFilePath("csv/coalesce");
        ListenableFuture<DiscoveredSchema> discoveryFuture = controller.discover(new DiscoverRequest(uriFromLocation(directory), uriFromLocation(directory), TableFormat.CSV, CSV_OPTIONS));
        DiscoveredSchema discoveredTableSet = discoveryFuture.get(5, TimeUnit.SECONDS);
        validateDiscoveredTables(discoveredTableSet);
    }

    private void doShallowDiscoveryTest(Executor executor)
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO, executor);
        Location directory = Util.testFilePath("csv/coalesce");
        ListenableFuture<DiscoveredSchema> shallowDiscoveryFuture = controller.discoverTablesShallow(new GuessRequest(uriFromLocation(directory), ImmutableMap.of()));
        DiscoveredSchema discoveredShallowSchema = shallowDiscoveryFuture.get(5, TimeUnit.SECONDS);
        assertThat(discoveredShallowSchema.tables())
                .hasSize(1)
                .allMatch(not(DiscoveredTable::valid));
    }

    private void validateDiscoveredTables(DiscoveredSchema discoveredTableSet)
    {
        assertThat(discoveredTableSet.tables()).hasSize(1);
        DiscoveredColumns discoveredSchema = discoveredTableSet.tables().get(0).columns();
        assertThat(discoveredSchema.columns()).containsExactly(Util.column("a", HIVE_INT).withSampleValue("1"),
                Util.column("b", adjustType(new DecimalTypeInfo(20, 0))).withSampleValue("2"),
                Util.column("c", HIVE_INT).withSampleValue("3"),
                Util.column("x", HiveTypes.HIVE_STRING).withSampleValue("hey"));
        assertThat(discoveredSchema.flags()).containsExactly(CsvFlags.HAS_QUOTED_FIELDS);
        assertThat(discoveredSchema.columns().stream().map(c -> c.sampleValue().orElse(""))).containsExactly("1", "2", "3", "hey");
    }

    private static void assertContainsBoomError(DiscoveredSchema discoveredSchema)
    {
        assertThat(discoveredSchema.tables())
                .hasSize(1)
                .first()
                .matches(t -> !t.valid())
                .matches(t -> t.errors().stream().anyMatch(error -> error.contains("boom")))
                .matches(t -> t.columns().equals(DiscoveredColumns.EMPTY_DISCOVERED_COLUMNS));
    }
}
