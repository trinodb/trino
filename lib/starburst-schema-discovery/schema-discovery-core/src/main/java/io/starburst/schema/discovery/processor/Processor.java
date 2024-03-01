/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.processor;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.starburst.schema.discovery.SchemaDiscovery;
import io.starburst.schema.discovery.TableChanges.TableName;
import io.starburst.schema.discovery.formats.lakehouse.LakehouseFormat;
import io.starburst.schema.discovery.infer.InferPartitions;
import io.starburst.schema.discovery.infer.InferredPartition;
import io.starburst.schema.discovery.infer.InferredPartitionProjection;
import io.starburst.schema.discovery.infer.TypeCoercion;
import io.starburst.schema.discovery.internal.Column;
import io.starburst.schema.discovery.internal.Errors;
import io.starburst.schema.discovery.internal.FormatGuess;
import io.starburst.schema.discovery.internal.HiveType;
import io.starburst.schema.discovery.internal.HiveTypes;
import io.starburst.schema.discovery.io.DiscoveryInput;
import io.starburst.schema.discovery.io.DiscoveryTrinoFileSystem;
import io.starburst.schema.discovery.io.DiscoveryTrinoInput;
import io.starburst.schema.discovery.models.DiscoveredColumns;
import io.starburst.schema.discovery.models.DiscoveredFormat;
import io.starburst.schema.discovery.models.DiscoveredPartitionValues;
import io.starburst.schema.discovery.models.DiscoveredPartitions;
import io.starburst.schema.discovery.models.DiscoveredPartitions.ValidatedPartitions;
import io.starburst.schema.discovery.models.DiscoveredSchema;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.LowerCaseString;
import io.starburst.schema.discovery.models.TableFormat;
import io.starburst.schema.discovery.models.TablePath;
import io.starburst.schema.discovery.options.DiscoveryMode;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.starburst.schema.discovery.options.OptionsMap;
import io.starburst.schema.discovery.processor.filetracker.FileTracker;
import io.starburst.schema.discovery.processor.filetracker.FileTrackerFactory;
import io.trino.filesystem.Location;
import io.trino.plugin.hive.projection.ProjectionType;
import io.trino.plugin.hive.type.TypeInfo;
import io.trino.spi.TrinoException;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.starburst.schema.discovery.formats.lakehouse.LakehouseUtil.enhanceIcebergTableLocationFromMetadata;
import static io.starburst.schema.discovery.infer.InferPartitions.PARTITION_SEPARATOR;
import static io.starburst.schema.discovery.internal.HiveTypes.STRING_TYPE;
import static io.starburst.schema.discovery.io.LocationUtils.directoryOrFileName;
import static io.starburst.schema.discovery.io.LocationUtils.parentOf;
import static io.starburst.schema.discovery.models.DiscoveredColumns.EMPTY_DISCOVERED_COLUMNS;
import static io.starburst.schema.discovery.models.DiscoveredFormat.EMPTY_DISCOVERED_FORMAT;
import static io.starburst.schema.discovery.models.DiscoveredTable.EMPTY_DISCOVERED_TABLE;
import static io.starburst.schema.discovery.models.LowerCaseString.toLowerCase;
import static io.starburst.schema.discovery.models.SlashEndedPath.ensureEndsWithSlash;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.reducing;

@SuppressWarnings("FormatStringAnnotation")
public class Processor
        extends ForwardingListenableFuture<DiscoveredSchema>
{
    private final Map<TableFormat, SchemaDiscovery> schemaDiscoveryInstances;
    private final DiscoveryTrinoFileSystem fileSystem;
    private final Location rootPath;
    private final OptionsMap options;
    private final Executor executor;
    private final SettableFuture<DiscoveredSchema> result = SettableFuture.create();
    private final Errors errors = new Errors();

    // intermediate results

    public record ProcessorPath(Location path, Optional<LakehouseFormat> lakehouseFormat) {}

    record ProcessorDiscoveredSchemas(Location parent, List<ProcessorFormatGuessSchema> formatGuessSchemas) {}

    record ProcessorFormatGuess(Location parent, List<Location> files, Optional<DiscoveredFormat> format) {}

    record ProcessorFormatGuessSchema(Location parent, DiscoveredFormat discoveredFormat, DiscoveredColumns columns) {}

    record ProcessorGuess(TableFormat format, FormatGuess formatGuess) {}

    record ProcessorGuessedSchema(Optional<LowerCaseString> schemaName, TableFormat format, Map<String, String> options, DiscoveredColumns columns) {}

    record ProcessorGuessedSchemaAndPartition(ProcessorGuessedSchema guessedSchema, List<InferredPartition> partitions) {}

    public Processor(Map<TableFormat, SchemaDiscovery> schemaDiscoveryInstances, DiscoveryTrinoFileSystem fileSystem, Location rootPath, OptionsMap options, Executor executor)
    {
        this.schemaDiscoveryInstances = ImmutableMap.copyOf(schemaDiscoveryInstances);
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.rootPath = requireNonNull(rootPath, "rootPath cannot be null");
        this.options = requireNonNull(options, "options is null");
        this.executor = requireNonNull(executor, "executor cannot be null");
    }

    public void startShallowProcessing()
    {
        FileTracker fileTracker = FileTrackerFactory.createFileTracker(options, rootPath);
        SampleFilesCrawler sampleFilesCrawler = new SampleFilesCrawler(fileSystem, rootPath, options, executor, fileTracker);
        var unused = FluentFuture.from(sampleFilesCrawler.startBuildSampleFilesListAsync())
                .transformAsync(this::startShallowTableLookup, executor)
                .transform(this::buildShallowTableToPartitionsMap, executor)
                .transform(this::buildShallowDiscoveredTables, executor)
                .transform(this::buildShallowDiscoveryResponse, executor)
                .catching(Throwable.class, this::setException, executor);
    }

    // process the entire tree from the root
    public void startRootProcessing()
    {
        FileTracker fileTracker = FileTrackerFactory.createFileTracker(options, rootPath);
        SampleFilesCrawler sampleFilesCrawler = new SampleFilesCrawler(fileSystem, rootPath, options, executor, fileTracker);
        FluentFuture<List<ProcessorFormatGuess>> continuation = FluentFuture.from(Futures.submitAsync(sampleFilesCrawler::startBuildSampleFilesListAsync, executor))
                .transformAsync(this::startProcessSampleFiles, executor);
        continueProcessing(continuation);
    }

    // process a single path in the tree using the given format/options
    public void startSubPathProcessing(Location fromPath, TableFormat format, Map<String, String> options)
    {
        FileTracker fileTracker = FileTrackerFactory.createFileTracker(this.options, rootPath);
        SampleFilesCrawler sampleFilesCrawler = new SampleFilesCrawler(fileSystem, fromPath, this.options, executor, fileTracker);
        DiscoveredFormat discoveredFormat = new DiscoveredFormat(format, options);
        FluentFuture<List<ProcessorFormatGuess>> continuation = FluentFuture.from(Futures.submitAsync(sampleFilesCrawler::startBuildSampleFilesListAsync, executor))
                .transformAsync(paths -> startProcessSubPathFiles(discoveredFormat, paths), executor);
        continueProcessing(continuation);
    }

    @Override
    protected ListenableFuture<DiscoveredSchema> delegate()
    {
        return result;
    }

    private void continueProcessing(FluentFuture<List<ProcessorFormatGuess>> continuation)
    {
        var unused = continuation.transformAsync(this::startProcessFormatGuesses, executor)
                .transform(this::reduceDiscoveredSchemas, executor)
                .transform(this::groupSchemas, executor)
                .transform(this::buildTablesAndSetResult, executor)
                .catching(Throwable.class, this::setException, executor);
    }

    private ListenableFuture<List<ProcessorFormatGuess>> startProcessSubPathFiles(DiscoveredFormat discoveredFormat, List<ProcessorPath> paths)
    {
        Map<Location, List<Location>> groupedByParent = paths.stream().map(ProcessorPath::path).collect(Collectors.groupingBy(Location::parentDirectory));
        ImmutableList<ListenableFuture<ProcessorFormatGuess>> futures = groupedByParent.entrySet()
                .stream()
                .map(entry -> Futures.immediateFuture(new ProcessorFormatGuess(entry.getKey(), entry.getValue(), Optional.of(discoveredFormat))))
                .collect(toImmutableList());
        return Futures.allAsList(futures);  // map of parent paths to the guessed format/options to use to do schema discovery
    }

    private ListenableFuture<List<ProcessorFormatGuess>> startProcessSampleFiles(List<ProcessorPath> samplePaths)
    {
        Map<Location, List<ProcessorPath>> groupedByParent = samplePaths.stream().collect(Collectors.groupingBy(processorPath -> parentOf(processorPath.path())));
        ImmutableList<ListenableFuture<ProcessorFormatGuess>> futures = groupedByParent.entrySet()
                .stream()
                .map(entry -> startFormatGuess(entry.getKey(), entry.getValue()))
                .collect(toImmutableList());
        return Futures.allAsList(futures);  // map of parent paths to the guessed format/options to use to do schema discovery
    }

    private ListenableFuture<List<ProcessorDiscoveredSchemas>> startProcessFormatGuesses(List<ProcessorFormatGuess> formatGuesses)
    {
        ImmutableList<ListenableFuture<ProcessorDiscoveredSchemas>> futures = formatGuesses.stream()
                .map(this::startMapFormatGuessToDiscoveredSchemas)
                .collect(toImmutableList());
        return Futures.allAsList(futures);  // list of parent -> list of discovered schemas. The discovered schemas still need to be reduced
    }

    private List<ProcessorFormatGuessSchema> reduceDiscoveredSchemas(List<ProcessorDiscoveredSchemas> discoveredSchemas)
    {
        return discoveredSchemas.stream()
                .map(discoveredSchema -> discoveredSchema.formatGuessSchemas().stream()
                        .reduce((f1, f2) -> mergeFormatGuessSchema(discoveredSchema.parent(), f1, f2))
                        .orElseGet(() -> new ProcessorFormatGuessSchema(discoveredSchema.parent(), EMPTY_DISCOVERED_FORMAT, EMPTY_DISCOVERED_COLUMNS)))
                .collect(toImmutableList());
    }

    private Map<TableAndPathKey, ? extends List<ProcessorGuessedSchemaAndPartition>> groupSchemas(List<ProcessorFormatGuessSchema> formatGuessSchemas)
    {
        GeneralOptions generalOptions = new GeneralOptions(options);
        return formatGuessSchemas.stream()
                .map(formatGuessSchema -> {
                    InferPartitions inferredPartitions = new InferPartitions(generalOptions, rootPath, formatGuessSchema.parent());
                    Optional<LowerCaseString> schemaName = generalOptions.lookForBuckets() ? Optional.empty() : inferPossibleSchemaName(formatGuessSchema.parent(), inferredPartitions);
                    ProcessorGuessedSchema guessedSchema = new ProcessorGuessedSchema(schemaName, formatGuessSchema.discoveredFormat().format(), formatGuessSchema.discoveredFormat().options(), formatGuessSchema.columns());
                    ProcessorGuessedSchemaAndPartition guessedSchemaAndPartition = new ProcessorGuessedSchemaAndPartition(guessedSchema, inferredPartitions.partitions());
                    return new SimpleEntry<>(new TableAndPathKey(new TableName(schemaName, inferredPartitions.tableName()), inferredPartitions.path()), guessedSchemaAndPartition);
                })
                .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, toImmutableList()))); // grouped by table name to list of potential tables, which need to be reduced
    }

    private Optional<LowerCaseString> inferPossibleSchemaName(Location fileParentPath, InferPartitions partitions)
    {
        if (partitions.partitions().isEmpty()) {
            return inferPossibleSchemaName(fileParentPath);
        }

        Set<String> partitionDirectoryNames = partitions.partitions()
                .stream()
                .map(p -> p.partitionProjection().isEqualSignSeparatedPartition() ? p.column().name() + PARTITION_SEPARATOR + p.value() : p.value())
                .collect(toImmutableSet());
        Location parent = parentOf(fileParentPath);

        while (!rootPath.equals(parent) && !rootPath.equals(parentOf(parent))) {
            if (!partitionDirectoryNames.contains(directoryOrFileName(parent))) {
                String possibleSchemaName = directoryOrFileName(parentOf(parent));
                return possibleSchemaName.isEmpty() ? Optional.empty() : Optional.of(toLowerCase(possibleSchemaName));
            }
            else {
                parent = parentOf(parent);
            }
        }
        return Optional.empty();
    }

    private Optional<LowerCaseString> inferPossibleSchemaName(Location parent)
    {
        if (rootPath.equals(parent) || rootPath.equals(parentOf(parent))) {
            return Optional.empty();
        }
        String possibleSchemaName = directoryOrFileName(parentOf(parent));
        return possibleSchemaName.isEmpty() ? Optional.empty() : Optional.of(toLowerCase(possibleSchemaName));
    }

    private Void buildTablesAndSetResult(Map<TableAndPathKey, ? extends List<ProcessorGuessedSchemaAndPartition>> tableToGuessesAndPartitions)
    {
        List<DiscoveredTable> tables = tableToGuessesAndPartitions.entrySet()
                .stream()
                .map(entry -> reduceGuessedSchemasAndPartitions(entry.getKey(), entry.getValue()))
                .collect(toImmutableList());
        tables = reduceRecursiveTables(tables);
        tables = tables.stream().map(this::enhanceIcebergTableLocation).collect(toImmutableList());

        DiscoveredSchema discoveredSchema = new DiscoveredSchema(ensureEndsWithSlash(rootPath), tables, errors.build());
        result.set(discoveredSchema);
        return null;
    }

    private DiscoveredTable enhanceIcebergTableLocation(DiscoveredTable table)
    {
        if (table.format() != TableFormat.ICEBERG) {
            return table;
        }
        try {
            return table.withPath(enhanceIcebergTableLocationFromMetadata(fileSystem, table.path()));
        }
        catch (Exception e) {
            errors.addTableError(table.path(), "Failed to read location from iceberg table's latest metadata file - [%s]", extractTrinoOrRootCauseMessage(e));
            return table.withErrors(errors.buildForPathAndChildren(table.path().path()));
        }
    }

    private List<DiscoveredTable> reduceRecursiveTables(List<DiscoveredTable> tables)
    {
        GeneralOptions generalOptions = new GeneralOptions(options);
        if (generalOptions.discoveryMode() != DiscoveryMode.RECURSIVE_DIRECTORIES) {
            return tables;
        }

        return tables.stream()
                .map(this::collapseTableToChildOfRoot)
                .collect(Collectors.groupingBy(DiscoveredTable::path))
                .entrySet()
                .stream()
                .map(this::reduceRecursiveTable)
                .collect(toImmutableList());
    }

    private DiscoveredTable reduceRecursiveTable(Map.Entry<TablePath, List<DiscoveredTable>> entry)
    {
        return entry.getValue().stream()
                .reduce((table1, table2) -> mergeTables(entry.getKey().toString(), table1, table2))
                .orElseGet(() -> DiscoveredTable.emptyWithPathAndErrors(entry.getKey(), ImmutableList.of("Error while processing recursive table")));
    }

    private DiscoveredTable collapseTableToChildOfRoot(DiscoveredTable t)
    {
        Location rootDirectChildPath = findRootDirectChildPath(t.path());
        return t.asRecursiveTable(
                ensureEndsWithSlash(rootDirectChildPath),
                new TableName(Optional.empty(), toLowerCase(directoryOrFileName(rootDirectChildPath))));
    }

    private Location findRootDirectChildPath(TablePath startFrom)
    {
        Location currentDirectory = Location.of(startFrom.path());
        while (!rootPath.equals(currentDirectory) && !rootPath.equals(parentOf(currentDirectory))) {
            currentDirectory = parentOf(currentDirectory);
        }
        return currentDirectory;
    }

    private ProcessorFormatGuessSchema mergeFormatGuessSchema(Location parent, ProcessorFormatGuessSchema f1, ProcessorFormatGuessSchema f2)
    {
        if (!isValid(f1.discoveredFormat().format(), f1.columns().columns())) {
            return new ProcessorFormatGuessSchema(parent, EMPTY_DISCOVERED_FORMAT, EMPTY_DISCOVERED_COLUMNS);
        }
        if (!isValid(f2.discoveredFormat().format(), f2.columns().columns())) {
            return new ProcessorFormatGuessSchema(parent, EMPTY_DISCOVERED_FORMAT, EMPTY_DISCOVERED_COLUMNS);
        }
        if (!f1.discoveredFormat().equals(f2.discoveredFormat())) {
            errors.addTableError(parent.toString(), "Format mismatch in [%s]. [%s] does not match [%s]", parent, f1.discoveredFormat(), f2.discoveredFormat());
            return new ProcessorFormatGuessSchema(parent, EMPTY_DISCOVERED_FORMAT, EMPTY_DISCOVERED_COLUMNS);
        }
        DiscoveredColumns discoveredColumns = f1.discoveredFormat().format() == TableFormat.CSV ?
                mergeDiscoveredColumnsStrictOrdering(parent.toString(), f1.columns(), f2.columns(), ImmutableList.of()) :
                mergeDiscoveredColumnsLenient(f1.columns(), f2.columns(), ImmutableList.of());
        return new ProcessorFormatGuessSchema(parent, f1.discoveredFormat(), discoveredColumns);
    }

    private ListenableFuture<ProcessorDiscoveredSchemas> startMapFormatGuessToDiscoveredSchemas(ProcessorFormatGuess formatGuess)
    {
        return formatGuess.format().map(discoveredFormat -> {
            ImmutableList<ListenableFuture<ProcessorFormatGuessSchema>> formatGuessFutures = formatGuess.files().stream()
                    .map(file -> Futures.submit(() -> startDiscoverSchema(formatGuess.parent(), file, discoveredFormat), executor))
                    .collect(toImmutableList());
            ListenableFuture<List<ProcessorFormatGuessSchema>> inverted = Futures.allAsList(formatGuessFutures);
            return new DiscoveredSchemasWorker(formatGuess.parent(), inverted, executor);
        }).orElseGet(() -> new DiscoveredSchemasWorker(formatGuess.parent(), Futures.immediateFuture(ImmutableList.of()), executor));
    }

    private ProcessorFormatGuessSchema startDiscoverSchema(Location parent, Location file, DiscoveredFormat discoveredFormat)
    {
        try {
            return switch (discoveredFormat.format()) {
                case ICEBERG, DELTA_LAKE -> new ProcessorFormatGuessSchema(parent, discoveredFormat, EMPTY_DISCOVERED_COLUMNS);
                default -> {
                    try (DiscoveryInput inputStream = new DiscoveryTrinoInput(fileSystem, file)) {
                        DiscoveredColumns columns = discoveryInstanceFromFormat(discoveredFormat.format()).discoverColumns(inputStream, discoveredFormat.options());
                        yield new ProcessorFormatGuessSchema(parent, discoveredFormat, columns);
                    }
                }
            };
        }
        catch (Exception e) {
            errors.addTableError(ensureEndsWithSlash(parent), "Error while discovering schema in format: [%s]", discoveredFormat.format());
            handleFileError(file, e);
            return new ProcessorFormatGuessSchema(parent, new DiscoveredFormat(TableFormat.ERROR, discoveredFormat.options()), EMPTY_DISCOVERED_COLUMNS);
        }
    }

    private void handleFileError(Location file, Exception e)
    {
        String trinoOrRootCauseMessage = extractTrinoOrRootCauseMessage(e);
        String discoverColumnsErrorMessage = "File error: [%s] - %s".formatted(file.toString(), trinoOrRootCauseMessage);
        errors.addTableError(file.toString(), discoverColumnsErrorMessage);
    }

    private static String extractTrinoOrRootCauseMessage(Exception e)
    {
        return Throwables.getCausalChain(e).stream().filter(ex -> ex instanceof TrinoException).findFirst().map(Throwable::getMessage)
                .orElseGet(() -> Throwables.getRootCause(e).getMessage());
    }

    private SchemaDiscovery discoveryInstanceFromFormat(TableFormat format)
    {
        return schemaDiscoveryInstances.getOrDefault(format, (__, ___) -> EMPTY_DISCOVERED_COLUMNS);
    }

    private Void setException(Throwable e)
    {
        result.setException(e);
        return null;
    }

    private DiscoveredTable reduceGuessedSchemasAndPartitions(TableAndPathKey tableAndPathKey, List<ProcessorGuessedSchemaAndPartition> discoveredSchemasAndPartitions)
    {
        String tablePath = tableAndPathKey.path().toString();
        return discoveredSchemasAndPartitions.stream()
                .map(guess -> guessToTable(tableAndPathKey.tableName().tableName(), tablePath, guess))
                .reduce((table1, table2) -> mergeTables(tablePath, table1, table2))
                .orElse(EMPTY_DISCOVERED_TABLE);
    }

    private DiscoveredTable guessToTable(LowerCaseString tableName, String tablePath, ProcessorGuessedSchemaAndPartition guess)
    {
        List<Column> partitionColumns = InferredPartition.buildColumns(guess.partitions());
        List<DiscoveredPartitionValues> partitionValues = InferredPartition.buildValues(guess.partitions());
        Map<LowerCaseString, InferredPartitionProjection> partitionProjections = guess.partitions().stream()
                .collect(toImmutableMap(p -> p.column().name(), InferredPartition::partitionProjection));
        ValidatedPartitions validatedPartitions = DiscoveredPartitions.createValidatedPartitions(partitionColumns, partitionValues, partitionProjections);
        List<LowerCaseString> buckets = InferredPartition.buildBuckets(guess.partitions());
        boolean valid = isValid(guess.guessedSchema().format(), guess.guessedSchema().columns().columns()) && validatedPartitions.errorMessage().isEmpty();
        validatedPartitions.errorMessage().ifPresent(partitionsError -> errors.addTableError(tablePath, partitionsError));
        if (!valid) {
            errors.addTableError(tablePath, "Table [%s] at [%s] is invalid - no valid columns were found", tableName, tablePath);
        }
        return new DiscoveredTable(valid,
                ensureEndsWithSlash(tablePath),
                new TableName(guess.guessedSchema().schemaName(), tableName),
                guess.guessedSchema().format(),
                guess.guessedSchema().options(),
                guess.guessedSchema().columns(),
                validatedPartitions.partitions(),
                buckets,
                errors.buildForPathAndChildren(tablePath));
    }

    private static boolean isValid(TableFormat format, List<Column> columns)
    {
        return (format != TableFormat.ERROR) && (!format.requiresColumnDefinitions() || !columns.isEmpty());
    }

    private DiscoveredTable mergeTables(String tablePath, DiscoveredTable table1, DiscoveredTable table2)
    {
        if (!table1.tableName().schemaName().equals(table2.tableName().schemaName())) {
            throw new RuntimeException("Internal error - mismatched schema names: [%s] and [%s], at: [%s]".formatted(table1.tableName().schemaName(), table2.tableName().schemaName(), tablePath));
        }
        if (table1.format() != TableFormat.ERROR && table2.format() != TableFormat.ERROR && !table1.format().equals(table2.format())) {
            errors.addTableError(tablePath, "Mismatched table formats, found: [%s] and [%s], at: [%s]".formatted(table1.format(), table2.format(), tablePath));
            return DiscoveredTable.emptyWithPathAndErrors(ensureEndsWithSlash(tablePath), errors.buildForPathAndChildren(tablePath));
        }

        List<Column> mergedPartitionColumns = mergePartitionColumns(table1.discoveredPartitions(), table2.discoveredPartitions());

        DiscoveredColumns reducedDiscoveredSchema = table1.format() == TableFormat.CSV ?
                mergeDiscoveredColumnsStrictOrdering(tablePath, table1.columns(), table2.columns(), mergedPartitionColumns) :
                mergeDiscoveredColumnsLenient(table1.columns(), table2.columns(), mergedPartitionColumns);
        if (!table1.buckets().equals(table2.buckets())) {
            errors.addTableError(tablePath, "Bucket mismatch in [%s]. [%s] does not match [%s]", tablePath, table1.buckets(), table2.buckets());
            return DiscoveredTable.emptyWithPathAndErrors(ensureEndsWithSlash(tablePath), errors.buildForPathAndChildren(tablePath));
        }

        ValidatedPartitions validatedUnionedPartitions = createDiscoveredPartitions(table1, table2, mergedPartitionColumns);
        boolean isValid = (table1.valid() && table2.valid()) && isValid(table1.format(), reducedDiscoveredSchema.columns()) && validatedUnionedPartitions.errorMessage().isEmpty();
        validatedUnionedPartitions.errorMessage().ifPresent(partitionsError -> errors.addTableError(tablePath, partitionsError));

        return new DiscoveredTable(isValid,
                ensureEndsWithSlash(tablePath),
                table1.tableName(),
                table1.format(),
                table1.options(),
                reducedDiscoveredSchema,
                validatedUnionedPartitions.partitions(),
                table1.buckets(),
                errors.buildForPathAndChildren(tablePath));
    }

    private InferredPartitionProjection mergeProjectionType(InferredPartitionProjection partitionProjection1, InferredPartitionProjection partitionProjection2)
    {
        boolean mergedIsStandardPartition = partitionProjection1.isEqualSignSeparatedPartition() && partitionProjection2.isEqualSignSeparatedPartition();
        ProjectionType mergedProjectionType = partitionProjection1.projectionType() == partitionProjection2.projectionType() ?
                partitionProjection1.projectionType() :
                ProjectionType.INJECTED;
        return new InferredPartitionProjection(mergedIsStandardPartition, mergedProjectionType);
    }

    private List<Column> mergePartitionColumns(DiscoveredPartitions partitions1, DiscoveredPartitions partitions2)
    {
        Map<LowerCaseString, Optional<Column>> groupedColumns = Stream.concat(partitions1.columns().stream(), partitions2.columns().stream())
                .collect(Collectors.groupingBy(Column::name, LinkedHashMap::new, reducing(this::reducePartitionColumn)));
        return groupedColumns.entrySet().stream()
                .map(entry -> entry.getValue().orElseGet(() -> new Column(entry.getKey(), new HiveType(HiveTypes.adjustType(STRING_TYPE)))))
                .collect(toImmutableList());
    }

    private Column reducePartitionColumn(Column column1, Column column2)
    {
        checkArgument(column1.name().equals(column2.name()));
        TypeInfo typeInfo = TypeCoercion.compatibleType(column1.type().typeInfo(), column2.type().typeInfo()).orElse(STRING_TYPE);
        return new Column(column1.name(), new HiveType(HiveTypes.adjustType(typeInfo)));
    }

    private DiscoveredColumns mergeDiscoveredColumnsStrictOrdering(String errorContext, DiscoveredColumns d1, DiscoveredColumns d2, List<Column> mergedPartitionColumns)
    {
        DiscoveredColumns smaller;
        DiscoveredColumns larger;
        if (d1.columns().size() <= d2.columns().size()) {
            smaller = d1;
            larger = d2;
        }
        else {
            smaller = d2;
            larger = d1;
        }

        Collection<LowerCaseString> partitionColumnNames = mergedPartitionColumns.stream().map(Column::name).collect(toImmutableSet());

        ArrayList<Column> reducedColumns = new ArrayList<>();
        for (int i = 0; i < Math.min(smaller.columns().size(), larger.columns().size()); ++i) {
            Column d1Column = smaller.columns().get(i);
            Column d2Column = larger.columns().get(i);
            if (!d1Column.name().equals(d2Column.name())) {
                errors.addTableError(errorContext, "Discovered columns in [%s] do not match with each other. Found: [%s], and also: [%s]",
                        errorContext, smaller.columns().stream().map(Column::name).collect(toImmutableList()), larger.columns().stream().map(Column::name).collect(toImmutableList()));
                errors.addTableError(errorContext, "Column name mismatch in [%s] at index [%d]. [%s] does not match [%s]", errorContext, i, d1Column.name(), d2Column.name());
                return EMPTY_DISCOVERED_COLUMNS;
            }
            if (partitionColumnNames.contains(d1Column.name())) {
                continue;
            }
            Optional<TypeInfo> reducedTypeInfo = TypeCoercion.compatibleType(d1Column.type().typeInfo(), d2Column.type().typeInfo());
            TypeInfo reducedType = HiveTypes.adjustType(reducedTypeInfo.orElse(STRING_TYPE));
            reducedColumns.add(new Column(d1Column.name(), new HiveType(reducedType), d1Column.sampleValue()));
        }
        for (int i = smaller.columns().size(); i < larger.columns().size(); ++i) {
            Column column = larger.columns().get(i);
            if (partitionColumnNames.contains(column.name())) {
                continue;
            }
            reducedColumns.add(column);
        }

        HashSet<String> unionedFlags = new HashSet<>(smaller.flags());
        unionedFlags.addAll(larger.flags());
        return new DiscoveredColumns(reducedColumns, unionedFlags);
    }

    private DiscoveredColumns mergeDiscoveredColumnsLenient(DiscoveredColumns d1, DiscoveredColumns d2, List<Column> mergedPartitionColumns)
    {
        Map<LowerCaseString, Column> columnsMap1 = d1.columns().stream().collect(toImmutableMap(Column::name, Function.identity()));
        Map<LowerCaseString, Column> columnsMap2 = d2.columns().stream().collect(toImmutableMap(Column::name, Function.identity()));

        Set<LowerCaseString> partitionColumnNames = mergedPartitionColumns.stream().map(Column::name).collect(toImmutableSet());

        Set<LowerCaseString> sortedUniqueColumnNames = new LinkedHashSet<>();
        d1.columns().forEach(c -> sortedUniqueColumnNames.add(c.name()));
        d2.columns().forEach(c -> sortedUniqueColumnNames.add(c.name()));

        ArrayList<Column> reducedColumns = new ArrayList<>();
        for (LowerCaseString columnName : sortedUniqueColumnNames) {
            if (partitionColumnNames.contains(columnName)) {
                continue;
            }

            Column column1Or2 = columnsMap1.getOrDefault(columnName, columnsMap2.get(columnName));
            Column column2Or1 = columnsMap2.getOrDefault(columnName, columnsMap1.get(columnName));
            if (column1Or2 == null || column2Or1 == null) {
                throw new RuntimeException("Something went wrong when merging columns, [%s] not found in any of columns: [%s], [%s]".formatted(columnName, columnsMap1.keySet(), columnsMap2.keySet()));
            }

            // if column is present only in one of maps, it will be equal here
            if (column1Or2.equals(column2Or1)) {
                reducedColumns.add(column1Or2);
                continue;
            }

            Optional<TypeInfo> reducedTypeInfo = TypeCoercion.compatibleType(column1Or2.type().typeInfo(), column2Or1.type().typeInfo());
            TypeInfo reducedType = HiveTypes.adjustType(reducedTypeInfo.orElse(STRING_TYPE));
            reducedColumns.add(new Column(columnName, new HiveType(reducedType), column1Or2.sampleValue()));
        }

        HashSet<String> unionedFlags = new HashSet<>(d1.flags());
        unionedFlags.addAll(d2.flags());
        return new DiscoveredColumns(reducedColumns, unionedFlags);
    }

    private ListenableFuture<ProcessorFormatGuess> startFormatGuess(Location parent, List<ProcessorPath> processorPaths)
    {
        return Futures.submit(() -> {
            ProcessorFormatGuess formatGuess = guessDirectoryFormat(parent, processorPaths);
            if (formatGuess.format().isEmpty()) {
                errors.addTableError(parent.toString(), "No format could be matched to file: [%s]", processorPaths.get(0).path());
            }
            return formatGuess;
        }, executor);
    }

    private ProcessorFormatGuess guessDirectoryFormat(Location parent, List<ProcessorPath> processorPaths)
    {
        ProcessorPath firstProcessorPath = processorPaths.get(0);   // only the first file in a directory is used to determine format/options
        List<Location> paths = processorPaths.stream().map(ProcessorPath::path).collect(toImmutableList());

        if (firstProcessorPath.lakehouseFormat().isPresent()) {
            LakehouseFormat lakehouseFormat = firstProcessorPath.lakehouseFormat().get();
            return new ProcessorFormatGuess(parent, paths, Optional.of(new DiscoveredFormat(lakehouseFormat.format(), GeneralOptions.DEFAULT_OPTIONS)));
        }
        else {
            Location firstPath = firstProcessorPath.path();
            TableName guessedTableName = new TableName(inferPossibleSchemaName(parent), toLowerCase(directoryOrFileName(parent)));
            OptionsMap optionsForTableName = this.options.withTableName(guessedTableName);
            GeneralOptions generalOptions = new GeneralOptions(optionsForTableName);

            Optional<DiscoveredFormat> forcedDiscoveredFormat = generalOptions.forcedFormat()
                    .map(format -> new DiscoveredFormat(format, optionsForTableName.unwrap()));
            if (forcedDiscoveredFormat.isPresent()) {
                return new ProcessorFormatGuess(parent, paths, forcedDiscoveredFormat);
            }

            Optional<TableFormat> formatMatchByFileExtension = Stream.of(TableFormat.values())
                    .filter(format -> format.isFileMatching(firstPath))
                    .findFirst();
            if (!generalOptions.skipFileExtensionCheck() && formatMatchByFileExtension.isPresent()) {
                return new ProcessorFormatGuess(parent, paths, formatMatchByFileExtension.map(f -> new DiscoveredFormat(f, optionsForTableName.unwrap())));
            }

            try (DiscoveryTrinoInput inputStream = new DiscoveryTrinoInput(fileSystem, firstPath)) {
                Optional<DiscoveredFormat> formatMatchByFileInspection = Stream.of(TableFormat.values())
                        .flatMap(format -> checkFormatMatch(inputStream.rewind(), format).stream())
                        .sorted(this::compareMatches)
                        .map(guess -> new DiscoveredFormat(guess.format(), appliedGuessOptions(guess)))
                        .findFirst();
                return new ProcessorFormatGuess(parent, paths, formatMatchByFileInspection);
            }
            catch (Exception e) {
                errors.addTableError(ensureEndsWithSlash(parent), "Error while discovering format for table: [%s], fail on file: [%s]".formatted(parent, firstPath));
                handleFileError(firstPath, e);
                return new ProcessorFormatGuess(parent, paths, Optional.of(EMPTY_DISCOVERED_FORMAT));
            }
        }
    }

    private ListenableFuture<List<TableAndPathKey>> startShallowTableLookup(List<ProcessorPath> sampleFiles)
    {
        Map<Location, List<ProcessorPath>> groupedByParent = sampleFiles.stream().collect(Collectors.groupingBy(processorPath -> parentOf(processorPath.path())));
        ImmutableList<ListenableFuture<TableAndPathKey>> futures = groupedByParent.keySet()
                .stream()
                .map(this::buildShallowTables)
                .collect(toImmutableList());
        return Futures.allAsList(futures);  // map of parent paths to the guessed schema/table
    }

    private ListenableFuture<TableAndPathKey> buildShallowTables(Location parent)
    {
        return Futures.submit(() -> {
            TableName guessedTableName = new TableName(inferPossibleSchemaName(parent), toLowerCase(directoryOrFileName(parent)));
            return new TableAndPathKey(guessedTableName, parent);
        }, executor);
    }

    private Map<TableAndPathKey, ? extends List<ProcessorGuessedSchemaAndPartition>> buildShallowTableToPartitionsMap(List<TableAndPathKey> tableAndPathKeys)
    {
        return tableAndPathKeys.stream()
                .map(tableAndPath -> {
                    OptionsMap optionsForTableName = this.options.withTableName(tableAndPath.tableName());
                    GeneralOptions generalOptions = new GeneralOptions(optionsForTableName);

                    InferPartitions inferredPartitions = new InferPartitions(generalOptions, rootPath, tableAndPath.path());
                    Optional<LowerCaseString> schemaName = generalOptions.lookForBuckets() ? Optional.empty() : inferPossibleSchemaName(tableAndPath.path(), inferredPartitions);
                    ProcessorGuessedSchema guessedSchema = new ProcessorGuessedSchema(schemaName, TableFormat.ERROR, optionsForTableName.unwrap(), EMPTY_DISCOVERED_COLUMNS);
                    ProcessorGuessedSchemaAndPartition guessedSchemaAndPartition = new ProcessorGuessedSchemaAndPartition(guessedSchema, inferredPartitions.partitions());

                    return new SimpleEntry<>(new TableAndPathKey(new TableName(schemaName, inferredPartitions.tableName()), inferredPartitions.path()), guessedSchemaAndPartition);
                })
                .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, toImmutableList())));
    }

    private List<DiscoveredTable> buildShallowDiscoveredTables(Map<TableAndPathKey, ? extends List<ProcessorGuessedSchemaAndPartition>> tablesWithPartitions)
    {
        return tablesWithPartitions.entrySet()
                .stream()
                .map(entry -> {
                    TableAndPathKey tablePathAndKey = entry.getKey();
                    String tablePath = tablePathAndKey.path().toString();
                    List<ProcessorGuessedSchemaAndPartition> discoveredPartitions = entry.getValue();

                    return discoveredPartitions.stream()
                            .map(partition -> guessToTable(tablePathAndKey.tableName().tableName(), tablePath, partition))
                            .reduce((table1, table2) -> mergeTablesShallow(tablePath, table1, table2))
                            .orElse(EMPTY_DISCOVERED_TABLE);
                })
                .filter(table -> !table.path().isEmpty())
                .collect(toImmutableList());
    }

    private Void buildShallowDiscoveryResponse(List<DiscoveredTable> shallowTables)
    {
        List<DiscoveredTable> pathSortedTables = reduceRecursiveTables(shallowTables)
                .stream()
                .sorted(Comparator.comparing(DiscoveredTable::path))
                .collect(toImmutableList());

        DiscoveredSchema discoveredSchema = new DiscoveredSchema(ensureEndsWithSlash(rootPath), pathSortedTables, errors.build());
        result.set(discoveredSchema);
        return null;
    }

    private DiscoveredTable mergeTablesShallow(String tablePath, DiscoveredTable table1, DiscoveredTable table2)
    {
        if (!table1.tableName().schemaName().equals(table2.tableName().schemaName())) {
            throw new RuntimeException("Internal error - mismatched schema names: [%s] and [%s], at: [%s]".formatted(table1.tableName().schemaName(), table2.tableName().schemaName(), tablePath));
        }
        if (!table1.buckets().equals(table2.buckets())) {
            errors.addTableError(tablePath, "Bucket mismatch in [%s]. [%s] does not match [%s]", tablePath, table1.buckets(), table2.buckets());
            return DiscoveredTable.emptyWithPathAndErrors(ensureEndsWithSlash(tablePath), errors.buildForPathAndChildren(tablePath));
        }

        List<Column> mergedPartitionColumns = mergePartitionColumns(table1.discoveredPartitions(), table2.discoveredPartitions());
        ValidatedPartitions validatedUnionedPartitions = createDiscoveredPartitions(table1, table2, mergedPartitionColumns);
        validatedUnionedPartitions.errorMessage().ifPresent(partitionError -> errors.addTableError(tablePath, partitionError));

        return new DiscoveredTable(
                false,
                ensureEndsWithSlash(tablePath),
                table1.tableName(),
                TableFormat.ERROR,
                table1.options(),
                EMPTY_DISCOVERED_COLUMNS,
                validatedUnionedPartitions.partitions(),
                table1.buckets(),
                errors.buildForPathAndChildren(tablePath));
    }

    private ValidatedPartitions createDiscoveredPartitions(DiscoveredTable table1, DiscoveredTable table2, List<Column> mergedPartitionColumns)
    {
        ImmutableList<DiscoveredPartitionValues> unionedPartitionValues = ImmutableList.<DiscoveredPartitionValues>builder()
                .addAll(table1.discoveredPartitions().values())
                .addAll(table2.discoveredPartitions().values())
                .build();
        Map<LowerCaseString, InferredPartitionProjection> unionedProjectionTypes = Stream.concat(
                table1.discoveredPartitions().columnProjections().entrySet().stream(),
                table2.discoveredPartitions().columnProjections().entrySet().stream()
        ).collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue, this::mergeProjectionType));
        return DiscoveredPartitions.createValidatedPartitions(mergedPartitionColumns, unionedPartitionValues, unionedProjectionTypes);
    }

    private Map<String, String> appliedGuessOptions(ProcessorGuess guess)
    {
        return options.withDefaults(guess.formatGuess().options()).unwrap();
    }

    private Optional<ProcessorGuess> checkFormatMatch(DiscoveryInput inputStream, TableFormat format)
    {
        inputStream.rewind();
        return discoveryInstanceFromFormat(format).checkFormatMatch(inputStream).map(formatGuess -> new ProcessorGuess(format, formatGuess));
    }

    private int compareMatches(ProcessorGuess guess1, ProcessorGuess guess2)
    {
        int guess1Confidence = (guess1.formatGuess().confidence() == io.starburst.schema.discovery.internal.FormatGuess.Confidence.HIGH) ? 0 : 1;
        int guess2Confidence = (guess2.formatGuess().confidence() == io.starburst.schema.discovery.internal.FormatGuess.Confidence.HIGH) ? 0 : 1;
        int diff = guess1Confidence - guess2Confidence;
        if (diff == 0) {
            int ordinalDiff = guess1.format().ordinal() - guess2.format().ordinal();    // TODO - can't think of anything better than ordinal
            diff = Integer.compare(ordinalDiff, 0);
        }
        return diff;
    }
}
