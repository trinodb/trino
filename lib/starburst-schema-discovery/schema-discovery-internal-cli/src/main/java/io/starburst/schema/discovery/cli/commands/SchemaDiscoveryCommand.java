/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.cli.commands;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.json.ObjectMapperProvider;
import io.starburst.schema.discovery.SchemaDiscoveryController;
import io.starburst.schema.discovery.cli.OutputType;
import io.starburst.schema.discovery.formats.csv.CsvOptions;
import io.starburst.schema.discovery.formats.orc.OrcDataSourceFactory;
import io.starburst.schema.discovery.formats.parquet.ParquetDataSourceFactory;
import io.starburst.schema.discovery.generation.Dialect;
import io.starburst.schema.discovery.internal.RescanMerger;
import io.starburst.schema.discovery.models.DiscoveredSchema;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.GeneratedOperations;
import io.starburst.schema.discovery.models.TableFormat;
import io.starburst.schema.discovery.request.DiscoverRequest;
import io.starburst.schema.discovery.request.GenerateOperationDifferencesRequest;
import io.starburst.schema.discovery.request.GenerateOperationsRequest;
import io.starburst.schema.discovery.request.GenerateOptions;
import io.starburst.schema.discovery.request.GuessRequest;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.orc.HdfsOrcDataSource;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.starburst.schema.discovery.generation.Dialect.TRINO;
import static java.util.Locale.ENGLISH;
import static picocli.CommandLine.Help.Visibility.ALWAYS;

@SuppressWarnings({"FieldCanBeLocal", "FieldMayBeFinal"})
@Command(name = "discover", mixinStandardHelpOptions = true)
public final class SchemaDiscoveryCommand
        extends DiscoveryCommandBase
        implements Runnable
{
    @Parameters(description = "URI to scan (e.g. s3://mys3/bucket)", arity = "1", paramLabel = "URI")
    private String uri;

    @Option(names = {"-o", "--option"}, description = "Discovery options - see below for details")
    private Map<String, String> options = Map.of();

    @Option(names = {"-t", "--timeout"}, description = "Schema Discovery timeout", showDefaultValue = ALWAYS)
    private Duration timeout = Duration.ofMinutes(5);

    @Option(names = "--format", description = "Output format - Valid values: ${COMPLETION-CANDIDATES}", showDefaultValue = ALWAYS)
    private Set<OutputType> outputType = ImmutableSet.of(OutputType.SQL);

    @Option(names = {"-s", "--schema"}, description = "Schema name", showDefaultValue = ALWAYS)
    private String schemaName = "discovered";

    @Option(names = "--buckets", description = "Bucket qty", showDefaultValue = ALWAYS)
    private int bucketQty = 10;

    @Option(names = "--partitions", description = "Include partitions", showDefaultValue = ALWAYS)
    private boolean includePartitions = true;

    @Option(names = "--previous", description = "Previous metadata - output will be changes/diff from previous")
    private Optional<String> previousMetadata = Optional.empty();

    @Option(names = "--catalog", description = "Catalog name to use for table creation")
    private Optional<String> catalogName = Optional.empty();

    @Option(names = "--dialect", description = "Dialect of hive connector. Trino or Galxy.", showDefaultValue = ALWAYS)
    private String dialect = TRINO.name();

    @ArgGroup(exclusive = false)
    private Rescan rescan;

    private final ObjectMapper objectMapper = new ObjectMapperProvider().get();

    private static class Rescan
    {
        @Option(names = "--rescan-uri", description = "To rescan a single table - URI to rescan (requires --rescan-type)", required = true)
        private String uri = "";

        @Option(names = "--rescan-type", description = "To rescan a single table - discovery type to use (requires --rescan-uri) - Valid values: ${COMPLETION-CANDIDATES}", required = true)
        private TableFormat format = TableFormat.ERROR;

        @Option(names = "--rescan-metadata", description = "Metadata from initial scan. If provided, generated output will contain initial scan combined with this rescan")
        private Optional<String> metadata = Optional.empty();
    }

    @Override
    public void run()
    {
        Map<String, String> combinedOptions = new HashMap<>(CsvOptions.standard());
        combinedOptions.putAll(options);
        ExecutorService executorService = Executors.newCachedThreadPool();
        try {
            URI parsedUri = new URI(uri);
            SchemaDiscoveryController controller = buildSchemaDiscoveryController(executorService);

            ListenableFuture<DiscoveredSchema> guess;
            if (rescan != null) {
                DiscoverRequest discover = new DiscoverRequest(parsedUri, URI.create(rescan.uri), rescan.format, combinedOptions);
                guess = controller.discover(discover);
            }
            else {
                guess = controller.guess(new GuessRequest(parsedUri, combinedOptions));
            }

            DiscoveredSchema discoveredSchema = mergeRescan(guess.get(timeout.toMillis(), TimeUnit.MILLISECONDS));

            GenerateOptions generateOptions = new GenerateOptions(schemaName, bucketQty, includePartitions, catalogName);
            GeneratedOperations operations = previousMetadata.map(previous -> generateDiff(parsedUri, controller, previous, discoveredSchema, generateOptions)).orElseGet(() -> generateOperations(controller, discoveredSchema, generateOptions));

            discoveredSchema.errors().forEach(error -> System.err.println("! " + error));
            outputType.forEach(type -> {
                System.out.println("===== " + type + " =====");
                System.out.println(output(type, discoveredSchema, operations));
                System.out.println();
            });
        }
        catch (URISyntaxException e) {
            throw new RuntimeException("Cannot parse URI: %s - %s".formatted(uri, e.getMessage()));
        }
        catch (TimeoutException | InterruptedException e) {
            throw new RuntimeException("Timed out discovering schema for URI: %s".formatted(uri));
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Error during schema discovery for URI: %s - %s".formatted(uri, e.getMessage()));
        }
        finally {
            executorService.shutdownNow();
        }
    }

    private DiscoveredSchema mergeRescan(DiscoveredSchema discoveredSchema)
    {
        if ((rescan == null) || rescan.metadata.isEmpty()) {
            return discoveredSchema;
        }

        return RescanMerger.mergeRescan(discoveredSchema, objectMapper, rescan.metadata.get());
    }

    private GeneratedOperations generateDiff(URI uri, SchemaDiscoveryController controller, String previousMetadataJson, DiscoveredSchema discoveredSchema, GenerateOptions generateOptions)
    {
        List<DiscoveredTable> previousTables;
        try {
            previousTables = objectMapper.readValue(previousMetadataJson, new TypeReference<>() {});
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException("Previous metadata is invalid", e);
        }
        return controller.generateOperationDifferences(new GenerateOperationDifferencesRequest(uri, previousTables, discoveredSchema.tables(), options, generateOptions));
    }

    private GeneratedOperations generateOperations(SchemaDiscoveryController controller, DiscoveredSchema discoveredSchema, GenerateOptions generateOptions)
    {
        return controller.generateOperations(new GenerateOperationsRequest(discoveredSchema, generateOptions));
    }

    private SchemaDiscoveryController buildSchemaDiscoveryController(ExecutorService executorService)
    {
        OrcDataSourceFactory orcDataSourceFactory = (id, size, options, inputFile) -> new HdfsOrcDataSource(id, size, options, inputFile, new FileFormatDataSourceStats());
        ParquetDataSourceFactory parquetDataSourceFactory = (inputFile) -> new TrinoParquetDataSource(inputFile, new ParquetReaderOptions(), new FileFormatDataSourceStats());
        return new SchemaDiscoveryController(SchemaDiscoveryCommand::getFileSystem, parquetDataSourceFactory, orcDataSourceFactory, Dialect.valueOf(dialect.toUpperCase(ENGLISH)), executorService);
    }

    private String output(OutputType outputType, DiscoveredSchema discoveredSchema, GeneratedOperations operations)
    {
        try {
            return switch (outputType) {
                case SQL -> String.join("", operations.sql());
                case JSON -> objectMapper.writeValueAsString(operations.operations());
                case METADATA -> objectMapper.writeValueAsString(discoveredSchema.tables());
            };
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
