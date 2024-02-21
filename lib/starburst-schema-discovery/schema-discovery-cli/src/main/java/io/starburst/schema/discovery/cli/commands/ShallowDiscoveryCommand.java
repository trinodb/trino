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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.json.ObjectMapperProvider;
import io.starburst.schema.discovery.SchemaDiscoveryController;
import io.starburst.schema.discovery.formats.orc.OrcDataSourceFactory;
import io.starburst.schema.discovery.formats.parquet.ParquetDataSourceFactory;
import io.starburst.schema.discovery.generation.Dialect;
import io.starburst.schema.discovery.models.DiscoveredSchema;
import io.starburst.schema.discovery.request.GuessRequest;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.orc.HdfsOrcDataSource;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.starburst.schema.discovery.generation.Dialect.TRINO;
import static io.starburst.schema.discovery.options.GeneralOptions.DEFAULT_OPTIONS;
import static io.starburst.schema.discovery.options.GeneralOptions.MAX_SAMPLE_FILES_PER_TABLE;
import static io.starburst.schema.discovery.options.GeneralOptions.MAX_SAMPLE_TABLES;
import static java.util.Locale.ENGLISH;
import static picocli.CommandLine.Help.Visibility.ALWAYS;

@Command(name = "shallow_discover", mixinStandardHelpOptions = true)
public final class ShallowDiscoveryCommand
        extends DiscoveryCommandBase
        implements Runnable
{
    @Parameters(description = "URI to scan (e.g. s3://mys3/bucket)", arity = "1", paramLabel = "URI")
    private String uri;

    @Option(names = {"-t", "--timeout"}, description = "Schema Discovery timeout", showDefaultValue = ALWAYS)
    private Duration timeout = Duration.ofSeconds(30);

    @Option(names = {"-o", "--option"}, description = "Discovery options - only [maxSampleFilesPerTable, maxSampleTables, excludePatterns, includePatterns] are used in this mode")
    private Map<String, String> options = Map.of();

    @Option(names = "--dialect", description = "Dialect of hive connector. Trino or Galxy.", showDefaultValue = ALWAYS)
    private String dialect = TRINO.name();

    @Override
    public void run()
    {
        ExecutorService executorService = Executors.newCachedThreadPool();
        try {
            URI parsedUri = new URI(uri);
            Map<String, String> defaultOverwrite = ImmutableMap.of(
                    MAX_SAMPLE_FILES_PER_TABLE, "1",
                    MAX_SAMPLE_TABLES, "200");
            Map<String, String> shallowDiscoveryOptions = ImmutableMap.<String, String>builder()
                    .putAll(DEFAULT_OPTIONS)
                    .putAll(defaultOverwrite)
                    .putAll(options)
                    .buildKeepingLast();

            OrcDataSourceFactory orcDataSourceFactory = (id, size, options, inputFile) -> new HdfsOrcDataSource(id, size, options, inputFile, new FileFormatDataSourceStats());
            ParquetDataSourceFactory parquetDataSourceFactory = (inputFile) -> new TrinoParquetDataSource(inputFile, new ParquetReaderOptions(), new FileFormatDataSourceStats());
            SchemaDiscoveryController controller = new SchemaDiscoveryController(ShallowDiscoveryCommand::getFileSystem, parquetDataSourceFactory, orcDataSourceFactory, Dialect.valueOf(dialect.toUpperCase(ENGLISH)), executorService);
            ListenableFuture<DiscoveredSchema> discoveredSchemaListenableFuture = controller.discoverTablesShallow(new GuessRequest(parsedUri, shallowDiscoveryOptions));

            DiscoveredSchema discoveredSchema = discoveredSchemaListenableFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            System.out.println(new ObjectMapperProvider().get().writerWithDefaultPrettyPrinter().writeValueAsString(discoveredSchema));
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
        catch (JsonProcessingException e) {
            throw new RuntimeException("Error while processing json", e);
        }
        finally {
            executorService.shutdownNow();
        }
    }
}
