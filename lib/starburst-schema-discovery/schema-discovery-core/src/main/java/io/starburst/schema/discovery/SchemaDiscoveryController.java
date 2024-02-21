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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.starburst.schema.discovery.formats.csv.CsvSchemaDiscovery;
import io.starburst.schema.discovery.formats.json.JsonSchemaDiscovery;
import io.starburst.schema.discovery.formats.orc.OrcDataSourceFactory;
import io.starburst.schema.discovery.formats.orc.OrcSchemaDiscovery;
import io.starburst.schema.discovery.formats.parquet.ParquetDataSourceFactory;
import io.starburst.schema.discovery.formats.parquet.ParquetSchemaDiscovery;
import io.starburst.schema.discovery.generation.Dialect;
import io.starburst.schema.discovery.generation.DiscoveredTablesDiffGenerator;
import io.starburst.schema.discovery.generation.DiscoveredTablesDiffGenerator.DiffTable;
import io.starburst.schema.discovery.generation.OperationGenerator;
import io.starburst.schema.discovery.internal.TableChangesBuilder;
import io.starburst.schema.discovery.io.DiscoveryTrinoFileSystem;
import io.starburst.schema.discovery.models.DiscoveredFormat;
import io.starburst.schema.discovery.models.DiscoveredSchema;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.GeneratedOperations;
import io.starburst.schema.discovery.models.Operation;
import io.starburst.schema.discovery.models.TableFormat;
import io.starburst.schema.discovery.options.OptionsMap;
import io.starburst.schema.discovery.processor.Processor;
import io.starburst.schema.discovery.request.DiscoverRequest;
import io.starburst.schema.discovery.request.GenerateOperationDifferencesRequest;
import io.starburst.schema.discovery.request.GenerateOperationsRequest;
import io.starburst.schema.discovery.request.GenerateSqlRequest;
import io.starburst.schema.discovery.request.GuessRequest;
import io.trino.filesystem.Location;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static io.starburst.schema.discovery.models.SlashEndedPath.ensureEndsWithSlash;
import static java.util.Objects.requireNonNull;

public class SchemaDiscoveryController
{
    private final Executor executor;
    private final Function<URI, DiscoveryTrinoFileSystem> fileSystemSupplier;
    private final Map<TableFormat, SchemaDiscovery> schemaDiscoveryInstances;
    private final Dialect dialect;

    public SchemaDiscoveryController(
            Function<URI, DiscoveryTrinoFileSystem> fileSystemSupplier,
            ParquetDataSourceFactory parquetDataSourceFactory,
            OrcDataSourceFactory orcDataSourceFactory,
            Dialect dialect)
    {
        this(fileSystemSupplier, parquetDataSourceFactory, orcDataSourceFactory, dialect, MoreExecutors.directExecutor());
    }

    public SchemaDiscoveryController(
            Function<URI, DiscoveryTrinoFileSystem> fileSystemSupplier,
            ParquetDataSourceFactory parquetDataSourceFactory,
            OrcDataSourceFactory orcDataSourceFactory,
            Dialect dialect,
            Executor executor)
    {
        this.fileSystemSupplier = requireNonNull(fileSystemSupplier, "fileSystemSupplier is null");
        this.dialect = requireNonNull(dialect, "dialect is null");
        this.executor = requireNonNull(executor, "executor is null");
        schemaDiscoveryInstances = ImmutableMap.of(
                TableFormat.CSV, CsvSchemaDiscovery.INSTANCE,
                TableFormat.JSON, JsonSchemaDiscovery.INSTANCE,
                TableFormat.PARQUET, new ParquetSchemaDiscovery(parquetDataSourceFactory),
                TableFormat.ORC, new OrcSchemaDiscovery(orcDataSourceFactory));
    }

    public ListenableFuture<DiscoveredSchema> guess(GuessRequest guess)
    {
        URI uri = normalizeUri(guess.uri());
        Processor processor = new Processor(schemaDiscoveryInstances, fileSystemSupplier.apply(uri), Location.of(uri.toString()), new OptionsMap(guess.options()), executor);
        processor.startRootProcessing();
        return processor;
    }

    public ListenableFuture<DiscoveredSchema> discover(DiscoverRequest discover)
    {
        URI uri = normalizeUri(discover.uri());
        Location rootPath = Location.of(uri.toString());
        DiscoveredFormat format = new DiscoveredFormat(discover.format(), discover.options());
        Processor processor = new Processor(schemaDiscoveryInstances, fileSystemSupplier.apply(uri), rootPath, new OptionsMap(format.options()), executor);
        URI discoverUri = normalizeUri(discover.path());
        Location path = Location.of(discoverUri.toString());
        processor.startSubPathProcessing(path, format.format(), format.options());
        return processor;
    }

    public ListenableFuture<DiscoveredSchema> discoverTablesShallow(GuessRequest guess)
    {
        URI uri = normalizeUri(guess.uri());
        Processor processor = new Processor(schemaDiscoveryInstances, fileSystemSupplier.apply(uri), Location.of(uri.toString()), new OptionsMap(guess.options()), executor);
        processor.startShallowProcessing();
        return processor;
    }

    public GeneratedOperations generateOperations(GenerateOperationsRequest generateSchema)
    {
        OperationGenerator operationGenerator = new OperationGenerator(generateSchema.generateOptions(), dialect);
        List<Operation> operations = operationGenerator.generateInitial(generateSchema.schema());
        List<String> sql = operationGenerator.generateSql(generateSchema.generateOptions().defaultSchemaName(), operations);
        return new GeneratedOperations(sql, operations);
    }

    public GeneratedOperations generateOperationDifferences(GenerateOperationDifferencesRequest difference)
    {
        Location rootPath = Location.of(difference.uri().toString());
        TableChangesBuilder builder = new TableChangesBuilder(ensureEndsWithSlash(rootPath));
        difference.oldTables().forEach(builder::addPreviousTable);
        difference.updatedTables().forEach(builder::addCurrentTable);
        OperationGenerator operationGenerator = new OperationGenerator(difference.generateOptions(), dialect);
        List<Operation> operations = operationGenerator.generateChanges(builder.build());
        List<String> sql = operationGenerator.generateSql(difference.generateOptions().defaultSchemaName(), operations);
        return new GeneratedOperations(sql, operations);
    }

    public List<String> generateSql(GenerateSqlRequest generateSql)
    {
        OperationGenerator operationGenerator = new OperationGenerator(generateSql.generateOptions(), dialect);
        return operationGenerator.generateSql(generateSql.generateOptions().defaultSchemaName(), generateSql.operations());
    }

    public List<DiffTable> generateDiffTables(String rootPath, List<DiscoveredTable> previousTables, List<DiscoveredTable> currentTables)
    {
        return DiscoveredTablesDiffGenerator.generateDiff(rootPath, previousTables, currentTables);
    }

    private static URI normalizeUri(URI uri)
    {
        String path = uri.getPath();
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        else if (path.isEmpty()) {
            path = "/";
        }
        // this is needed due to new TrinoFileSystem requiring triple slash sometimes, f.e. local:///x/y/z, where URI strips it off to local:/x/y/z
        String pathPrefix = uri.toString().substring(0, uri.toString().indexOf(uri.getPath()));

        return uri.resolve(path).toString().startsWith(pathPrefix) ?
                uri.resolve(path) :
                URI.create(pathPrefix + path);
    }
}
