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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.starburst.schema.discovery.formats.csv.CsvOptions;
import io.starburst.schema.discovery.models.DiscoveredSchema;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.GeneratedOperations;
import io.starburst.schema.discovery.models.TableFormat;
import io.starburst.schema.discovery.options.CommaDelimitedOptionsParser;
import io.starburst.schema.discovery.request.DiscoverRequest;
import io.starburst.schema.discovery.request.GenerateOperationDifferencesRequest;
import io.starburst.schema.discovery.request.GenerateOperationsRequest;
import io.starburst.schema.discovery.request.GenerateOptions;
import io.starburst.schema.discovery.request.GuessRequest;
import io.trino.spi.TrinoException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.starburst.schema.discovery.internal.RescanMerger.mergeRescan;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.StandardErrorCode.PROCEDURE_CALL_FAILED;
import static java.util.Objects.requireNonNull;

public class SchemaExplorer
{
    private final SchemaDiscoveryController schemaDiscoveryController;
    private final ObjectMapper objectMapper;
    private final CommaDelimitedOptionsParser optionsParser;

    public SchemaExplorer(SchemaDiscoveryController schemaDiscoveryController, ObjectMapper objectMapper, CommaDelimitedOptionsParser optionsParser)
    {
        this.schemaDiscoveryController = requireNonNull(schemaDiscoveryController, "schemaDiscoveryController is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.optionsParser = requireNonNull(optionsParser, "optionsParser is null");
    }

    public record Rescan(URI rescanUri, TableFormat rescanType, Optional<String> rescanMetadata) {}

    public record Discovered(DiscoveredSchema discoveredSchema, GeneratedOperations generatedOperations) {}

    public record DiscoveryConfig(String uriStr, String optionsString, GenerateOptions generateOptions, Optional<String> previousMetadataJson, Optional<String> rescanType, Optional<String> rescanUri, Optional<String> rescanMetadata) {}

    public Discovered discover(DiscoveryConfig discoveryConfig, Consumer<String> locationValidator)
    {
        Map<String, String> options = buildOptions(discoveryConfig.optionsString());
        Optional<Rescan> rescanMaybe;
        if (discoveryConfig.rescanUri().isPresent() || discoveryConfig.rescanType().isPresent()) {
            if (discoveryConfig.rescanUri().isPresent() != discoveryConfig.rescanType().isPresent()) {
                throw new TrinoException(INVALID_ARGUMENTS, "rescan_uri and rescan_type must either both be null or both be non-null");
            }
            TableFormat tableFormat;
            try {
                tableFormat = TableFormat.valueOf(discoveryConfig.rescanType().get());
            }
            catch (IllegalArgumentException e) {
                throw new TrinoException(INVALID_ARGUMENTS, "invalid rescan_type: %s. Must be one of: %s".formatted(discoveryConfig.rescanType().get(), Arrays.toString(TableFormat.values())));
            }
            try {
                rescanMaybe = Optional.of(new Rescan(new URI(discoveryConfig.rescanUri().get()), tableFormat, discoveryConfig.rescanMetadata()));
                discoveryConfig.rescanUri().ifPresent(locationValidator);
            }
            catch (URISyntaxException e) {
                throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Invalid URI: " + discoveryConfig.rescanUri().get(), e);
            }
        }
        else {
            rescanMaybe = Optional.empty();
        }

        try {
            URI uri = new URI(discoveryConfig.uriStr());
            ListenableFuture<DiscoveredSchema> future = rescanMaybe.map(rescan -> schemaDiscoveryController.discover(new DiscoverRequest(uri, rescan.rescanUri(), rescan.rescanType(), options)))
                    .orElseGet(() -> schemaDiscoveryController.guess(new GuessRequest(uri, options)));

            ListenableFuture<DiscoveredSchema> adjustedSchemaFuture = transform(
                    future,
                    discovery -> rescanMaybe.flatMap(Rescan::rescanMetadata).map(metadata -> mergeRescan(discovery, objectMapper, metadata)).orElse(discovery),
                    directExecutor());
            ListenableFuture<GeneratedOperations> operationsFuture = transform(
                    adjustedSchemaFuture,
                    adjustedDiscovery -> buildGeneratedOperations(uri, options, discoveryConfig.generateOptions(), adjustedDiscovery, discoveryConfig.previousMetadataJson()),
                    directExecutor());
            return new Discovered(adjustedSchemaFuture.get(), operationsFuture.get());
        }
        catch (URISyntaxException e) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Invalid URI: " + discoveryConfig.uriStr(), e);
        }
        catch (ExecutionException e) {
            String rootErrorMessage = Throwables.getRootCause(e).getMessage();
            String finalMessage = e.getMessage().equals(rootErrorMessage) ? rootErrorMessage : "%s, reason: %s".formatted(e.getMessage(), rootErrorMessage);
            throw new TrinoException(PROCEDURE_CALL_FAILED, "Failure for URI: %s caused by: %s".formatted(discoveryConfig.uriStr(), finalMessage), e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private GeneratedOperations buildGeneratedOperations(URI uri, Map<String, String> options, GenerateOptions generateOptions, DiscoveredSchema discovery, Optional<String> previousMetadataJson)
    {
        return previousMetadataJson.map(previous -> buildGeneratedOperationsFromPrevious(uri, options, generateOptions, discovery, previous))
                .orElseGet(() -> schemaDiscoveryController.generateOperations(new GenerateOperationsRequest(discovery, generateOptions)));
    }

    private GeneratedOperations buildGeneratedOperationsFromPrevious(URI uri, Map<String, String> options, GenerateOptions generateOptions, DiscoveredSchema discovery, String previousMetadataJson)
    {
        try {
            List<DiscoveredTable> discoveredTables = objectMapper.readValue(previousMetadataJson, new TypeReference<>() {});
            GenerateOperationDifferencesRequest request = new GenerateOperationDifferencesRequest(uri, discoveredTables, discovery.tables(), options, generateOptions);
            return schemaDiscoveryController.generateOperationDifferences(request);
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Invalid previous_metadata_json: " + previousMetadataJson, e);
        }
    }

    @VisibleForTesting
    public Map<String, String> buildOptions(String optionsStr)
    {
        return buildOptions(optionsStr, ImmutableMap.of());
    }

    public Map<String, String> buildOptions(String optionsStr, Map<String, String> defaultOverwrites)
    {
        Map<String, String> additionalOptions = optionsParser.parse(optionsStr);
        Map<String, String> options = new HashMap<>(CsvOptions.standard());
        options.putAll(defaultOverwrites);
        options.putAll(additionalOptions);
        return options;
    }
}
