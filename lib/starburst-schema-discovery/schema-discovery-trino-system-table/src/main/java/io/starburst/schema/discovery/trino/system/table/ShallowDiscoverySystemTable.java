/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.trino.system.table;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.starburst.schema.discovery.SchemaDiscoveryController;
import io.starburst.schema.discovery.SchemaExplorer;
import io.starburst.schema.discovery.models.DiscoveredSchema;
import io.starburst.schema.discovery.options.CommaDelimitedOptionsParser;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.starburst.schema.discovery.request.GuessRequest;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static io.starburst.schema.discovery.options.GeneralOptions.MAX_SAMPLE_FILES_PER_TABLE;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.StandardErrorCode.PROCEDURE_CALL_FAILED;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static java.util.Objects.requireNonNull;

public final class ShallowDiscoverySystemTable
        extends DiscoverySystemTableBase
        implements SystemTable
{
    private static final Map<String, String> DEFAULT_OPTIONS_OVERWRITE = ImmutableMap.of(
            MAX_SAMPLE_FILES_PER_TABLE, "1");
    public static final SchemaTableName SCHEMA_TABLE_NAME = new SchemaTableName("schema_discovery", "shallow_discovery");
    private static final ConnectorTableMetadata TABLE_METADATA = new ConnectorTableMetadata(SCHEMA_TABLE_NAME, ImmutableList.of(
            buildColumn("uri", "URI to scan"),
            buildColumn("options", "Discovery options - only [maxSampleFilesPerTable, maxSampleTables, excludePatterns, includePatterns, discoveryMode] are used"),
            buildColumn("shallow_metadata_json", "Discovered shallow tables as JSON")));

    private final SchemaDiscoveryController schemaDiscoveryController;

    public ShallowDiscoverySystemTable(SchemaDiscoveryController schemaDiscoveryController, ObjectMapper objectMapper, DiscoveryLocationAccessControlAdapter locationAccessControl)
    {
        super(objectMapper, locationAccessControl);
        this.schemaDiscoveryController = requireNonNull(schemaDiscoveryController, "schemaDiscoveryController is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return TABLE_METADATA;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        String uriStr = tryGetSingleVarcharValue(constraint, 0).orElseThrow(() -> new TrinoException(INVALID_ARGUMENTS, "Missing URI argument"));
        String options = tryGetSingleVarcharValue(constraint, 1).orElse("");
        Map<String, String> finalOptions = new SchemaExplorer(schemaDiscoveryController, objectMapper(), new CommaDelimitedOptionsParser(ImmutableList.of(GeneralOptions.class)))
                .buildOptions(options, DEFAULT_OPTIONS_OVERWRITE);

        try {
            URI uri = new URI(uriStr);
            GuessRequest guessRequest = new GuessRequest(uri, finalOptions);
            DiscoveredSchema discoveredSchema = schemaDiscoveryController.discoverTablesShallow(guessRequest).get();

            InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(TABLE_METADATA);
            return table.addRow(uriStr, options, objectMapper().writeValueAsString(discoveredSchema)).build().cursor();
        }
        catch (URISyntaxException e) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Invalid URI: " + uriStr, e);
        }
        catch (ExecutionException e) {
            String rootErrorMessage = Throwables.getRootCause(e).getMessage();
            String finalMessage = e.getMessage().equals(rootErrorMessage) ? rootErrorMessage : "%s, reason: %s".formatted(e.getMessage(), rootErrorMessage);
            throw new TrinoException(PROCEDURE_CALL_FAILED, "Failure for URI: %s caused by: %s".formatted(uriStr, finalMessage), e);
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Could not generate JSON for: " + DiscoveredSchema.class.getSimpleName(), e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
