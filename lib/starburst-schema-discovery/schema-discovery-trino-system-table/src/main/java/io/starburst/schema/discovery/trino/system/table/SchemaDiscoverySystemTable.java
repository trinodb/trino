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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.starburst.schema.discovery.SchemaDiscoveryController;
import io.starburst.schema.discovery.SchemaExplorer;
import io.starburst.schema.discovery.SchemaExplorer.Discovered;
import io.starburst.schema.discovery.SchemaExplorer.DiscoveryConfig;
import io.starburst.schema.discovery.formats.csv.CsvOptions;
import io.starburst.schema.discovery.infer.OptionDescription;
import io.starburst.schema.discovery.internal.Column;
import io.starburst.schema.discovery.models.DiscoveredColumns;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.GeneratedOperations;
import io.starburst.schema.discovery.models.TableFormat;
import io.starburst.schema.discovery.options.CommaDelimitedOptionsParser;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.starburst.schema.discovery.request.GenerateOptions;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.InMemoryRecordSet.Builder;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static java.util.Objects.requireNonNull;

public final class SchemaDiscoverySystemTable
        extends DiscoverySystemTableBase
        implements SystemTable
{
    private final SchemaDiscoveryController schemaDiscoveryController;

    public static final SchemaTableName SCHEMA_TABLE_NAME = new SchemaTableName("schema_discovery", "discovery");

    private static final String DEFAULT_SCHEMA_NAME = "discovered";
    private static final ConnectorTableMetadata TABLE_METADATA = new ConnectorTableMetadata(SCHEMA_TABLE_NAME, ImmutableList.of(
            buildColumn("uri", "URI to scan"),
            buildColumn("previous_metadata_json", "metadata_json from a previous query to use for generating updates - otherwise should be null"),
            buildColumn("schema", "schema name to use - %s if not provided".formatted(DEFAULT_SCHEMA_NAME)),
            buildColumn("options", buildOptionsHelp()),
            buildColumn("sql", "Discovered schema as SQL statements"),
            buildColumn("json", "Discovered schema as JSON"),
            buildColumn("errors", "Any errors discovered as a JSON list of strings"),
            buildColumn("metadata_json", "Metadata JSON for use in future update query"),
            buildColumn("rescan_uri", "Rescan single table mode - the URI to rescan (requires rescan_type)"),
            buildColumn("rescan_type", "Rescan single table mode - discovery type to use (requires rescan_uri). One of: " + Arrays.toString(TableFormat.values())),
            buildColumn("rescan_metadata_json", "Rescan single table mode. Metadata from initial scan. If provided, generated output will contain initial scan combined with this rescan")));

    private static final int MAX_BUCKET_QTY = 10;

    @Inject
    public SchemaDiscoverySystemTable(SchemaDiscoveryController schemaDiscoveryController, ObjectMapper objectMapper, DiscoveryLocationAccessControlAdapter locationAccessControl)
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
        String uri = tryGetSingleVarcharValue(constraint, 0).orElseThrow(() -> new TrinoException(INVALID_ARGUMENTS, "Missing URI argument"));
        validateLocationAccess(session.getIdentity(), uri);

        Optional<String> previousMetadataJson = tryGetSingleVarcharValue(constraint, 1);
        String schema = tryGetSingleVarcharValue(constraint, 2).orElse("");
        String options = tryGetSingleVarcharValue(constraint, 3).orElse("");
        Optional<String> rescanUri = tryGetSingleVarcharValue(constraint, 8);
        Optional<String> rescanType = tryGetSingleVarcharValue(constraint, 9);
        Optional<String> rescanMetadata = tryGetSingleVarcharValue(constraint, 10);

        SchemaExplorer schemaExplorer = new SchemaExplorer(schemaDiscoveryController, objectMapper(), new CommaDelimitedOptionsParser(ImmutableList.of(GeneralOptions.class, CsvOptions.class)));
        DiscoveryConfig discoveryConfig = new DiscoveryConfig(
                uri,
                options,
                new GenerateOptions(schema.isBlank() ? "discovered" : schema, MAX_BUCKET_QTY, true, Optional.empty()),
                previousMetadataJson,
                rescanType,
                rescanUri,
                rescanMetadata);
        Discovered discovered = schemaExplorer.discover(discoveryConfig, location -> validateLocationAccess(session.getIdentity(), location));
        GeneratedOperations operations = discovered.generatedOperations();

        Builder table = InMemoryRecordSet.builder(TABLE_METADATA);
        String sql = String.join("", operations.sql());
        String operationsJson = toJson(operations.operations());
        String metadataJson = toJson(clean(discovered.discoveredSchema().tables()));
        String errorsJson = toJson(discovered.discoveredSchema().errors());
        table.addRow(uri, previousMetadataJson.orElse(null), schema, options, sql, operationsJson, errorsJson, metadataJson, rescanUri.orElse(null), rescanType.orElse(null), rescanMetadata.orElse(null));
        return table.build().cursor();
    }

    private List<DiscoveredTable> clean(List<DiscoveredTable> tables)
    {
        return tables.stream()
                .map(this::clean)
                .collect(toImmutableList());
    }

    private DiscoveredTable clean(DiscoveredTable table)
    {
        List<Column> cleanedColumns = table.columns().columns().stream()
                .map(Column::withoutSampleValue)
                .collect(toImmutableList());
        return table.withColumns(new DiscoveredColumns(cleanedColumns, table.columns().flags()));
    }

    private static String buildOptionsHelp()
    {
        StringBuilder str = new StringBuilder();
        str.append("Discovery override options as a string. E.g. U&'delimiter=\\0009,comment=#,ignoreLeadingWhiteSpace=false'\n\n");
        str.append("Note: table specific options can be set as: <schema>.<table>.field=value\n\n");
        str.append("DEFAULTS\n");
        CsvOptions.standard().forEach((option, value) -> str.append(option).append(" - ").append(value).append('\n'));
        str.append('\n');
        str.append("STANDARD OPTIONS\n");
        buildOptionsHelp(str, GeneralOptions.class);
        str.append('\n');
        str.append("CSV OPTIONS\n");
        buildOptionsHelp(str, CsvOptions.class);
        return str.toString();
    }

    private static void buildOptionsHelp(StringBuilder str, Class<?> clazz)
    {
        Stream.of(clazz.getDeclaredFields()).forEach(field -> {
            OptionDescription description = field.getAnnotation(OptionDescription.class);
            if (description != null) {
                try {
                    String value = description.value();
                    Object[] enumConstants = description.enums().getEnumConstants();
                    if (enumConstants != null) {
                        value += " " + Arrays.toString(enumConstants);
                    }
                    str.append(field.get(null).toString()).append(" - ").append(value).append('\n');
                }
                catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
