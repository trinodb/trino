/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.openlineage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputDatasetBuilder;
import io.openlineage.client.OpenLineage.JobBuilder;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.client.OpenLineage.RunFacetsBuilder;
import io.openlineage.client.OpenLineageClient;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.OutputColumnMetadata;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryFailureInfo;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryOutputMetadata;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.eventlistener.TableInfo;
import io.trino.spi.resourcegroups.QueryType;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

public class OpenLineageListener
        implements EventListener
{
    private static final Logger logger = Logger.get(OpenLineageListener.class);
    private static final ObjectMapper QUERY_STATISTICS_MAPPER = new ObjectMapperProvider().get();

    private final OpenLineage openLineage = new OpenLineage(URI.create("https://github.com/trinodb/trino/plugin/trino-openlineage"));
    private final OpenLineageClient client;
    private final String jobNamespace;
    private final String datasetNamespace;
    private final Set<QueryType> includeQueryTypes;

    @Inject
    public OpenLineageListener(OpenLineageClient client, OpenLineageListenerConfig listenerConfig)
    {
        this.client = requireNonNull(client, "client is null");
        requireNonNull(listenerConfig, "listenerConfig is null");
        this.jobNamespace = listenerConfig.getNamespace().orElse(defaultNamespace(listenerConfig.getTrinoURI()));
        this.datasetNamespace = defaultNamespace(listenerConfig.getTrinoURI());
        this.includeQueryTypes = ImmutableSet.copyOf(listenerConfig.getIncludeQueryTypes());
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        if (queryTypeSupported(queryCreatedEvent.getContext())) {
            UUID runID = getQueryId(queryCreatedEvent.getMetadata());

            RunEvent event = getStartEvent(runID, queryCreatedEvent);
            client.emit(event);
            return;
        }
        logger.debug("Query type %s not supported. Supported query types %s",
                queryCreatedEvent.getContext().getQueryType().toString(),
                this.includeQueryTypes);
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        if (queryTypeSupported(queryCompletedEvent.getContext())) {
            UUID runID = getQueryId(queryCompletedEvent.getMetadata());

            RunEvent event = getCompletedEvent(runID, queryCompletedEvent);
            client.emit(event);
            return;
        }
        logger.debug("Query type %s not supported. Supported query types %s",
                queryCompletedEvent.getContext().getQueryType().toString(),
                this.includeQueryTypes);
    }

    private boolean queryTypeSupported(QueryContext queryContext)
    {
        return queryContext
                .getQueryType()
                .map(this.includeQueryTypes::contains)
                .orElse(false);
    }

    private UUID getQueryId(QueryMetadata queryMetadata)
    {
        return UUID.nameUUIDFromBytes(queryMetadata.getQueryId().getBytes(UTF_8));
    }

    private RunFacet getTrinoQueryContextFacet(QueryContext queryContext)
    {
        RunFacet queryContextFacet = openLineage.newRunFacet();

        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();

        properties.put("server_address", queryContext.getServerAddress());
        properties.put("environment", queryContext.getEnvironment());

        queryContext.getQueryType().ifPresent(queryType ->
                properties.put("query_type", queryType.toString()));

        queryContextFacet
                .getAdditionalProperties()
                .putAll(properties.buildOrThrow());

        return queryContextFacet;
    }

    private RunFacet getTrinoMetadataFacet(QueryMetadata queryMetadata)
    {
        RunFacet trinoMetadataFacet = openLineage.newRunFacet();

        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();

        queryMetadata.getPlan().ifPresent(
                queryPlan -> properties.put("query_plan", queryPlan));

        queryMetadata.getTransactionId().ifPresent(
                transactionId -> properties.put("transaction_id", transactionId));

        trinoMetadataFacet
                .getAdditionalProperties()
                .putAll(properties.buildOrThrow());

        return trinoMetadataFacet;
    }

    private RunFacet getTrinoQueryStatisticsFacet(QueryStatistics queryStatistics)
    {
        RunFacet trinoQueryStatisticsFacet = openLineage.newRunFacet();

        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();

        QUERY_STATISTICS_MAPPER.convertValue(queryStatistics, HashMap.class).forEach(
                (key, value) -> {
                    if (key != null && value != null) {
                        properties.put(key.toString(), value.toString());
                    }
                });

        trinoQueryStatisticsFacet
                .getAdditionalProperties()
                .putAll(properties.buildOrThrow());

        return trinoQueryStatisticsFacet;
    }

    public RunEvent getStartEvent(UUID runID, QueryCreatedEvent queryCreatedEvent)
    {
        RunFacetsBuilder runFacetsBuilder = getBaseRunFacetsBuilder(queryCreatedEvent.getContext());

        runFacetsBuilder.put(OpenLineageTrinoFacet.TRINO_METADATA.asText(),
                getTrinoMetadataFacet(queryCreatedEvent.getMetadata()));
        runFacetsBuilder.put(OpenLineageTrinoFacet.TRINO_QUERY_CONTEXT.asText(),
                getTrinoQueryContextFacet(queryCreatedEvent.getContext()));

        return openLineage.newRunEventBuilder()
                    .eventType(RunEvent.EventType.START)
                    .eventTime(queryCreatedEvent.getCreateTime().atZone(UTC))
                    .run(openLineage.newRunBuilder().runId(runID).facets(runFacetsBuilder.build()).build())
                    .job(getBaseJobBuilder(queryCreatedEvent.getMetadata()).build())
                    .build();
    }

    public RunEvent getCompletedEvent(UUID runID, QueryCompletedEvent queryCompletedEvent)
    {
        boolean failed = queryCompletedEvent.getMetadata().getQueryState().equals("FAILED");

        RunFacetsBuilder runFacetsBuilder = getBaseRunFacetsBuilder(queryCompletedEvent.getContext());

        runFacetsBuilder.put(OpenLineageTrinoFacet.TRINO_METADATA.asText(),
                getTrinoMetadataFacet(queryCompletedEvent.getMetadata()));
        runFacetsBuilder.put(OpenLineageTrinoFacet.TRINO_QUERY_CONTEXT.asText(),
                getTrinoQueryContextFacet(queryCompletedEvent.getContext()));
        runFacetsBuilder.put(OpenLineageTrinoFacet.TRINO_QUERY_STATISTICS.asText(),
                getTrinoQueryStatisticsFacet(queryCompletedEvent.getStatistics()));

        if (failed) {
            queryCompletedEvent
                    .getFailureInfo()
                    .flatMap(QueryFailureInfo::getFailureMessage)
                    .ifPresent(failureMessage -> runFacetsBuilder
                            .errorMessage(openLineage
                                    .newErrorMessageRunFacetBuilder()
                                    .message(failureMessage)
                                    .build()));
        }

        return openLineage.newRunEventBuilder()
                .eventType(
                        failed
                                ? RunEvent.EventType.FAIL
                                : RunEvent.EventType.COMPLETE)
                .eventTime(queryCompletedEvent.getEndTime().atZone(UTC))
                .run(openLineage.newRunBuilder().runId(runID).facets(runFacetsBuilder.build()).build())
                .job(getBaseJobBuilder(queryCompletedEvent.getMetadata()).build())
                .inputs(buildInputs(queryCompletedEvent.getMetadata()))
                .outputs(buildOutputs(queryCompletedEvent.getIoMetadata()))
                .build();
    }

    private RunFacetsBuilder getBaseRunFacetsBuilder(QueryContext queryContext)
    {
        return openLineage.newRunFacetsBuilder()
                .processing_engine(openLineage.newProcessingEngineRunFacetBuilder()
                        .name("trino")
                        .version(queryContext.getServerVersion())
                        .build());
    }

    private JobBuilder getBaseJobBuilder(QueryMetadata queryMetadata)
    {
        return openLineage.newJobBuilder()
                .namespace(this.jobNamespace)
                .name(queryMetadata.getQueryId())
                .facets(openLineage.newJobFacetsBuilder()
                            .jobType(openLineage.newJobTypeJobFacet("BATCH", "TRINO", "QUERY"))
                            .sql(openLineage.newSQLJobFacet(queryMetadata.getQuery()))
                            .build());
    }

    private List<InputDataset> buildInputs(QueryMetadata queryMetadata)
    {
        return queryMetadata
                .getTables()
                .stream()
                .filter(TableInfo::isDirectlyReferenced)
                .map(table -> {
                    String datasetName = getDatasetName(table);
                    InputDatasetBuilder inputDatasetBuilder = openLineage
                            .newInputDatasetBuilder()
                            .namespace(this.datasetNamespace)
                            .name(datasetName);

                    DatasetFacetsBuilder datasetFacetsBuilder = openLineage.newDatasetFacetsBuilder()
                            .schema(openLineage.newSchemaDatasetFacetBuilder()
                            .fields(
                                    table
                                        .getColumns()
                                        .stream()
                                        .map(field -> openLineage.newSchemaDatasetFacetFieldsBuilder()
                                                .name(field.getColumn())
                                                .build()
                                        ).toList())
                            .build());

                    return inputDatasetBuilder
                            .facets(datasetFacetsBuilder.build())
                            .build();
                })
                .collect(toImmutableList());
    }

    private List<OutputDataset> buildOutputs(QueryIOMetadata ioMetadata)
    {
        Optional<QueryOutputMetadata> outputs = ioMetadata.getOutput();
        if (outputs.isPresent()) {
            QueryOutputMetadata outputMetadata = outputs.get();
            List<OutputColumnMetadata> outputColumns = outputMetadata.getColumns().orElse(List.of());

            OpenLineage.ColumnLineageDatasetFacetFieldsBuilder columnLineageDatasetFacetFieldsBuilder = openLineage.newColumnLineageDatasetFacetFieldsBuilder();

            outputColumns.forEach(column ->
                    columnLineageDatasetFacetFieldsBuilder.put(column.getColumnName(),
                            openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                                    .inputFields(column
                                            .getSourceColumns()
                                            .stream()
                                            .map(inputColumn -> openLineage.newColumnLineageDatasetFacetFieldsAdditionalInputFieldsBuilder()
                                                    .field(inputColumn.getColumnName())
                                                    .namespace(this.datasetNamespace)
                                                    .name(getDatasetName(inputColumn.getCatalog(), inputColumn.getSchema(), inputColumn.getTable()))
                                                    .build())
                                            .toList()
                                    ).build()));

            return ImmutableList.of(
                    openLineage.newOutputDatasetBuilder()
                            .namespace(this.datasetNamespace)
                            .name(getDatasetName(outputMetadata.getCatalogName(), outputMetadata.getSchema(), outputMetadata.getTable()))
                            .facets(openLineage.newDatasetFacetsBuilder()
                                    .columnLineage(openLineage.newColumnLineageDatasetFacet(columnLineageDatasetFacetFieldsBuilder.build()))
                                    .schema(openLineage.newSchemaDatasetFacetBuilder()
                                            .fields(
                                                    outputColumns.stream()
                                                            .map(column -> openLineage.newSchemaDatasetFacetFieldsBuilder()
                                                                    .name(column.getColumnName())
                                                                    .type(column.getColumnType())
                                                                    .build())
                                                            .toList()
                                            ).build()
                                    ).build()
                            ).build());
        }

        return ImmutableList.of();
    }

    private String getDatasetName(TableInfo tableInfo)
    {
        return getDatasetName(tableInfo.getCatalog(), tableInfo.getSchema(), tableInfo.getTable());
    }

    private String getDatasetName(String catalogName, String schemaName, String tableName)
    {
        return format("%s.%s.%s", catalogName, schemaName, tableName);
    }

    private static String defaultNamespace(URI uri)
    {
        if (!uri.getScheme().isEmpty()) {
            return uri.toString().replace(uri.getScheme(), "trino");
        }
        return "trino://" + uri;
    }
}
