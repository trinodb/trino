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
package io.trino.plugin.opa;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.plugin.opa.schema.OpaBatchColumnMaskQueryResult;
import io.trino.plugin.opa.schema.OpaColumnMaskQueryResult;
import io.trino.plugin.opa.schema.OpaQueryContext;
import io.trino.plugin.opa.schema.OpaQueryInput;
import io.trino.plugin.opa.schema.OpaQueryInputAction;
import io.trino.plugin.opa.schema.OpaQueryInputResource;
import io.trino.plugin.opa.schema.OpaQueryResult;
import io.trino.plugin.opa.schema.OpaRowFiltersQueryResult;
import io.trino.plugin.opa.schema.OpaViewExpression;
import io.trino.plugin.opa.schema.TrinoColumn;
import io.trino.plugin.opa.schema.TrinoTable;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.security.AccessDeniedException;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class OpaHighLevelClient
{
    private final JsonCodec<OpaQueryResult> queryResultCodec;
    private final JsonCodec<OpaRowFiltersQueryResult> rowFiltersQueryResultCodec;
    private final JsonCodec<OpaColumnMaskQueryResult> columnMaskQueryResultCodec;
    private final JsonCodec<OpaBatchColumnMaskQueryResult> batchColumnMaskQueryResultCodec;
    private final OpaHttpClient opaHttpClient;
    private final URI opaPolicyUri;
    private final Optional<URI> opaRowFiltersUri;
    private final Optional<URI> opaColumnMaskingUri;
    private final Optional<URI> opaBatchColumnMaskingUri;

    @Inject
    public OpaHighLevelClient(
            JsonCodec<OpaQueryResult> queryResultCodec,
            JsonCodec<OpaRowFiltersQueryResult> rowFiltersQueryResultCodec,
            JsonCodec<OpaColumnMaskQueryResult> columnMaskQueryResultCodec,
            JsonCodec<OpaBatchColumnMaskQueryResult> batchColumnMaskQueryResultCodec,
            OpaHttpClient opaHttpClient,
            OpaConfig config)
    {
        this.queryResultCodec = requireNonNull(queryResultCodec, "queryResultCodec is null");
        this.rowFiltersQueryResultCodec = requireNonNull(rowFiltersQueryResultCodec, "rowFiltersQueryResultCodec is null");
        this.columnMaskQueryResultCodec = requireNonNull(columnMaskQueryResultCodec, "columnMaskQueryResultCodec is null");
        this.batchColumnMaskQueryResultCodec = requireNonNull(batchColumnMaskQueryResultCodec, "batchColumnMaskQueryResultCodec is null");
        this.opaHttpClient = requireNonNull(opaHttpClient, "opaHttpClient is null");
        this.opaPolicyUri = config.getOpaUri();
        this.opaRowFiltersUri = config.getOpaRowFiltersUri();
        this.opaColumnMaskingUri = config.getOpaColumnMaskingUri();
        this.opaBatchColumnMaskingUri = config.getOpaBatchColumnMaskingUri();
    }

    public boolean queryOpa(OpaQueryInput input)
    {
        return opaHttpClient.consumeOpaResponse(opaHttpClient.submitOpaRequest(input, opaPolicyUri, queryResultCodec)).result();
    }

    private boolean queryOpaWithSimpleAction(OpaQueryContext context, String operation)
    {
        return queryOpa(buildQueryInputForSimpleAction(context, operation));
    }

    public boolean queryOpaWithSimpleResource(OpaQueryContext context, String operation, OpaQueryInputResource resource)
    {
        return queryOpa(buildQueryInputForSimpleResource(context, operation, resource));
    }

    public boolean queryOpaWithSourceAndTargetResource(OpaQueryContext context, String operation, OpaQueryInputResource resource, OpaQueryInputResource targetResource)
    {
        return queryOpa(
                new OpaQueryInput(
                        context,
                        OpaQueryInputAction.builder()
                                .operation(operation)
                                .resource(resource)
                                .targetResource(targetResource)
                                .build()));
    }

    public void queryAndEnforce(
            OpaQueryContext context,
            String actionName,
            Runnable deny,
            OpaQueryInputResource resource)
    {
        if (!queryOpaWithSimpleResource(context, actionName, resource)) {
            deny.run();
            // we should never get here because deny should throw
            throw new AccessDeniedException("Access denied for action %s and resource %s".formatted(actionName, resource));
        }
    }

    public void queryAndEnforce(
            OpaQueryContext context,
            String actionName,
            Runnable deny)
    {
        if (!queryOpaWithSimpleAction(context, actionName)) {
            deny.run();
            // we should never get here because deny should throw
            throw new AccessDeniedException("Access denied for action %s".formatted(actionName));
        }
    }

    public <T> Set<T> parallelFilterFromOpa(
            Collection<T> items,
            Function<T, OpaQueryInput> requestBuilder)
    {
        return opaHttpClient.parallelFilterFromOpa(items, requestBuilder, opaPolicyUri, queryResultCodec);
    }

    public List<OpaViewExpression> getRowFilterExpressionsFromOpa(OpaQueryContext context, CatalogSchemaTableName table)
    {
        OpaQueryInput queryInput = new OpaQueryInput(
                context,
                OpaQueryInputAction.builder()
                        .operation("GetRowFilters")
                        .resource(OpaQueryInputResource.builder().table(new TrinoTable(table)).build())
                        .build());
        return opaRowFiltersUri
                .map(uri -> opaHttpClient.consumeOpaResponse(opaHttpClient.submitOpaRequest(queryInput, uri, rowFiltersQueryResultCodec)).result())
                .orElse(ImmutableList.of());
    }

    public Map<ColumnSchema, OpaViewExpression> getTableColumnMasksFromOpa(OpaQueryContext context, CatalogSchemaTableName table, List<ColumnSchema> columns)
    {
        return opaBatchColumnMaskingUri.map(batchUri -> getBatchTableColumnMasksFromOpa(batchUri, context, table, columns))
                .or(() -> opaColumnMaskingUri.map(maskUri -> getParallelTableColumnMasksFromOpa(maskUri, context, table, columns)))
                .orElse(ImmutableMap.of());
    }

    public static OpaQueryInput buildQueryInputForSimpleResource(OpaQueryContext context, String operation, OpaQueryInputResource resource)
    {
        return new OpaQueryInput(context, OpaQueryInputAction.builder().operation(operation).resource(resource).build());
    }

    private Map<ColumnSchema, OpaViewExpression> getBatchTableColumnMasksFromOpa(URI uri, OpaQueryContext context, CatalogSchemaTableName table, List<ColumnSchema> columns)
    {
        OpaQueryInput input = new OpaQueryInput(context, OpaQueryInputAction.builder()
                .operation("GetColumnMask")
                .filterResources(columns.stream().map(column -> OpaQueryInputResource.builder().column(new TrinoColumn(table, column.getName(), column.getType())).build()).collect(toImmutableList()))
                .build());
        OpaBatchColumnMaskQueryResult result = opaHttpClient.consumeOpaResponse(opaHttpClient.submitOpaRequest(input, uri, batchColumnMaskQueryResultCodec));

        return result.result().stream()
                .collect(toImmutableMap(item -> columns.get(item.index()), OpaBatchColumnMaskQueryResult.OpaBatchColumnMaskQueryResultItem::viewExpression));
    }

    private Map<ColumnSchema, OpaViewExpression> getParallelTableColumnMasksFromOpa(URI uri, OpaQueryContext context, CatalogSchemaTableName table, List<ColumnSchema> columns)
    {
        return opaHttpClient.parallelColumnMasksFromOpa(
                columns,
                column -> new OpaQueryInput(
                        context,
                        OpaQueryInputAction.builder()
                                .operation("GetColumnMask")
                                .resource(OpaQueryInputResource.builder().column(new TrinoColumn(table, column.getName(), column.getType())).build())
                                .build()),
                uri,
                columnMaskQueryResultCodec);
    }

    private static OpaQueryInput buildQueryInputForSimpleAction(OpaQueryContext context, String operation)
    {
        return new OpaQueryInput(context, OpaQueryInputAction.builder().operation(operation).build());
    }
}
