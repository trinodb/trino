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
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.json.JsonCodec;
import io.trino.plugin.opa.schema.OpaBatchQueryResult;
import io.trino.plugin.opa.schema.OpaPluginContext;
import io.trino.plugin.opa.schema.OpaQueryContext;
import io.trino.plugin.opa.schema.OpaQueryInput;
import io.trino.plugin.opa.schema.OpaQueryInputAction;
import io.trino.plugin.opa.schema.OpaQueryInputResource;
import io.trino.plugin.opa.schema.TrinoFunction;
import io.trino.plugin.opa.schema.TrinoSchema;
import io.trino.plugin.opa.schema.TrinoTable;
import io.trino.plugin.opa.schema.TrinoUser;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class OpaBatchAccessControl
        extends OpaAccessControl
{
    private final JsonCodec<OpaBatchQueryResult> batchResultCodec;
    private final URI opaBatchedPolicyUri;
    private final OpaHttpClient opaHttpClient;

    @Inject
    public OpaBatchAccessControl(
            LifeCycleManager lifeCycleManager,
            OpaHighLevelClient opaHighLevelClient,
            JsonCodec<OpaBatchQueryResult> batchResultCodec,
            OpaHttpClient opaHttpClient,
            OpaConfig config,
            OpaPluginContext pluginContext)
    {
        super(lifeCycleManager, opaHighLevelClient, config, pluginContext);
        this.opaBatchedPolicyUri = config.getOpaBatchUri().orElseThrow();
        this.batchResultCodec = requireNonNull(batchResultCodec, "batchResultCodec is null");
        this.opaHttpClient = requireNonNull(opaHttpClient, "opaHttpClient is null");
    }

    @Override
    public Collection<Identity> filterViewQueryOwnedBy(Identity identity, Collection<Identity> queryOwners)
    {
        return batchFilterFromOpa(
                buildQueryContext(identity),
                "FilterViewQueryOwnedBy",
                queryOwners,
                queryOwner -> OpaQueryInputResource.builder()
                        .user(new TrinoUser(queryOwner))
                        .build());
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        return batchFilterFromOpa(
                buildQueryContext(context),
                "FilterCatalogs",
                catalogs,
                catalog -> OpaQueryInputResource.builder()
                        .catalog(catalog)
                        .build());
    }

    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        return batchFilterFromOpa(
                buildQueryContext(context),
                "FilterSchemas",
                schemaNames,
                schema -> OpaQueryInputResource.builder().schema(new TrinoSchema(catalogName, schema)).build());
    }

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        return batchFilterFromOpa(
                buildQueryContext(context),
                "FilterTables",
                tableNames,
                table -> OpaQueryInputResource.builder().table(new TrinoTable(catalogName, table.getSchemaName(), table.getTableName())).build());
    }

    @Override
    public Map<SchemaTableName, Set<String>> filterColumns(SystemSecurityContext context, String catalogName, Map<SchemaTableName, Set<String>> tableColumns)
    {
        BiFunction<SchemaTableName, List<String>, OpaQueryInput> requestBuilder = batchRequestBuilder(
                buildQueryContext(context),
                "FilterColumns",
                (schemaTableName, columns) -> OpaQueryInputResource.builder()
                        .table(new TrinoTable(catalogName, schemaTableName.getSchemaName(), schemaTableName.getTableName()).withColumns(ImmutableSet.copyOf(columns)))
                        .build());
        return opaHttpClient.parallelBatchFilterFromOpa(tableColumns, requestBuilder, opaBatchedPolicyUri, batchResultCodec);
    }

    @Override
    public Set<SchemaFunctionName> filterFunctions(SystemSecurityContext context, String catalogName, Set<SchemaFunctionName> functionNames)
    {
        return batchFilterFromOpa(
                buildQueryContext(context),
                "FilterFunctions",
                functionNames,
                function -> OpaQueryInputResource.builder()
                        .function(new TrinoFunction(new TrinoSchema(catalogName, function.getSchemaName()), function.getFunctionName()))
                        .build());
    }

    private <T> Set<T> batchFilterFromOpa(OpaQueryContext context, String operation, Collection<T> items, Function<T, OpaQueryInputResource> converter)
    {
        return opaHttpClient.batchFilterFromOpa(
                items,
                batchRequestBuilder(context, operation, converter),
                opaBatchedPolicyUri,
                batchResultCodec);
    }

    private static <V> Function<List<V>, OpaQueryInput> batchRequestBuilder(OpaQueryContext context, String operation, Function<V, OpaQueryInputResource> resourceMapper)
    {
        return items -> new OpaQueryInput(
                context,
                OpaQueryInputAction.builder()
                        .operation(operation)
                        .filterResources(items.stream().map(resourceMapper).collect(toImmutableList()))
                        .build());
    }

    private static <K, V> BiFunction<K, List<V>, OpaQueryInput> batchRequestBuilder(OpaQueryContext context, String operation, BiFunction<K, List<V>, OpaQueryInputResource> resourceMapper)
    {
        return (resourcesKey, resourcesList) -> new OpaQueryInput(
                context,
                OpaQueryInputAction.builder()
                        .operation(operation)
                        .filterResources(ImmutableList.of(resourceMapper.apply(resourcesKey, resourcesList)))
                        .build());
    }
}
