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
package io.trino.plugin.openpolicyagent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.plugin.openpolicyagent.schema.OpaBatchQueryResult;
import io.trino.plugin.openpolicyagent.schema.OpaQueryContext;
import io.trino.plugin.openpolicyagent.schema.OpaQueryInput;
import io.trino.plugin.openpolicyagent.schema.OpaQueryInputAction;
import io.trino.plugin.openpolicyagent.schema.OpaQueryInputResource;
import io.trino.plugin.openpolicyagent.schema.TrinoSchema;
import io.trino.plugin.openpolicyagent.schema.TrinoTable;
import io.trino.plugin.openpolicyagent.schema.TrinoUser;
import io.trino.spi.connector.SchemaTableName;
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

public class OpaBatchAccessControl
        extends OpaAccessControl
{
    private final JsonCodec<OpaBatchQueryResult> batchResultCodec;
    private final URI opaBatchedPolicyUri;
    private final OpaHttpClient opaHttpClient;

    @Inject
    public OpaBatchAccessControl(
            OpaHighLevelClient opaHighLevelClient,
            JsonCodec<OpaBatchQueryResult> batchResultCodec,
            OpaHttpClient httpClient,
            OpaConfig config)
    {
        super(opaHighLevelClient);
        this.opaBatchedPolicyUri = config.getOpaBatchUri().orElseThrow();
        this.batchResultCodec = batchResultCodec;
        this.opaHttpClient = httpClient;
    }

    private <V> Function<List<V>, OpaQueryInput> batchRequestBuilder(OpaQueryContext context, String operation, Function<V, OpaQueryInputResource> resourceMapper)
    {
        return (items) -> new OpaQueryInput(
                context,
                new OpaQueryInputAction.Builder()
                        .operation(operation)
                        .filterResources(items.stream().map(resourceMapper).collect(toImmutableList()))
                        .build());
    }

    private <K, V> BiFunction<K, List<V>, OpaQueryInput> batchRequestBuilder(OpaQueryContext context, String operation, BiFunction<K, List<V>, OpaQueryInputResource> resourceMapper)
    {
        return (k, v) -> new OpaQueryInput(
                context,
                new OpaQueryInputAction.Builder()
                        .operation(operation)
                        .filterResources(ImmutableList.of(resourceMapper.apply(k, v)))
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

    @Override
    public Collection<Identity> filterViewQueryOwnedBy(Identity identity, Collection<Identity> queryOwners)
    {
        return batchFilterFromOpa(
                OpaQueryContext.fromIdentity(identity),
                "FilterViewQueryOwnedBy",
                queryOwners,
                (item) -> new OpaQueryInputResource
                        .Builder()
                        .user(new TrinoUser(item))
                        .build());
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        return batchFilterFromOpa(
                OpaQueryContext.fromSystemSecurityContext(context),
                "FilterCatalogs",
                catalogs,
                (item) -> new OpaQueryInputResource
                        .Builder()
                        .catalog(item)
                        .build());
    }

    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        return batchFilterFromOpa(
                OpaQueryContext.fromSystemSecurityContext(context),
                "FilterSchemas",
                schemaNames,
                (item) -> new OpaQueryInputResource
                        .Builder()
                        .schema(new TrinoSchema
                                .Builder()
                                .catalogName(catalogName)
                                .schemaName(item)
                                .build())
                        .build());
    }

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        return batchFilterFromOpa(
                OpaQueryContext.fromSystemSecurityContext(context),
                "FilterTables",
                tableNames,
                (i) -> new OpaQueryInputResource
                        .Builder()
                        .table(new TrinoTable
                                .Builder()
                                .catalogName(catalogName)
                                .schemaName(i.getSchemaName())
                                .tableName(i.getTableName())
                                .build())
                        .build());
    }

    @Override
    public Map<SchemaTableName, Set<String>> filterColumns(SystemSecurityContext context, String catalogName, Map<SchemaTableName, Set<String>> tableColumns)
    {
        BiFunction<SchemaTableName, List<String>, OpaQueryInput> requestBuilder = batchRequestBuilder(
                OpaQueryContext.fromSystemSecurityContext(context),
                "FilterColumns",
                (schemaTableName, columns) ->
                        new OpaQueryInputResource
                                .Builder()
                                .table(
                                        new TrinoTable
                                                .Builder()
                                                .catalogName(catalogName)
                                                .schemaName(schemaTableName.getSchemaName())
                                                .tableName(schemaTableName.getTableName())
                                                .columns(ImmutableSet.copyOf(columns))
                                                .build())
                                .build());
        return opaHttpClient.parallelBatchFilterFromOpa(tableColumns, requestBuilder, opaBatchedPolicyUri, batchResultCodec);
    }
}
