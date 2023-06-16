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
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

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

    private List<Integer> batchQueryOpa(OpaQueryInput input)
    {
        return opaHttpClient.getOpaResponse(input, opaBatchedPolicyUri, batchResultCodec).result();
    }

    private <T> Set<T> batchFilterFromOpa(SystemSecurityContext context, String operation, Collection<T> items, Function<List<T>, List<OpaQueryInputResource>> converter)
    {
        if (items.isEmpty()) {
            return ImmutableSet.of();
        }
        List<T> orderedItems = ImmutableList.copyOf(items);
        OpaQueryInputAction action = new OpaQueryInputAction.Builder()
                .operation(operation)
                .filterResources(requireNonNull(converter.apply(orderedItems)))
                .build();
        OpaQueryInput query = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);
        return batchQueryOpa(query)
                .stream()
                .map(orderedItems::get)
                .collect(toImmutableSet());
    }

    private <T> Function<List<T>, List<OpaQueryInputResource>> mapItemToResource(Function<T, OpaQueryInputResource> converter)
    {
        return (s) -> s.stream().map(converter).collect(toImmutableList());
    }

    @Override
    public Collection<Identity> filterViewQueryOwnedBy(SystemSecurityContext context, Collection<Identity> queryOwners)
    {
        return batchFilterFromOpa(
                context,
                "FilterViewQueryOwnedBy",
                queryOwners,
                mapItemToResource((item) -> new OpaQueryInputResource
                        .Builder()
                        .user(new TrinoUser(item))
                        .build()));
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        return batchFilterFromOpa(
                context,
                "FilterCatalogs",
                catalogs,
                mapItemToResource(
                        (i) -> new OpaQueryInputResource
                                .Builder()
                                .catalog(i)
                                .build()));
    }

    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        return batchFilterFromOpa(
                context,
                "FilterSchemas",
                schemaNames,
                mapItemToResource(
                        (i) -> new OpaQueryInputResource
                                .Builder()
                                .schema(new TrinoSchema
                                        .Builder()
                                        .catalogName(catalogName)
                                        .schemaName(i)
                                        .build())
                                .build()));
    }

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        return batchFilterFromOpa(
                context,
                "FilterTables",
                tableNames,
                mapItemToResource(
                        (i) -> new OpaQueryInputResource
                                .Builder()
                                .table(new TrinoTable
                                        .Builder()
                                        .catalogName(catalogName)
                                        .schemaName(i.getSchemaName())
                                        .tableName(i.getTableName())
                                        .build())
                                .build()));
    }

    @Override
    public Set<String> filterColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        return batchFilterFromOpa(
                context,
                "FilterColumns",
                columns,
                (s) -> List.of(new OpaQueryInputResource
                        .Builder()
                        .table(TrinoTable
                                .Builder
                                .fromTrinoTable(table)
                                .columns(ImmutableSet.copyOf(s))
                                .build())
                        .build()));
    }
}
