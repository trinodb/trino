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

import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.plugin.openpolicyagent.schema.OpaQueryContext;
import io.trino.plugin.openpolicyagent.schema.OpaQueryInput;
import io.trino.plugin.openpolicyagent.schema.OpaQueryInputAction;
import io.trino.plugin.openpolicyagent.schema.OpaQueryInputResource;
import io.trino.plugin.openpolicyagent.schema.OpaQueryResult;
import io.trino.plugin.openpolicyagent.schema.TrinoSchema;
import io.trino.plugin.openpolicyagent.schema.TrinoTable;
import io.trino.plugin.openpolicyagent.schema.TrinoUser;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.security.Identity;

import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class OpaHighLevelClient
{
    private final JsonCodec<OpaQueryResult> queryResultCodec;
    private final URI opaPolicyUri;
    private final OpaHttpClient opaHttpClient;

    @Inject
    public OpaHighLevelClient(
            JsonCodec<OpaQueryResult> queryResultCodec,
            OpaHttpClient opaHttpClient,
            OpaConfig config)
    {
        this.queryResultCodec = requireNonNull(queryResultCodec, "queryResultCodec is null");
        this.opaHttpClient = requireNonNull(opaHttpClient, "opaHttpClient is null");
        this.opaPolicyUri = config.getOpaUri();
    }

    public boolean queryOpa(OpaQueryInput input)
    {
        return opaHttpClient.consumeOpaResponse(opaHttpClient.submitOpaRequest(input, opaPolicyUri, queryResultCodec)).result();
    }

    public boolean queryOpaWithSimpleAction(OpaQueryContext context, String operation)
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

    public <T> void queryAndEnforce(
            OpaQueryContext context,
            String actionName,
            Consumer<T> denyCallable,
            BiFunction<OpaQueryInputResource.Builder, T, OpaQueryInputResource.Builder> builderSetter,
            T value)
    {
        OpaQueryInputResource resource = builderSetter.apply(OpaQueryInputResource.builder(), value).build();
        if (!queryOpaWithSimpleResource(context, actionName, resource)) {
            denyCallable.accept(value);
        }
    }

    public <T> void queryAndEnforce(
            OpaQueryContext context,
            String actionName,
            DenyCallable denyCallable,
            BiFunction<OpaQueryInputResource.Builder, T, OpaQueryInputResource.Builder> builderSetter,
            T value)
    {
        queryAndEnforce(
                context,
                actionName,
                failedItem -> denyCallable.deny(),
                builderSetter,
                value);
    }

    public void queryAndEnforce(
            OpaQueryContext context,
            String actionName,
            Consumer<String> denyCallable,
            CatalogSchemaName schema)
    {
        queryAndEnforce(
                context,
                actionName,
                () -> denyCallable.accept(schema.toString()),
                OpaQueryInputResource.Builder::schema,
                TrinoSchema.fromTrinoCatalogSchema(schema));
    }

    public void queryAndEnforce(
            OpaQueryContext context,
            String actionName,
            Consumer<String> denyCallable,
            CatalogSchemaName schema,
            Map<String, ?> properties)
    {
        queryAndEnforce(
                context,
                actionName,
                () -> denyCallable.accept(schema.toString()),
                OpaQueryInputResource.Builder::schema,
                TrinoSchema.Builder.fromTrinoCatalogSchema(schema).properties(properties).build());
    }

    public void queryAndEnforce(
            OpaQueryContext context,
            String actionName,
            Consumer<String> denyCallable,
            CatalogSchemaTableName table)
    {
        queryAndEnforce(
                context,
                actionName,
                () -> denyCallable.accept(table.toString()),
                OpaQueryInputResource.Builder::table,
                TrinoTable.fromTrinoTable(table));
    }

    public void queryAndEnforce(
            OpaQueryContext context,
            String actionName,
            BiConsumer<String, Set<String>> denyCallable,
            CatalogSchemaTableName table,
            Set<String> columns)
    {
        queryAndEnforce(
                context,
                actionName,
                () -> denyCallable.accept(table.toString(), columns),
                OpaQueryInputResource.Builder::table,
                TrinoTable.Builder.fromTrinoTable(table).columns(columns).build());
    }

    public void queryAndEnforce(
            OpaQueryContext context,
            String actionName,
            Consumer<String> denyCallable,
            CatalogSchemaTableName table,
            Map<String, ?> properties)
    {
        queryAndEnforce(
                context,
                actionName,
                () -> denyCallable.accept(table.toString()),
                OpaQueryInputResource.Builder::table,
                TrinoTable.Builder.fromTrinoTable(table).properties(properties).build());
    }

    public void queryAndEnforce(
            OpaQueryContext context,
            String actionName,
            DenyCallable denyCallable,
            Identity identity)
    {
        queryAndEnforce(context, actionName, denyCallable, OpaQueryInputResource.Builder::user, new TrinoUser(identity));
    }

    public void queryAndEnforce(
            OpaQueryContext context,
            String actionName,
            DenyCallable denyCallable)
    {
        if (!queryOpaWithSimpleAction(context, actionName)) {
            denyCallable.deny();
        }
    }

    public <T> Set<T> parallelFilterFromOpa(
            Collection<T> items,
            Function<T, OpaQueryInput> requestBuilder)
    {
        return opaHttpClient.parallelFilterFromOpa(items, requestBuilder, opaPolicyUri, queryResultCodec);
    }

    public static OpaQueryInput buildQueryInputForSimpleAction(OpaQueryContext context, String operation)
    {
        return new OpaQueryInput(context, OpaQueryInputAction.builder().operation(operation).build());
    }

    public static OpaQueryInput buildQueryInputForSimpleResource(OpaQueryContext context, String operation, OpaQueryInputResource resource)
    {
        return new OpaQueryInput(context, OpaQueryInputAction.builder().operation(operation).resource(resource).build());
    }

    interface DenyCallable
    {
        void deny();
    }
}
