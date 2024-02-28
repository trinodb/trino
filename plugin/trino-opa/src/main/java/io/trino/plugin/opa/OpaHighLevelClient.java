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

import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.plugin.opa.schema.OpaQueryContext;
import io.trino.plugin.opa.schema.OpaQueryInput;
import io.trino.plugin.opa.schema.OpaQueryInputAction;
import io.trino.plugin.opa.schema.OpaQueryInputResource;
import io.trino.plugin.opa.schema.OpaQueryResult;
import io.trino.spi.security.AccessDeniedException;

import java.net.URI;
import java.util.Collection;
import java.util.Set;
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

    public static OpaQueryInput buildQueryInputForSimpleResource(OpaQueryContext context, String operation, OpaQueryInputResource resource)
    {
        return new OpaQueryInput(context, OpaQueryInputAction.builder().operation(operation).resource(resource).build());
    }

    private static OpaQueryInput buildQueryInputForSimpleAction(OpaQueryContext context, String operation)
    {
        return new OpaQueryInput(context, OpaQueryInputAction.builder().operation(operation).build());
    }
}
