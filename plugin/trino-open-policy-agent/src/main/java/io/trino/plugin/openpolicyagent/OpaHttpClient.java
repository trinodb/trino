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

import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.trino.plugin.openpolicyagent.schema.OpaQuery;
import io.trino.plugin.openpolicyagent.schema.OpaQueryInput;
import io.trino.plugin.openpolicyagent.schema.OpaQueryResult;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.preparePost;

public class OpaHttpClient
{
    private final HttpClient httpClient;
    private final JsonCodec<OpaQuery> serializer;
    private final Executor executor;

    @Inject
    public OpaHttpClient(
            @ForOpa HttpClient httpClient,
            JsonCodec<OpaQuery> serializer,
            @ForOpa Executor executor)
    {
        this.httpClient = httpClient;
        this.serializer = serializer;
        this.executor = executor;
    }

    public <T> FluentFuture<T> submitOpaRequest(OpaQueryInput input, URI uri, JsonCodec<T> deserializer)
    {
        Request request;
        try {
            request = preparePost()
                    .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                    .setUri(uri)
                    .setBodyGenerator(jsonBodyGenerator(serializer, new OpaQuery(input)))
                    .build();
        }
        catch (IllegalArgumentException e) {
            throw new OpaQueryException.SerializeFailed(e);
        }
        return FluentFuture.from(httpClient.executeAsync(request, createFullJsonResponseHandler(deserializer)))
                .transform((response) -> parseOpaResponse(response, uri), executor);
    }

    public <T> T getOpaResponse(OpaQueryInput input, URI uri, JsonCodec<T> deserializer)
    {
        return propagatingConsumeFuture(submitOpaRequest(input, uri, deserializer));
    }

    private <T> T parseOpaResponse(FullJsonResponseHandler.JsonResponse<T> response, URI uri)
    {
        int statusCode = response.getStatusCode();
        if (HttpStatus.familyForStatusCode(statusCode) != HttpStatus.Family.SUCCESSFUL) {
            if (statusCode == HttpStatus.NOT_FOUND.code()) {
                throw new OpaQueryException.PolicyNotFound(uri.toString());
            }
            throw new OpaQueryException.OpaServerError(uri.toString(), statusCode, response.toString());
        }
        if (!response.hasValue()) {
            throw new OpaQueryException.DeserializeFailed(response.getException());
        }
        return response.getValue();
    }

    private <T> T propagatingConsumeFuture(ListenableFuture<T> opaResponseFuture)
    {
        try {
            return opaResponseFuture.get();
        }
        catch (ExecutionException e) {
            if (e.getCause() != null && e.getCause() instanceof OpaQueryException) {
                throw (OpaQueryException) e.getCause();
            }
            throw new OpaQueryException.QueryFailed(e);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> Set<T> parallelFilterFromOpa(Collection<T> items, Function<T, OpaQueryInput> requestBuilder, URI uri, JsonCodec<? extends OpaQueryResult> deserializer)
    {
        List<FluentFuture<Optional<T>>> allFutures = items
                .stream()
                .map((item) -> submitOpaRequest(requestBuilder.apply(item), uri, deserializer)
                        .transform((result) -> result.result() ? Optional.of(item) : Optional.<T>empty(), executor))
                .collect(toImmutableList());
        return propagatingConsumeFuture(
                Futures.whenAllComplete(allFutures).call(() ->
                                allFutures
                                        .stream()
                                        .map(this::propagatingConsumeFuture)
                                        .filter(Optional::isPresent)
                                        .map(Optional::get)
                                        .collect(toImmutableSet()),
                        executor));
    }
}
