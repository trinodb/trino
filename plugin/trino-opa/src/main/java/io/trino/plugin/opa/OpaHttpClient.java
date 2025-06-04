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
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.JsonBodyGenerator;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.plugin.opa.schema.OpaBatchQueryResult;
import io.trino.plugin.opa.schema.OpaColumnMaskQueryResult;
import io.trino.plugin.opa.schema.OpaQuery;
import io.trino.plugin.opa.schema.OpaQueryInput;
import io.trino.plugin.opa.schema.OpaQueryResult;
import io.trino.plugin.opa.schema.OpaViewExpression;
import io.trino.spi.connector.ColumnSchema;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.preparePost;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

public class OpaHttpClient
{
    private final HttpClient httpClient;
    private final JsonCodec<OpaQuery> serializer;
    private final Executor executor;
    private final boolean logRequests;
    private final boolean logResponses;
    private static final Logger log = Logger.get(OpaHttpClient.class);

    @Inject
    public OpaHttpClient(
            @ForOpa HttpClient httpClient,
            JsonCodec<OpaQuery> serializer,
            @ForOpa Executor executor,
            OpaConfig config)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.serializer = requireNonNull(serializer, "serializer is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.logRequests = config.getLogRequests();
        this.logResponses = config.getLogResponses();
    }

    public <T> FluentFuture<T> submitOpaRequest(OpaQueryInput input, URI uri, JsonCodec<T> deserializer)
    {
        Request request;
        JsonBodyGenerator<OpaQuery> requestBodyGenerator;
        try {
            requestBodyGenerator = jsonBodyGenerator(serializer, new OpaQuery(input));
            request = preparePost()
                    .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                    .setUri(uri)
                    .setBodyGenerator(requestBodyGenerator)
                    .build();
        }
        catch (IllegalArgumentException e) {
            log.error(e, "Failed to serialize OPA request body when attempting to send request to URI \"%s\"", uri.toString());
            throw new OpaQueryException.SerializeFailed(e);
        }
        if (logRequests) {
            log.debug(
                    "Sending OPA request to URI \"%s\" ; request body = %s ; request headers = %s",
                    uri.toString(),
                    new String(requestBodyGenerator.getBody(), UTF_8),
                    request.getHeaders());
        }
        return FluentFuture.from(httpClient.executeAsync(request, createFullJsonResponseHandler(deserializer)))
                .transform(response -> parseOpaResponse(response, uri), executor);
    }

    public <T> T consumeOpaResponse(ListenableFuture<T> opaResponseFuture)
    {
        try {
            return opaResponseFuture.get();
        }
        catch (ExecutionException e) {
            if (e.getCause() instanceof OpaQueryException queryException) {
                throw queryException;
            }
            log.error(e, "Failed to obtain response from OPA due to an unknown error");
            throw new OpaQueryException.QueryFailed(e);
        }
        catch (InterruptedException e) {
            log.error(e, "OPA request was interrupted in flight");
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public Map<ColumnSchema, OpaViewExpression> parallelColumnMasksFromOpa(List<ColumnSchema> items, Function<ColumnSchema, OpaQueryInput> requestBuilder, URI uri, JsonCodec<? extends OpaColumnMaskQueryResult> deserializer)
    {
        return parallelRequest(
                items,
                requestBuilder,
                (item, result) -> result.result().map(viewExpression -> Map.entry(item, viewExpression)),
                uri,
                deserializer).stream().collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public <T> Set<T> parallelFilterFromOpa(Collection<T> items, Function<T, OpaQueryInput> requestBuilder, URI uri, JsonCodec<? extends OpaQueryResult> deserializer)
    {
        return ImmutableSet.copyOf(parallelRequest(
                items,
                requestBuilder,
                (item, result) -> result.result() ? Optional.of(item) : Optional.empty(),
                uri,
                deserializer));
    }

    public <T> Set<T> batchFilterFromOpa(Collection<T> items, Function<List<T>, OpaQueryInput> requestBuilder, URI uri, JsonCodec<? extends OpaBatchQueryResult> deserializer)
    {
        if (items.isEmpty()) {
            return ImmutableSet.of();
        }
        String dummyMapKey = "filter";
        return parallelBatchFilterFromOpa(ImmutableMap.of(dummyMapKey, items), (mapKey, mapValue) -> requestBuilder.apply(mapValue), uri, deserializer).getOrDefault(dummyMapKey, ImmutableSet.of());
    }

    public <K, V> Map<K, Set<V>> parallelBatchFilterFromOpa(Map<K, ? extends Collection<V>> items, BiFunction<K, List<V>, OpaQueryInput> requestBuilder, URI uri, JsonCodec<? extends OpaBatchQueryResult> deserializer)
    {
        List<Map.Entry<K, ImmutableList<V>>> parallelRequestItems = items.entrySet()
                .stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .map(entry -> Map.entry(entry.getKey(), ImmutableList.copyOf(entry.getValue())))
                .collect(toImmutableList());
        return parallelRequest(
                    parallelRequestItems,
                    entry -> requestBuilder.apply(entry.getKey(), entry.getValue()),
                    (entry, result) ->
                            Optional.of(requireNonNullElse(result.result(), ImmutableList.<Integer>of()))
                                    .flatMap(indices -> indices.isEmpty() ? Optional.empty() : Optional.of(indices))
                                    .map(indices -> indices.stream()
                                        .map(index -> entry.getValue().get(index))
                                        .collect(toImmutableSet()))
                                    .map(values -> Map.entry(entry.getKey(), values)),
                    uri,
                    deserializer)
                .stream()
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private <T> T parseOpaResponse(FullJsonResponseHandler.JsonResponse<T> response, URI uri)
    {
        int statusCode = response.getStatusCode();
        String uriString = uri.toString();
        if (HttpStatus.familyForStatusCode(statusCode) != HttpStatus.Family.SUCCESSFUL) {
            if (statusCode == HttpStatus.NOT_FOUND.code()) {
                log.warn("OPA responded with not found error for policy with URI \"%s\"", uriString);
                throw new OpaQueryException.PolicyNotFound(uriString);
            }

            log.error("Received unknown error from OPA for URI \"%s\" with status code = %d", uriString, statusCode);
            throw new OpaQueryException.OpaServerError(uriString, statusCode, response.toString());
        }
        if (!response.hasValue()) {
            log.error(response.getException(), "OPA response for URI \"%s\" with status code = %d could not be deserialized", uriString, statusCode);
            throw new OpaQueryException.DeserializeFailed(response.getException());
        }
        if (logResponses) {
            log.debug(
                    "OPA response for URI \"%s\" received: status code = %d ; response payload = %s ; response headers = %s",
                    uriString,
                    statusCode,
                    new String(response.getJsonBytes(), UTF_8),
                    response.getHeaders());
        }
        return response.getValue();
    }

    private <T, U, X> List<X> parallelRequest(Collection<T> items, Function<T, OpaQueryInput> requestBuilder, BiFunction<T, U, Optional<X>> parser, URI uri, JsonCodec<U> deserializer)
    {
        if (items.isEmpty()) {
            return ImmutableList.of();
        }
        List<FluentFuture<Optional<X>>> allFutures = items.stream()
                .map(item -> submitOpaRequest(requestBuilder.apply(item), uri, deserializer)
                        .transform(result -> parser.apply(item, result), executor))
                .collect(toImmutableList());
        return consumeOpaResponse(
                Futures.whenAllComplete(allFutures).call(() -> allFutures.stream()
                                .map(this::consumeOpaResponse)
                                .filter(Optional::isPresent)
                                .map(Optional::get)
                                .collect(toImmutableList()),
                        executor));
    }
}
