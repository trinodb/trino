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
package io.trino.client;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.units.Duration;
import jakarta.annotation.Nullable;
import okhttp3.Call;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ProtocolException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.getCausalChain;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.net.HttpHeaders.ACCEPT_ENCODING;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static io.trino.client.HttpStatusCodes.shouldRetry;
import static io.trino.client.JsonCodec.jsonCodec;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
class StatementClientV1
        implements StatementClient
{
    private static final MediaType MEDIA_TYPE_TEXT = MediaType.parse("text/plain; charset=utf-8");
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    private static final Splitter COLLECTION_HEADER_SPLITTER = Splitter.on('=').limit(2).trimResults();
    private static final String USER_AGENT_VALUE = StatementClientV1.class.getSimpleName() +
            "/" +
            firstNonNull(StatementClientV1.class.getPackage().getImplementationVersion(), "unknown");
    private static final long MAX_MATERIALIZED_JSON_RESPONSE_SIZE = 128 * 1024;

    private final Call.Factory httpCallFactory;
    private final String query;
    private final AtomicReference<QueryResults> currentResults = new AtomicReference<>();
    private final AtomicReference<ResultRows> currentRows = new AtomicReference<>();
    private final AtomicReference<String> setCatalog = new AtomicReference<>();
    private final AtomicReference<String> setSchema = new AtomicReference<>();
    private final AtomicReference<List<String>> setPath = new AtomicReference<>();
    private final AtomicReference<String> setAuthorizationUser = new AtomicReference<>();
    private final AtomicBoolean resetAuthorizationUser = new AtomicBoolean();
    private final Map<String, String> setSessionProperties = new ConcurrentHashMap<>();
    private final Set<String> resetSessionProperties = Sets.newConcurrentHashSet();
    private final Map<String, ClientSelectedRole> setRoles = new ConcurrentHashMap<>();
    private final Map<String, String> addedPreparedStatements = new ConcurrentHashMap<>();
    private final Set<String> deallocatedPreparedStatements = Sets.newConcurrentHashSet();
    private final AtomicReference<String> startedTransactionId = new AtomicReference<>();
    private final AtomicBoolean clearTransactionId = new AtomicBoolean();
    private final ZoneId timeZone;
    private final Duration requestTimeoutNanos;
    private final Optional<String> user;
    private final Optional<String> originalUser;
    private final String clientCapabilities;
    private final boolean compressionDisabled;

    private final AtomicReference<State> state = new AtomicReference<>(State.RUNNING);

    // Data accessor for raw and encoded data
    private final ResultRowsDecoder resultRowsDecoder;

    public StatementClientV1(Call.Factory httpCallFactory, Call.Factory segmentHttpCallFactory, ClientSession session, String query, Optional<Set<String>> clientCapabilities)
    {
        requireNonNull(httpCallFactory, "httpCallFactory is null");
        requireNonNull(session, "session is null");
        requireNonNull(query, "query is null");

        this.httpCallFactory = httpCallFactory;
        this.timeZone = session.getTimeZone();
        this.query = query;
        this.requestTimeoutNanos = session.getClientRequestTimeout();
        this.user = Stream.of(session.getAuthorizationUser(), session.getSessionUser(), session.getUser())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
        this.originalUser = Stream.of(session.getSessionUser(), session.getUser())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
        this.clientCapabilities = Joiner.on(",").join(clientCapabilities.orElseGet(() -> stream(ClientCapabilities.values())
                .map(Enum::name)
                .collect(toImmutableSet())));
        this.compressionDisabled = session.isCompressionDisabled();

        this.resultRowsDecoder = new ResultRowsDecoder(new OkHttpSegmentLoader(requireNonNull(segmentHttpCallFactory, "segmentHttpCallFactory is null")));

        Request request = buildQueryRequest(session, query, session.getEncoding());
        // Pass empty as materializedJsonSizeLimit to always materialize the first response
        // to avoid losing the response body if the initial response parsing fails
        executeRequest(request, "starting query", OptionalLong.empty(), this::isTransient);
    }

    private Request buildQueryRequest(ClientSession session, String query, Optional<String> requestedEncoding)
    {
        HttpUrl url = HttpUrl.get(session.getServer());
        if (url == null) {
            throw new ClientException("Invalid server URL: " + session.getServer());
        }
        url = url.newBuilder().encodedPath("/v1/statement").build();

        Request.Builder builder = prepareRequest(url)
                .post(RequestBody.create(query, MEDIA_TYPE_TEXT));

        if (session.getSource() != null) {
            builder.addHeader(TRINO_HEADERS.requestSource(), session.getSource());
        }

        session.getTraceToken().ifPresent(token -> builder.addHeader(TRINO_HEADERS.requestTraceToken(), token));

        if (session.getClientTags() != null && !session.getClientTags().isEmpty()) {
            builder.addHeader(TRINO_HEADERS.requestClientTags(), Joiner.on(",").join(session.getClientTags()));
        }
        if (session.getClientInfo() != null) {
            builder.addHeader(TRINO_HEADERS.requestClientInfo(), session.getClientInfo());
        }
        session.getCatalog().ifPresent(value -> builder.addHeader(TRINO_HEADERS.requestCatalog(), value));
        session.getSchema().ifPresent(value -> builder.addHeader(TRINO_HEADERS.requestSchema(), value));
        if (session.getPath() != null && !session.getPath().isEmpty()) {
            builder.addHeader(TRINO_HEADERS.requestPath(), Joiner.on(",").join(session.getPath()));
        }
        builder.addHeader(TRINO_HEADERS.requestTimeZone(), session.getTimeZone().getId());
        if (session.getLocale() != null) {
            builder.addHeader(TRINO_HEADERS.requestLanguage(), session.getLocale().toLanguageTag());
        }

        Map<String, String> property = session.getProperties();
        for (Entry<String, String> entry : property.entrySet()) {
            builder.addHeader(TRINO_HEADERS.requestSession(), entry.getKey() + "=" + urlEncode(entry.getValue()));
        }

        Map<String, String> resourceEstimates = session.getResourceEstimates();
        for (Entry<String, String> entry : resourceEstimates.entrySet()) {
            builder.addHeader(TRINO_HEADERS.requestResourceEstimate(), entry.getKey() + "=" + urlEncode(entry.getValue()));
        }

        Map<String, ClientSelectedRole> roles = session.getRoles();
        for (Entry<String, ClientSelectedRole> entry : roles.entrySet()) {
            builder.addHeader(TRINO_HEADERS.requestRole(), entry.getKey() + '=' + urlEncode(entry.getValue().toString()));
        }

        Map<String, String> extraCredentials = session.getExtraCredentials();
        for (Entry<String, String> entry : extraCredentials.entrySet()) {
            builder.addHeader(TRINO_HEADERS.requestExtraCredential(), entry.getKey() + "=" + urlEncode(entry.getValue()));
        }

        Map<String, String> statements = session.getPreparedStatements();
        for (Entry<String, String> entry : statements.entrySet()) {
            builder.addHeader(TRINO_HEADERS.requestPreparedStatement(), urlEncode(entry.getKey()) + "=" + urlEncode(entry.getValue()));
        }

        builder.addHeader(TRINO_HEADERS.requestTransactionId(), session.getTransactionId() == null ? "NONE" : session.getTransactionId());

        builder.addHeader(TRINO_HEADERS.requestClientCapabilities(), clientCapabilities);

        requestedEncoding.ifPresent(encoding -> builder.addHeader(TRINO_HEADERS.requestQueryDataEncoding(), encoding));

        return builder.build();
    }

    @Override
    public String getQuery()
    {
        return query;
    }

    @Override
    public ZoneId getTimeZone()
    {
        return timeZone;
    }

    @Override
    public boolean isRunning()
    {
        return state.get() == State.RUNNING;
    }

    @Override
    public boolean isClientAborted()
    {
        return state.get() == State.CLIENT_ABORTED;
    }

    @Override
    public boolean isClientError()
    {
        return state.get() == State.CLIENT_ERROR;
    }

    @Override
    public boolean isFinished()
    {
        return state.get() == State.FINISHED;
    }

    @Override
    public StatementStats getStats()
    {
        return currentResults.get().getStats();
    }

    @Override
    public QueryStatusInfo currentStatusInfo()
    {
        return currentResults.get();
    }

    @Override
    public ResultRows currentRows()
    {
        checkState(isRunning(), "current position is not valid (cursor past end)");
        return currentRows.get();
    }

    @Override
    public QueryData currentData() // Raw over the wire representation
    {
        checkState(isRunning(), "current position is not valid (cursor past end)");
        QueryResults queryResults = currentResults.get();

        if (queryResults == null || queryResults.getData() == null) {
            return null;
        }

        return queryResults.getData();
    }

    @Override
    public QueryStatusInfo finalStatusInfo()
    {
        checkState(!isRunning(), "current position is still valid");
        return currentResults.get();
    }

    @Override
    public Optional<String> getEncoding()
    {
        return resultRowsDecoder.getEncoding();
    }

    @Override
    public Optional<String> getSetCatalog()
    {
        return Optional.ofNullable(setCatalog.get());
    }

    @Override
    public Optional<String> getSetSchema()
    {
        return Optional.ofNullable(setSchema.get());
    }

    @Override
    public Optional<List<String>> getSetPath()
    {
        return Optional.ofNullable(setPath.get());
    }

    @Override
    public Optional<String> getSetAuthorizationUser()
    {
        return Optional.ofNullable(setAuthorizationUser.get());
    }

    @Override
    public boolean isResetAuthorizationUser()
    {
        return resetAuthorizationUser.get();
    }

    @Override
    public Map<String, String> getSetSessionProperties()
    {
        return ImmutableMap.copyOf(setSessionProperties);
    }

    @Override
    public Set<String> getResetSessionProperties()
    {
        return ImmutableSet.copyOf(resetSessionProperties);
    }

    @Override
    public Map<String, ClientSelectedRole> getSetRoles()
    {
        return ImmutableMap.copyOf(setRoles);
    }

    @Override
    public Map<String, String> getAddedPreparedStatements()
    {
        return ImmutableMap.copyOf(addedPreparedStatements);
    }

    @Override
    public Set<String> getDeallocatedPreparedStatements()
    {
        return ImmutableSet.copyOf(deallocatedPreparedStatements);
    }

    @Override
    @Nullable
    public String getStartedTransactionId()
    {
        return startedTransactionId.get();
    }

    @Override
    public boolean isClearTransactionId()
    {
        return clearTransactionId.get();
    }

    private Request.Builder prepareRequest(HttpUrl url)
    {
        Request.Builder builder = new Request.Builder()
                .addHeader(USER_AGENT, USER_AGENT_VALUE)
                .url(url);
        user.ifPresent(requestUser -> builder.addHeader(TRINO_HEADERS.requestUser(), requestUser));
        originalUser.ifPresent(originalUser -> builder.addHeader(TRINO_HEADERS.requestOriginalUser(), originalUser));
        if (compressionDisabled) {
            builder.header(ACCEPT_ENCODING, "identity");
        }
        return builder;
    }

    @Override
    public boolean advance()
    {
        if (!isRunning()) {
            return false;
        }

        URI nextUri = currentStatusInfo().getNextUri();
        if (nextUri == null) {
            state.compareAndSet(State.RUNNING, State.FINISHED);
            return false;
        }

        Request request = prepareRequest(HttpUrl.get(nextUri)).build();
        return executeRequest(request, "fetching next", OptionalLong.of(MAX_MATERIALIZED_JSON_RESPONSE_SIZE), (e) -> true);
    }

    private boolean executeRequest(Request request, String taskName, OptionalLong materializedJsonSizeLimit, Function<Exception, Boolean> isRetryable)
    {
        Exception cause = null;
        long start = System.nanoTime();
        long attempts = 0;

        while (true) {
            if (isClientAborted()) {
                return false;
            }

            if (attempts > 0) {
                Duration sinceStart = Duration.nanosSince(start);
                if (sinceStart.compareTo(requestTimeoutNanos) > 0) {
                    state.compareAndSet(State.RUNNING, State.CLIENT_ERROR);
                    throw new RuntimeException(format("Error fetching next (attempts: %s, duration: %s)", attempts, sinceStart), cause);
                }
                // back-off on retry
                try {
                    MILLISECONDS.sleep(attempts * 100);
                }
                catch (InterruptedException e) {
                    try {
                        close();
                    }
                    finally {
                        Thread.currentThread().interrupt();
                    }
                    state.compareAndSet(State.RUNNING, State.CLIENT_ERROR);
                    throw new RuntimeException("StatementClient thread was interrupted");
                }
            }
            attempts++;

            JsonResponse<QueryResults> response;
            try {
                response = JsonResponse.execute(QUERY_RESULTS_CODEC, httpCallFactory, request, materializedJsonSizeLimit);
            }
            catch (RuntimeException e) {
                if (!isRetryable.apply(e)) {
                    throw e;
                }
                cause = e;
                continue;
            }
            if (isTransient(response.getException())) {
                cause = response.getException();
                continue;
            }
            if (response.getStatusCode() != HTTP_OK || !response.hasValue()) {
                if (!shouldRetry(response.getStatusCode())) {
                    state.compareAndSet(State.RUNNING, State.CLIENT_ERROR);
                    throw requestFailedException(taskName, request, response);
                }
                continue;
            }

            processResponse(response.getHeaders(), response.getValue());
            return true;
        }
    }

    private boolean isTransient(Throwable exception)
    {
        return exception != null && getCausalChain(exception).stream()
                .anyMatch(e -> (e instanceof InterruptedIOException && e.getMessage().equals("timeout")
                        || e instanceof ProtocolException
                        || e instanceof SocketTimeoutException));
    }

    private void processResponse(Headers headers, QueryResults results)
    {
        setCatalog.set(headers.get(TRINO_HEADERS.responseSetCatalog()));
        setSchema.set(headers.get(TRINO_HEADERS.responseSetSchema()));
        setPath.set(safeSplitToList(headers.get(TRINO_HEADERS.responseSetPath())));
        String setAuthorizationUser = headers.get(TRINO_HEADERS.responseSetAuthorizationUser());
        if (setAuthorizationUser != null) {
            this.setAuthorizationUser.set(setAuthorizationUser);
        }

        String resetAuthorizationUser = headers.get(TRINO_HEADERS.responseResetAuthorizationUser());
        if (resetAuthorizationUser != null) {
            this.resetAuthorizationUser.set(Boolean.parseBoolean(resetAuthorizationUser));
        }

        for (String setSession : headers.values(TRINO_HEADERS.responseSetSession())) {
            List<String> keyValue = COLLECTION_HEADER_SPLITTER.splitToList(setSession);
            if (keyValue.size() != 2) {
                continue;
            }
            setSessionProperties.put(keyValue.get(0), urlDecode(keyValue.get(1)));
        }
        resetSessionProperties.addAll(headers.values(TRINO_HEADERS.responseClearSession()));

        for (String setRole : headers.values(TRINO_HEADERS.responseSetRole())) {
            List<String> keyValue = COLLECTION_HEADER_SPLITTER.splitToList(setRole);
            if (keyValue.size() != 2) {
                continue;
            }
            setRoles.put(keyValue.get(0), ClientSelectedRole.valueOf(urlDecode(keyValue.get(1))));
        }

        for (String entry : headers.values(TRINO_HEADERS.responseAddedPrepare())) {
            List<String> keyValue = COLLECTION_HEADER_SPLITTER.splitToList(entry);
            if (keyValue.size() != 2) {
                continue;
            }
            addedPreparedStatements.put(urlDecode(keyValue.get(0)), urlDecode(keyValue.get(1)));
        }
        for (String entry : headers.values(TRINO_HEADERS.responseDeallocatedPrepare())) {
            deallocatedPreparedStatements.add(urlDecode(entry));
        }

        String startedTransactionId = headers.get(TRINO_HEADERS.responseStartedTransactionId());
        if (startedTransactionId != null) {
            this.startedTransactionId.set(startedTransactionId);
        }
        if (headers.get(TRINO_HEADERS.responseClearTransactionId()) != null) {
            clearTransactionId.set(true);
        }

        currentResults.set(results);
        currentRows.set(resultRowsDecoder.toRows(results));
    }

    private List<String> safeSplitToList(String value)
    {
        if (value == null || value.isEmpty()) {
            return ImmutableList.of();
        }
        return Splitter.on(',').trimResults().splitToList(value);
    }

    private RuntimeException requestFailedException(String task, Request request, JsonResponse<QueryResults> response)
    {
        if (!response.hasValue()) {
            if (response.getStatusCode() == HTTP_UNAUTHORIZED) {
                return new ClientException("Authentication failed" +
                        response.getResponseBody()
                                .map(message -> ": " + message)
                                .orElse(""));
            }
            return new RuntimeException(
                    format("Error %s at %s returned an invalid response: %s [Error: %s]", task, request.url(), response, response.getResponseBody().orElse("<Response Too Large>")),
                    response.getException());
        }
        return new RuntimeException(format("Error %s at %s returned HTTP %s", task, request.url(), response.getStatusCode()));
    }

    @Override
    public void cancelLeafStage()
    {
        checkState(!isClientAborted(), "client is closed");

        URI uri = currentStatusInfo().getPartialCancelUri();
        if (uri != null) {
            httpDelete(uri);
        }
    }

    @Override
    public void close()
    {
        // If the query is not done, abort the query.
        if (state.compareAndSet(State.RUNNING, State.CLIENT_ABORTED)) {
            URI uri = currentResults.get().getNextUri();
            if (uri != null) {
                httpDelete(uri);
            }
        }
    }

    private void httpDelete(URI uri)
    {
        Request request = prepareRequest(HttpUrl.get(uri))
                .delete()
                .build();
        try {
            httpCallFactory.newCall(request)
                    .execute()
                    .close();
        }
        catch (IOException ignored) {
            // callers expect this method not to throw
        }
    }

    private static String urlEncode(String value)
    {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static String urlDecode(String value)
    {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private enum State
    {
        /**
         * submitted to server, not in terminal state (including planning, queued, running, etc)
         */
        RUNNING,
        CLIENT_ERROR,
        CLIENT_ABORTED,
        /**
         * finished on remote Trino server (including failed and successfully completed)
         */
        FINISHED,
    }
}
