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
package io.trino.plugin.inference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.airlift.stats.CounterStat;
import io.airlift.stats.TimeStat;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.plugin.inference.InferenceErrorCode.INFERENCE_AUTHENTICATION_FAILED;
import static io.trino.plugin.inference.InferenceErrorCode.INFERENCE_INVALID_RESPONSE;
import static io.trino.plugin.inference.InferenceErrorCode.INFERENCE_SERVER_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.readString;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static tools.jackson.core.json.JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS;

/**
 * Client for the REST API of the Open Inference Protocol (V2), as implemented by Triton, OpenVINO Model Server, MLServer, and other model servers.
 * <p>
 * Requests that fail to connect, or that the server rejects as temporarily unavailable, are retried with an exponential backoff.
 * The protocol does not define authentication, so the client sends a configured token in an HTTP header, which is what model servers,
 * the proxies in front of them, and managed endpoints check.
 *
 * @see <a href="https://github.com/kserve/open-inference-protocol">Open Inference Protocol</a>
 */
public class OpenInferenceClient
{
    private static final JsonCodec<ModelMetadata> MODEL_METADATA_CODEC = jsonCodec(ModelMetadata.class);
    private static final JsonCodec<InferenceRequest> INFERENCE_REQUEST_CODEC = jsonCodec(InferenceRequest.class);
    // some servers, such as Triton, send NaN and infinite output values as the non-standard JSON numbers NaN, Infinity, and -Infinity
    @VisibleForTesting
    static final JsonCodec<InferenceResponse> INFERENCE_RESPONSE_CODEC = new JsonCodecFactory(new JsonMapperProvider().get()
            .rebuild()
            .enable(ALLOW_NON_NUMERIC_NUMBERS)
            .build())
            .jsonCodec(InferenceResponse.class);

    // too many requests, bad gateway, service unavailable, and gateway timeout are returned while a server or a proxy is overloaded, starting, or restarting
    private static final Set<Integer> RETRYABLE_STATUS_CODES = ImmutableSet.of(429, 502, 503, 504);
    private static final java.time.Duration MIN_RETRY_DELAY = java.time.Duration.ofMillis(500);
    private static final java.time.Duration MAX_RETRY_DELAY = java.time.Duration.ofSeconds(10);

    // error responses can be large, for example an HTML page of a proxy
    private static final int MAX_ERROR_BODY_LENGTH = 1000;

    private final HttpClient httpClient;
    private final URI endpoint;
    private final HeaderName authenticationHeaderName;
    private final Supplier<Optional<String>> authenticationHeaderValue;
    private final ScheduledExecutorService retryExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("inference-retry-%s"));
    private final FailsafeExecutor<StringResponse> failsafe;

    private final TimeStat metadataRequestTime = new TimeStat(MILLISECONDS);
    private final TimeStat inferenceRequestTime = new TimeStat(MILLISECONDS);
    private final CounterStat inferenceRows = new CounterStat();
    private final CounterStat retries = new CounterStat();
    private final CounterStat failedRequests = new CounterStat();

    @Inject
    public OpenInferenceClient(@ForInference HttpClient httpClient, InferenceConfig config)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.endpoint = config.getEndpoint();
        this.authenticationHeaderName = HeaderName.of(config.getAuthenticationHeaderName());
        this.authenticationHeaderValue = authenticationHeaderValue(config);
        RetryPolicy<StringResponse> retryPolicy = RetryPolicy.<StringResponse>builder()
                .withMaxRetries(config.getMaxRetries())
                .withBackoff(MIN_RETRY_DELAY, MAX_RETRY_DELAY)
                .withJitter(0.1)
                .handleIf(OpenInferenceClient::isNetworkFailure)
                .handleResultIf(response -> RETRYABLE_STATUS_CODES.contains(response.getStatusCode()))
                .onRetry(_ -> retries.update(1))
                .build();
        this.failsafe = Failsafe.with(retryPolicy).with(retryExecutor);
    }

    @PreDestroy
    public void shutdown()
    {
        retryExecutor.shutdownNow();
    }

    public ModelMetadata fetchModelMetadata(String modelName, Optional<String> modelVersion)
    {
        URI uri = modelUri(modelName, modelVersion).build();
        long start = System.nanoTime();
        CompletableFuture<ModelMetadata> metadata = execute(() -> prepareGet().setUri(uri))
                .thenApply(response -> parse(MODEL_METADATA_CODEC, uri, response))
                .whenComplete((_, _) -> metadataRequestTime.add(new Duration(System.nanoTime() - start, NANOSECONDS)));
        return getDone(wrapFailure(metadata, "Failed to fetch metadata of model %s from %s".formatted(modelName, uri), _ -> "Check that the model server is running, and the inference.endpoint property of the catalog"));
    }

    public CompletableFuture<InferenceResponse> infer(String modelName, Optional<String> modelVersion, InferenceRequest inferenceRequest)
    {
        URI uri = modelUri(modelName, modelVersion)
                .appendPath("infer")
                .build();
        long start = System.nanoTime();
        CompletableFuture<InferenceResponse> response = execute(() -> preparePost()
                .setUri(uri)
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(INFERENCE_REQUEST_CODEC, inferenceRequest)))
                .thenApply(value -> parse(INFERENCE_RESPONSE_CODEC, uri, value))
                .whenComplete((_, failure) -> {
                    inferenceRequestTime.add(new Duration(System.nanoTime() - start, NANOSECONDS));
                    if (failure == null) {
                        inferenceRows.update(inferenceRequest.inputs().getFirst().shape().getFirst());
                    }
                });
        return wrapFailure(response, "Inference request for model %s to %s failed".formatted(modelName, uri), OpenInferenceClient::inferenceFailureHint);
    }

    private static String inferenceFailureHint(Throwable failure)
    {
        if (Throwables.getCausalChain(failure).stream().anyMatch(TimeoutException.class::isInstance)) {
            return "The model server did not respond in time. Increase the inference.http-client.request-timeout and inference.http-client.idle-timeout properties, or lower the inference.max-batch-size property of the catalog";
        }
        return "Check that the model server is running";
    }

    /**
     * Returns the value of a completed future, and rethrows its failure.
     */
    public static <T> T getDone(CompletableFuture<T> future)
    {
        try {
            return future.join();
        }
        catch (CompletionException e) {
            if (e.getCause() instanceof TrinoException trinoException) {
                throw trinoException;
            }
            throw new TrinoException(INFERENCE_SERVER_ERROR, "Request to the model server failed: " + e.getCause().getMessage(), e.getCause());
        }
    }

    private CompletableFuture<StringResponse> execute(Supplier<Request.Builder> request)
    {
        // build the request for every attempt, so every attempt reads the current token
        return failsafe.getStageAsync(() -> {
            Request.Builder builder = request.get();
            authenticationHeaderValue.get().ifPresent(value -> builder.setHeader(authenticationHeaderName, value));
            return toCompletableFuture(httpClient.executeAsync(builder.build(), createStringResponseHandler()));
        });
    }

    // connection failures and timeouts, which the HTTP client reports as I/O exceptions
    private static boolean isNetworkFailure(Throwable failure)
    {
        Throwable cause = unwrap(failure);
        return cause instanceof IOException || cause instanceof UncheckedIOException || cause instanceof TimeoutException;
    }

    private <T> CompletableFuture<T> wrapFailure(CompletableFuture<T> future, String message, Function<Throwable, String> hint)
    {
        return future.whenComplete((_, failure) -> {
            if (failure != null) {
                failedRequests.update(1);
            }
        }).exceptionallyCompose(failure -> {
            Throwable cause = unwrap(failure);
            if (cause instanceof TrinoException) {
                return CompletableFuture.failedFuture(cause);
            }
            return CompletableFuture.failedFuture(new TrinoException(INFERENCE_SERVER_ERROR, "%s: %s. %s".formatted(
                    message,
                    requireNonNullElse(cause.getMessage(), cause.getClass().getName()),
                    hint.apply(cause)), cause));
        });
    }

    private static Throwable unwrap(Throwable failure)
    {
        if (failure instanceof CompletionException && failure.getCause() != null) {
            return failure.getCause();
        }
        return failure;
    }

    private HttpUriBuilder modelUri(String modelName, Optional<String> modelVersion)
    {
        HttpUriBuilder builder = uriBuilderFrom(endpoint)
                .appendPath("v2/models")
                .appendPath(modelName);
        modelVersion.ifPresent(version -> builder
                .appendPath("versions")
                .appendPath(version));
        return builder;
    }

    private static <T> T parse(JsonCodec<T> codec, URI uri, StringResponse response)
    {
        if (response.getStatusCode() == 401 || response.getStatusCode() == 403) {
            throw new TrinoException(INFERENCE_AUTHENTICATION_FAILED, "Model server rejected the authentication of the request to %s with HTTP status %s. Check the inference.authentication properties of the catalog".formatted(uri, response.getStatusCode()));
        }
        if (response.getStatusCode() != 200) {
            String hint = switch (response.getStatusCode()) {
                case 404 -> ". Check the model_name and model_version arguments, and the inference.endpoint property of the catalog";
                case 413 -> ". Lower the inference.max-batch-size property of the catalog";
                default -> "";
            };
            // error responses of proxies are often HTML pages with many lines
            String body = CharMatcher.whitespace().trimAndCollapseFrom(truncate(response.getBody(), MAX_ERROR_BODY_LENGTH), ' ');
            String message = "Request to %s failed with HTTP status %s%s".formatted(uri, response.getStatusCode(), hint);
            if (!body.isEmpty()) {
                message += ". Response: " + body;
            }
            throw new TrinoException(INFERENCE_SERVER_ERROR, message);
        }
        try {
            return codec.fromJson(response.getBody());
        }
        catch (IllegalArgumentException e) {
            // the root cause is the error of the JSON parser, such as an unexpected character, and its location
            String reason = requireNonNullElse(Throwables.getRootCause(e).getMessage(), "").lines().findFirst().orElse("");
            throw new TrinoException(INFERENCE_INVALID_RESPONSE, "Invalid response from %s: %s. The model server must respond with JSON in the format of the Open Inference Protocol".formatted(uri, reason), e);
        }
    }

    private static Supplier<Optional<String>> authenticationHeaderValue(InferenceConfig config)
    {
        Supplier<Optional<String>> token;
        if (config.getAuthenticationTokenFile().isPresent()) {
            File file = config.getAuthenticationTokenFile().get();
            token = () -> Optional.of(readToken(file));
        }
        else {
            Optional<String> value = config.getAuthenticationToken();
            token = () -> value;
        }
        if (config.getAuthenticationHeaderName().equalsIgnoreCase(AUTHORIZATION)) {
            return () -> token.get().map(value -> "Bearer " + value);
        }
        return token;
    }

    private static String readToken(File file)
    {
        try {
            return readString(file.toPath(), UTF_8).strip();
        }
        catch (IOException e) {
            throw new TrinoException(INFERENCE_AUTHENTICATION_FAILED, "Failed to read the authentication token file " + file, e);
        }
    }

    private static String truncate(String value, int maxLength)
    {
        if (value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength) + "...";
    }

    @Managed
    @Nested
    public TimeStat getMetadataRequestTime()
    {
        return metadataRequestTime;
    }

    @Managed
    @Nested
    public TimeStat getInferenceRequestTime()
    {
        return inferenceRequestTime;
    }

    @Managed
    @Nested
    public CounterStat getInferenceRows()
    {
        return inferenceRows;
    }

    @Managed
    @Nested
    public CounterStat getRetries()
    {
        return retries;
    }

    @Managed
    @Nested
    public CounterStat getFailedRequests()
    {
        return failedRequests;
    }

    // servers can omit the tensor lists
    public record ModelMetadata(String name, List<Tensor> inputs, List<Tensor> outputs)
    {
        public ModelMetadata
        {
            inputs = ImmutableList.copyOf(requireNonNullElse(inputs, ImmutableList.of()));
            outputs = ImmutableList.copyOf(requireNonNullElse(outputs, ImmutableList.of()));
        }
    }

    public record InferenceRequest(List<Tensor> inputs, List<RequestedOutput> outputs)
    {
        public InferenceRequest
        {
            inputs = ImmutableList.copyOf(inputs);
            outputs = ImmutableList.copyOf(outputs);
        }
    }

    public record RequestedOutput(String name) {}

    public record InferenceResponse(List<Tensor> outputs)
    {
        public InferenceResponse
        {
            outputs = ImmutableList.copyOf(requireNonNullElse(outputs, ImmutableList.of()));
        }
    }

    /**
     * A tensor in a request or response, or a tensor description in model metadata.
     * In model metadata, {@code data} is absent, and a shape dimension of {@code -1} means a variable size.
     * Tensor data uses row-major order.
     */
    public record Tensor(String name, String datatype, List<Long> shape, List<Object> data) {}
}
