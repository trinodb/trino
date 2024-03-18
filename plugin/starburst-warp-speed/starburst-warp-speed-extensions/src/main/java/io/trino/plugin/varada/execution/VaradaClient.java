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
package io.trino.plugin.varada.execution;

import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClient.HttpResponseFuture;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.JsonBodyGenerator;
import io.airlift.http.client.Request;
import io.airlift.http.client.ResponseHandler;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.impl.DefaultJwtBuilder;
import io.jsonwebtoken.jackson.io.JacksonSerializer;
import io.trino.plugin.warp.extension.configuration.WarpExtensionConfiguration;
import io.varada.tools.CatalogNameProvider;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static io.jsonwebtoken.security.Keys.hmacShaKeyFor;
import static io.trino.plugin.warp.extension.configuration.WarpExtensionConfiguration.HTTP_REST_PORT_ENABLED;
import static io.trino.plugin.warp.extension.configuration.WarpExtensionConfiguration.USE_HTTP_SERVER_PORT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("unchecked")
@Singleton
public class VaradaClient
{
    public static final JsonCodec<Void> VOID_RESULTS_CODEC = JsonCodec.jsonCodec(Void.class);
    private static final Logger logger = Logger.get(VaradaClient.class);
    private final CatalogNameProvider catalogNameProvider;
    private final HttpClient httpClient;
    private final WarpExtensionConfiguration warpExtensionConfiguration;
    private final Optional<Supplier<JwtBuilder>> jwtBuilder;

    @Inject
    public VaradaClient(CatalogNameProvider catalogNameProvider,
            @ForVarada HttpClient httpClient,
            WarpExtensionConfiguration warpExtensionConfiguration)
    {
        this.catalogNameProvider = requireNonNull(catalogNameProvider, "httpClient is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.warpExtensionConfiguration = requireNonNull(warpExtensionConfiguration);

        jwtBuilder = warpExtensionConfiguration.getInternalCommunicationSharedSecret() != null ?
                Optional.of(() -> new DefaultJwtBuilder()
                        .serializeToJsonWith(new JacksonSerializer<>())
                        .signWith(hmacShaKeyFor(Hashing.sha256().hashString(warpExtensionConfiguration.getInternalCommunicationSharedSecret(), UTF_8).asBytes()))
                        .subject(warpExtensionConfiguration.getClusterUUID())
                        .expiration(Date.from(ZonedDateTime.now(ZoneId.systemDefault()).plusMinutes(5).toInstant()))) :
                Optional.empty();
    }

    public static boolean isOk(int statusCode)
    {
        return (statusCode == HttpStatus.OK.code()) ||
                (statusCode == HttpStatus.NO_CONTENT.code());
    }

    public <T extends TaskData> void sendWithRetry(HttpUriBuilder uri, T taskData)
    {
        if (Objects.isNull(uri)) {
            throw new RuntimeException("uri is null");
        }
        URI fullUri = uri.appendPath(catalogNameProvider.getRestTaskPrefix())
                .appendPath(taskData.getTaskName()).build();
        String callerName = taskData.getClass().getSimpleName();
        Request.Builder builder = Request.Builder.preparePost()
                .setUri(fullUri)
                .addHeaders(taskData.getTaskHeaders())
                .setBodyGenerator(JsonBodyGenerator.jsonBodyGenerator(taskData.getCodec(), taskData))
                .addHeader("X-Trino-User", "internal");
        handleBearer(builder);

        sendWithRetry(builder.build(), FullJsonResponseHandler.createFullJsonResponseHandler(VOID_RESULTS_CODEC), callerName);
    }

    public <T> T sendWithRetry(Request request, FullJsonResponseHandler<T> responseHandler)
    {
        return sendWithRetry(request, responseHandler, "");
    }

    public <T> T sendWithRetry(Request request, FullJsonResponseHandler<T> responseHandler, String callerName)
    {
        Request.Builder builder = Request.Builder.fromRequest(request)
                .addHeader("X-Trino-User", "internal");
        handleBearer(builder);
        Request requestWithUser = builder.build();
        return invokeWithRetry(() -> {
            JsonResponse<T> response = httpClient.execute(requestWithUser, responseHandler);
            if (isOk(response.getStatusCode())) {
                if (response.getJson() != null) {
                    return response.getValue();
                }
                else {
                    return null;
                }
            }
            else {
                logger.error("error response for %s : %d=>%s / %s",
                        requestWithUser.getUri(),
                        response.getStatusCode(),
                        response.getJson(),
                        response.getException());
                if (response.getException() != null) {
                    throw response.getException();
                }
                throw new IOException("failed response " + response.getStatusCode());
            }
        }, request.getUri(), callerName);
    }

    public <T, E extends Exception> HttpResponseFuture<T> executeAsync(Request request, ResponseHandler<T, E> responseHandler)
    {
        Request.Builder builder = Request.Builder.fromRequest(request)
                .addHeader("X-Trino-User", "internal");
        handleBearer(builder);
        return httpClient.executeAsync(builder.build(), responseHandler);
    }

    public <T> T invokeWithRetry(Callable<T> func, URI uri, String callerName)
    {
        return Failsafe.with(new RetryPolicy<>()
                        .withMaxAttempts(2)
                        .onFailedAttempt((executionAttemptedEvent) -> logger.warn(executionAttemptedEvent.getLastFailure(), "failed executing %s REST command to URI %s", callerName, uri))
                        .withDelay(Duration.ofSeconds(10))
                        .handle(IOException.class, UncheckedIOException.class, IllegalStateException.class))
                .get(func::call);
    }

    public HttpUriBuilder getRestEndpoint(URI nodeUri)
    {
        return getRestEndpoint(nodeUri, true);
    }

    public HttpUriBuilder getRestEndpoint(URI nodeUri, boolean isInternal)
    {
        int restPort;
        try {
            restPort = nodeUri.getPort() > 0 ? nodeUri.getPort() : URI.create(nodeUri.toString()).toURL().getDefaultPort();
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        if (!warpExtensionConfiguration.isUseHttpServerPort()) {
            restPort = getRestHttpPort(
                    Map.of(USE_HTTP_SERVER_PORT, Boolean.toString(warpExtensionConfiguration.isUseHttpServerPort()),
                            HTTP_REST_PORT_ENABLED, Boolean.toString(warpExtensionConfiguration.isRestHttpDefaultPortEnabled()),
                            WarpExtensionConfiguration.HTTP_REST_PORT, Integer.toString(warpExtensionConfiguration.getRestHttpPort())),
                    nodeUri.getPort());
        }
        HttpUriBuilder uriBuilder = HttpUriBuilder.uriBuilderFrom(nodeUri)
                .port(restPort);
        if (warpExtensionConfiguration.isUseHttpServerPort()) {
            uriBuilder.appendPath("ext");
            if (isInternal && warpExtensionConfiguration.getInternalCommunicationSharedSecret() != null) {
                uriBuilder.appendPath("internal");
            }
            uriBuilder.appendPath(catalogNameProvider.get());
        }
        return uriBuilder;
    }

    private void handleBearer(Request.Builder builder)
    {
        jwtBuilder.ifPresent(jwtBuilderSupplier -> builder.addHeader("X-Trino-Internal-Bearer", jwtBuilderSupplier.get().compact()));
    }

    public static int getRestHttpPort(Map<String, String> config, int nodePort)
    {
        return Integer.parseInt(getRestHttpPortStr(config, nodePort));
    }

    public static String getRestHttpPortStr(Map<String, String> config, int nodePort)
    {
        String restPort = config.getOrDefault(
                WarpExtensionConfiguration.HTTP_REST_PORT,
                String.valueOf(WarpExtensionConfiguration.HTTP_REST_DEFAULT_PORT));
        if (!Boolean.parseBoolean(config.getOrDefault(USE_HTTP_SERVER_PORT, Boolean.TRUE.toString()))) {
            if (!Boolean.parseBoolean(config.getOrDefault(HTTP_REST_PORT_ENABLED, Boolean.TRUE.toString()))) {
                restPort = String.valueOf(nodePort + 1);
            }
        }
        return restPort;
    }
}
