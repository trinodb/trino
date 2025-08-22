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

package io.trino.plugin.teradata.clearscape;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.teradata.clearscape.Headers.APPLICATION_JSON;
import static io.trino.plugin.teradata.clearscape.Headers.AUTHORIZATION;
import static io.trino.plugin.teradata.clearscape.Headers.BEARER;
import static io.trino.plugin.teradata.clearscape.Headers.CONTENT_TYPE;

public class TeradataHttpClient
{
    private final String baseUrl;

    private final HttpClient httpClient;

    private final ObjectMapper objectMapper;

    public TeradataHttpClient(String baseUrl)
    {
        this(HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build(), baseUrl);
    }

    public TeradataHttpClient(HttpClient httpClient, String baseUrl)
    {
        this.httpClient = httpClient;
        this.baseUrl = baseUrl;
        this.objectMapper = JsonMapper.builder()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS, false)
                .build();
    }

    // Creating an environment is a blocking operation by default, and it takes ~1.5min to finish
    public CompletableFuture<EnvironmentResponse> createEnvironment(CreateEnvironmentRequest createEnvironmentRequest,
            String token)
    {
        var requestBody = handleCheckedException(() -> objectMapper.writeValueAsString(createEnvironmentRequest));

        var httpRequest = HttpRequest.newBuilder(URI.create(baseUrl.concat("/environments")))
                .headers(
                        AUTHORIZATION, BEARER + token,
                        CONTENT_TYPE, APPLICATION_JSON)
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        return httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
                .thenApply(httpResponse -> handleHttpResponse(httpResponse, new TypeReference<>() {}));
    }

    // Avoids long connections and the risk of connection termination by intermediary
    public CompletableFuture<EnvironmentResponse> pollingCreateEnvironment(
            CreateEnvironmentRequest createEnvironmentRequest,
            String token)
    {
        throw new UnsupportedOperationException();
    }

    public EnvironmentResponse getEnvironment(GetEnvironmentRequest getEnvironmentRequest, String token)
    {
        var httpRequest = HttpRequest.newBuilder(URI.create(baseUrl
                        .concat("/environments/")
                        .concat(getEnvironmentRequest.name())))
                .headers(AUTHORIZATION, BEARER + token)
                .GET()
                .build();

        var httpResponse =
                handleCheckedException(() -> httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString()));
        return handleHttpResponse(httpResponse, new TypeReference<>() {});
    }

    // Deleting an environment is a blocking operation by default, and it takes ~1.5min to finish
    public CompletableFuture<Void> deleteEnvironment(DeleteEnvironmentRequest deleteEnvironmentRequest, String token)
    {
        var httpRequest = HttpRequest.newBuilder(URI.create(baseUrl
                        .concat("/environments/")
                        .concat(deleteEnvironmentRequest.name())))
                .headers(AUTHORIZATION, BEARER + token)
                .DELETE()
                .build();

        return httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
                .thenApply(httpResponse -> handleHttpResponse(httpResponse, new TypeReference<>() {}));
    }

    public CompletableFuture<Void> startEnvironment(EnvironmentRequest environmentRequest, String token)
    {
        var requestBody = handleCheckedException(() -> objectMapper.writeValueAsString(environmentRequest.request()));
        return getVoidCompletableFuture(environmentRequest.name(), token, requestBody);
    }

    public CompletableFuture<Void> stopEnvironment(EnvironmentRequest environmentRequest, String token)
    {
        var requestBody = handleCheckedException(() -> objectMapper.writeValueAsString(environmentRequest.request()));
        return getVoidCompletableFuture(environmentRequest.name(), token, requestBody);
    }

    private CompletableFuture<Void> getVoidCompletableFuture(String name, String token, String jsonPayLoadString)
    {
        HttpRequest.BodyPublisher publisher = HttpRequest.BodyPublishers.ofString(jsonPayLoadString);
        var httpRequest = HttpRequest.newBuilder(URI.create(baseUrl
                        .concat("/environments/")
                        .concat(name)))
                .headers(AUTHORIZATION, BEARER + token,
                        CONTENT_TYPE, APPLICATION_JSON)
                .method("PATCH", publisher)
                .build();
        var httpResponse =
                handleCheckedException(() -> httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString()));
        return handleHttpResponse(httpResponse, new TypeReference<>() {});
    }

    private <T> T handleHttpResponse(HttpResponse<String> httpResponse, TypeReference<T> typeReference)
    {
        var body = httpResponse.body();
        if (httpResponse.statusCode() >= 200 && httpResponse.statusCode() <= 299) {
            return handleCheckedException(() -> {
                if (typeReference.getType().getTypeName().equals(Void.class.getTypeName())) {
                    return null;
                }
                else {
                    return objectMapper.readValue(body, typeReference);
                }
            });
        }
        else if (httpResponse.statusCode() >= 400 && httpResponse.statusCode() <= 499) {
            throw new Error4xxException(httpResponse.statusCode(), body);
        }
        else if (httpResponse.statusCode() >= 500 && httpResponse.statusCode() <= 599) {
            throw new Error5xxException(httpResponse.statusCode(), body);
        }
        else {
            throw new BaseException(httpResponse.statusCode(), body);
        }
    }

    private <T> T handleCheckedException(CheckedSupplier<T> checkedSupplier)
    {
        try {
            return checkedSupplier.get();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    private interface CheckedSupplier<T>
    {
        T get()
                throws IOException, InterruptedException;
    }
}
