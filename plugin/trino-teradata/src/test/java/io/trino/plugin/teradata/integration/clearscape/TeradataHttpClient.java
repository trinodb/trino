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
package io.trino.plugin.teradata.integration.clearscape;

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

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static java.util.Objects.requireNonNull;

public class TeradataHttpClient
{
    private static final String APPLICATION_JSON = "application/json";
    private static final String BEARER = "Bearer ";

    private final String baseUrl;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public TeradataHttpClient(String baseUrl)
    {
        requireNonNull(baseUrl, "baseUrl should not be null");
        this.baseUrl = baseUrl;
        httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
        objectMapper = JsonMapper.builder()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS, false)
                .build();
    }

    public CompletableFuture<EnvironmentResponse> createEnvironment(CreateEnvironmentRequest createEnvironmentRequest, String token)
    {
        String requestBody = handleCheckedException(() -> objectMapper.writeValueAsString(createEnvironmentRequest));
        HttpRequest httpRequest = HttpRequest.newBuilder(URI.create(baseUrl.concat("/environments")))
                .headers(
                        AUTHORIZATION, BEARER + token,
                        CONTENT_TYPE, APPLICATION_JSON)
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();
        return httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
                .thenApply(httpResponse -> handleHttpResponse(httpResponse, new TypeReference<>() {}));
    }

    public EnvironmentResponse fetchEnvironment(GetEnvironmentRequest getEnvironmentRequest, String token)
    {
        HttpRequest httpRequest = HttpRequest.newBuilder(URI.create(baseUrl
                        .concat("/environments/")
                        .concat(getEnvironmentRequest.name())))
                .headers(AUTHORIZATION, BEARER + token)
                .GET()
                .build();
        HttpResponse<String> httpResponse = handleCheckedException(() -> httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString()));
        return handleHttpResponse(httpResponse, new TypeReference<>() {});
    }

    public CompletableFuture<Void> deleteEnvironment(DeleteEnvironmentRequest deleteEnvironmentRequest, String token)
    {
        HttpRequest httpRequest = HttpRequest.newBuilder(URI.create(baseUrl + "/environments/" + deleteEnvironmentRequest.name()))
                .headers(AUTHORIZATION, BEARER + token)
                .DELETE()
                .build();

        httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString());
        return CompletableFuture.completedFuture(null);
    }

    public void startEnvironment(EnvironmentRequest environmentRequest, String token)
    {
        String requestBody = handleCheckedException(() -> objectMapper.writeValueAsString(environmentRequest.request()));
        getVoidCompletableFuture(environmentRequest.name(), token, requestBody);
    }

    public void stopEnvironment(EnvironmentRequest environmentRequest, String token)
    {
        String requestBody = handleCheckedException(() -> objectMapper.writeValueAsString(environmentRequest.request()));
        getVoidCompletableFuture(environmentRequest.name(), token, requestBody);
    }

    private void getVoidCompletableFuture(String name, String token, String jsonPayLoadString)
    {
        HttpRequest.BodyPublisher publisher = HttpRequest.BodyPublishers.ofString(jsonPayLoadString);
        HttpRequest httpRequest = HttpRequest.newBuilder(URI.create(baseUrl + "/environments/" + name))
                .headers(AUTHORIZATION, BEARER + token, CONTENT_TYPE, APPLICATION_JSON)
                .method("PATCH", publisher)
                .build();

        httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString());
    }

    private <T> T handleHttpResponse(HttpResponse<String> httpResponse, TypeReference<T> typeReference)
    {
        String body = httpResponse.body();
        if (httpResponse.statusCode() >= 200 && httpResponse.statusCode() <= 299) {
            return handleCheckedException(() -> {
                if (typeReference.getType().getTypeName().equals(Void.class.getTypeName())) {
                    return null;
                }
                return objectMapper.readValue(body, typeReference);
            });
        }
        throw new ClearScapeServiceException(httpResponse.statusCode(), body);
    }

    private static <T> T handleCheckedException(CheckedSupplier<T> checkedSupplier)
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
