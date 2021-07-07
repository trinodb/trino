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
package io.trino.server.security.oauth2;

import com.github.scribejava.core.httpclient.HttpClient;
import com.github.scribejava.core.httpclient.multipart.MultipartPayload;
import com.github.scribejava.core.model.OAuthAsyncRequestCallback;
import com.github.scribejava.core.model.OAuthRequest.ResponseConverter;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.ByteStreams;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.ResponseHandler;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.http.client.ResponseHandlerUtils.propagate;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.HttpHeaders.USER_AGENT;
import static javax.ws.rs.core.MediaType.APPLICATION_FORM_URLENCODED;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

public class ScribeHttpClient
        implements HttpClient
{
    private static final Multimap<String, String> DEFAULT_HEADERS = ImmutableMultimap.<String, String>builder()
            .put(CONTENT_TYPE, APPLICATION_FORM_URLENCODED)
            .put(ACCEPT, APPLICATION_JSON)
            .build();

    private final io.airlift.http.client.HttpClient httpClient;

    public ScribeHttpClient(io.airlift.http.client.HttpClient httpClient)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    @Override
    public <T> Future<T> executeAsync(String userAgent, Map<String, String> headers, Verb httpVerb, String completeUrl, byte[] bodyContents, OAuthAsyncRequestCallback<T> callback, ResponseConverter<T> converter)
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public <T> Future<T> executeAsync(String userAgent, Map<String, String> headers, Verb httpVerb, String completeUrl, MultipartPayload bodyContents, OAuthAsyncRequestCallback<T> callback, ResponseConverter<T> converter)
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public <T> Future<T> executeAsync(String userAgent, Map<String, String> headers, Verb httpVerb, String completeUrl, String bodyContents, OAuthAsyncRequestCallback<T> callback, ResponseConverter<T> converter)
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public <T> Future<T> executeAsync(String userAgent, Map<String, String> headers, Verb httpVerb, String completeUrl, File bodyContents, OAuthAsyncRequestCallback<T> callback, ResponseConverter<T> converter)
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Response execute(String userAgent, Map<String, String> headers, Verb httpVerb, String completeUrl, byte[] bodyContents)
            throws InterruptedException, ExecutionException, IOException
    {
        return execute(userAgent, headers, httpVerb, completeUrl, createStaticBodyGenerator(bodyContents));
    }

    @Override
    public Response execute(String userAgent, Map<String, String> headers, Verb httpVerb, String completeUrl, MultipartPayload bodyContents)
            throws InterruptedException, ExecutionException, IOException
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Response execute(String userAgent, Map<String, String> headers, Verb httpVerb, String completeUrl, String bodyContents)
            throws InterruptedException, ExecutionException, IOException
    {
        return execute(userAgent, headers, httpVerb, completeUrl, createStaticBodyGenerator(bodyContents, UTF_8));
    }

    @Override
    public Response execute(String userAgent, Map<String, String> headers, Verb httpVerb, String completeUrl, File bodyContents)
            throws InterruptedException, ExecutionException, IOException
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void close()
            throws IOException
    {
        httpClient.close();
    }

    private Response execute(
            String userAgent,
            Map<String, String> headers,
            Verb httpVerb,
            String completeUrl,
            BodyGenerator bodyGenerator)
    {
        ImmutableMultimap.Builder<String, String> requestHeaders = ImmutableMultimap.<String, String>builder()
                .putAll(DEFAULT_HEADERS)
                .putAll(headers.entrySet());
        if (userAgent != null) {
            requestHeaders.put(USER_AGENT, userAgent);
        }
        Request request = Request.builder()
                .setMethod(httpVerb.name())
                .setUri(URI.create(completeUrl))
                .setBodyGenerator(bodyGenerator)
                .addHeaders(requestHeaders.build())
                .build();
        return httpClient.execute(request, new ConvertingResponseHandler());
    }

    private static class ConvertingResponseHandler
            implements ResponseHandler<Response, RuntimeException>
    {
        @Override
        public Response handleException(Request request, Exception exception)
        {
            throw propagate(request, exception);
        }

        @Override
        public Response handle(Request request, io.airlift.http.client.Response response)
        {
            return convertResponse(response);
        }

        private static Response convertResponse(io.airlift.http.client.Response response)
        {
            int code = response.getStatusCode();
            Map<String, String> headers = response.getHeaders().asMap()
                    .entrySet().stream()
                    .collect(toImmutableMap(
                            e -> e.getKey().toString(),
                            e -> String.join(",", e.getValue())));
            try {
                return new Response(
                        code,
                        HttpStatus.fromStatusCode(code).toString(),
                        headers,
                        // assume response is UTF-8, since Scribe Response would do that anyway
                        new String(ByteStreams.toByteArray(response.getInputStream()), UTF_8));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
