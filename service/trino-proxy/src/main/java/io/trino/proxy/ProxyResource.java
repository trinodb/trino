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
package io.trino.proxy;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import jakarta.annotation.PreDestroy;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.ResponseBuilder;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.core.JsonToken.END_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.FIELD_NAME;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_STRING;
import static com.google.common.hash.Hashing.hmacSha256;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.COOKIE;
import static com.google.common.net.HttpHeaders.SET_COOKIE;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.jaxrs.AsyncResponseHandler.bindAsyncResponse;
import static io.trino.plugin.base.util.JsonUtils.jsonFactoryBuilder;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static jakarta.ws.rs.core.Response.Status.BAD_GATEWAY;
import static jakarta.ws.rs.core.Response.Status.FORBIDDEN;
import static jakarta.ws.rs.core.Response.Status.NO_CONTENT;
import static jakarta.ws.rs.core.Response.Status.OK;
import static jakarta.ws.rs.core.Response.noContent;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.readAllBytes;
import static java.util.Collections.list;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;

@Path("/")
public class ProxyResource
{
    private static final Logger log = Logger.get(ProxyResource.class);

    private static final String X509_ATTRIBUTE = "jakarta.servlet.request.X509Certificate";
    private static final Duration ASYNC_TIMEOUT = new Duration(2, MINUTES);
    private static final JsonFactory JSON_FACTORY = jsonFactoryBuilder().disable(CANONICALIZE_FIELD_NAMES).build();

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("proxy-%s"));
    private final OkHttpClient httpClient;
    private final JsonWebTokenHandler jwtHandler;
    private final URI remoteUri;
    private final HashFunction hmac;

    private static final com.google.common.net.MediaType JSON = com.google.common.net.MediaType.create("application", "json");

    @Inject
    public ProxyResource(@ForProxy OkHttpClient httpClient, JsonWebTokenHandler jwtHandler, ProxyConfig config)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.jwtHandler = requireNonNull(jwtHandler, "jwtHandler is null");
        this.remoteUri = requireNonNull(config.getUri(), "uri is null");
        this.hmac = hmacSha256(loadSharedSecret(config.getSharedSecretFile()));
    }

    @PreDestroy
    public void shutdown()
    {
        executor.shutdownNow();
    }

    @GET
    @Path("/v1/info")
    @Produces(APPLICATION_JSON)
    public void getInfo(
            @Context HttpServletRequest servletRequest,
            @Suspended AsyncResponse asyncResponse)
    {
        Request.Builder request = new Request.Builder()
                .get()
                .url(UriBuilder.fromUri(remoteUri).replacePath("/v1/info").build().toString());

        performRequest(servletRequest, asyncResponse, request, response ->
                responseWithHeaders(Response.ok(response.getBody()), response));
    }

    @POST
    @Path("/v1/statement")
    @Produces(APPLICATION_JSON)
    public void postStatement(
            String statement,
            @Context HttpServletRequest servletRequest,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        Request.Builder request = new Request.Builder()
                .post(RequestBody.create(statement, MediaType.parse("application/json")))
                .url(UriBuilder.fromUri(remoteUri).replacePath("/v1/statement").build().toString());

        performRequest(servletRequest, asyncResponse, request, response -> buildResponse(uriInfo, response));
    }

    @GET
    @Path("/v1/proxy")
    @Produces(APPLICATION_JSON)
    public void getNext(
            @QueryParam("uri") String uri,
            @QueryParam("hmac") String hash,
            @Context HttpServletRequest servletRequest,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        if (!hmac.hashString(uri, UTF_8).equals(HashCode.fromString(hash))) {
            throw badRequest(FORBIDDEN, "Failed to validate HMAC of URI");
        }

        Request.Builder request = new Request.Builder()
                .get()
                .url(uri);

        performRequest(servletRequest, asyncResponse, request, response -> buildResponse(uriInfo, response));
    }

    @DELETE
    @Path("/v1/proxy")
    @Produces(APPLICATION_JSON)
    public void cancelQuery(
            @QueryParam("uri") String uri,
            @QueryParam("hmac") String hash,
            @Context HttpServletRequest servletRequest,
            @Suspended AsyncResponse asyncResponse)
    {
        if (!hmac.hashString(uri, UTF_8).equals(HashCode.fromString(hash))) {
            throw badRequest(FORBIDDEN, "Failed to validate HMAC of URI");
        }

        Request.Builder request = new Request.Builder()
                .delete()
                .url(uri);

        performRequest(servletRequest, asyncResponse, request, response -> responseWithHeaders(noContent(), response));
    }

    private void performRequest(
            HttpServletRequest servletRequest,
            AsyncResponse asyncResponse,
            Request.Builder requestBuilder,
            Function<ProxyResponse, Response> responseBuilder)
    {
        setupBearerToken(servletRequest, requestBuilder);

        for (String name : list(servletRequest.getHeaderNames())) {
            if (isTrinoHeader(name) || name.equalsIgnoreCase(COOKIE)) {
                for (String value : list(servletRequest.getHeaders(name))) {
                    requestBuilder.addHeader(name, value);
                }
            }
            else if (name.equalsIgnoreCase(USER_AGENT)) {
                for (String value : list(servletRequest.getHeaders(name))) {
                    requestBuilder.addHeader(name, "[Trino Proxy] " + value);
                }
            }
        }

        Request request = requestBuilder.build();
        ListenableFuture<Response> future = executeHttp(request)
                .transform(responseBuilder::apply, executor)
                .catching(ProxyException.class, e -> handleProxyException(request, e), directExecutor());

        setupAsyncResponse(asyncResponse, future);
    }

    private Response buildResponse(UriInfo uriInfo, ProxyResponse response)
    {
        byte[] body = rewriteResponse(response.getBody(), uri -> rewriteUri(uriInfo, uri));
        return responseWithHeaders(Response.ok(body), response);
    }

    private String rewriteUri(UriInfo uriInfo, String uri)
    {
        return uriInfo.getAbsolutePathBuilder()
                .replacePath("/v1/proxy")
                .queryParam("uri", uri)
                .queryParam("hmac", hmac.hashString(uri, UTF_8))
                .build()
                .toString();
    }

    private void setupAsyncResponse(AsyncResponse asyncResponse, ListenableFuture<Response> future)
    {
        bindAsyncResponse(asyncResponse, future, executor)
                .withTimeout(ASYNC_TIMEOUT, () -> Response
                        .status(BAD_GATEWAY)
                        .type(TEXT_PLAIN_TYPE)
                        .entity("Request to remote Trino server timed out after" + ASYNC_TIMEOUT)
                        .build());
    }

    private FluentFuture<ProxyResponse> executeHttp(Request request)
    {
        SettableFuture<ProxyResponse> future = SettableFuture.create();

        // Enqueue the call and resolve the future
        httpClient.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e)
            {
                future.setException(e); // Set the exception if the request fails
            }

            @Override
            public void onResponse(Call call, okhttp3.Response response)
            {
                if (response.code() == NO_CONTENT.getStatusCode()) {
                    future.set(new ProxyResponse(response.headers(), new byte[0]));
                    return;
                }

                if (response.code() != OK.getStatusCode()) {
                    try (ResponseBody body = response.body()) {
                        future.setException(new ProxyException(format("Bad status code from remote Trino server: %s: %s", response.code(), body.string())));
                        return;
                    }
                    catch (IOException e) {
                        future.setException(e);
                        return;
                    }
                }

                String contentType = response.header(CONTENT_TYPE);
                if (contentType == null) {
                    throw new ProxyException("No Content-Type set in response from remote Trino server");
                }
                if (!com.google.common.net.MediaType.parse(contentType).is(JSON)) {
                    throw new ProxyException("Bad Content-Type from remote Trino server:" + contentType);
                }

                try (ResponseBody body = response.body()) {
                    future.set(new ProxyResponse(response.headers(), body.bytes()));
                    return;
                }
                catch (IOException e) {
                    throw new ProxyException("Failed reading response from remote Trino server", e);
                }
            }
        });

        return FluentFuture.from(future);
    }

    private void setupBearerToken(HttpServletRequest servletRequest, Request.Builder requestBuilder)
    {
        if (!jwtHandler.isConfigured()) {
            return;
        }

        X509Certificate[] certs = (X509Certificate[]) servletRequest.getAttribute(X509_ATTRIBUTE);
        if ((certs == null) || (certs.length == 0)) {
            throw badRequest(FORBIDDEN, "No TLS certificate present for request");
        }
        String principal = certs[0].getSubjectX500Principal().getName();

        String accessToken = jwtHandler.getBearerToken(principal);
        requestBuilder.addHeader(AUTHORIZATION, "Bearer " + accessToken);
    }

    private static <T> T handleProxyException(Request request, ProxyException e)
    {
        log.warn(e, "Proxy request failed: %s %s", request.method(), request.url());
        throw badRequest(BAD_GATEWAY, e.getMessage());
    }

    private static WebApplicationException badRequest(Status status, String message)
    {
        throw new WebApplicationException(
                Response.status(status)
                        .type(TEXT_PLAIN_TYPE)
                        .entity(message)
                        .build());
    }

    private static boolean isTrinoHeader(String name)
    {
        return name.toLowerCase(ENGLISH).startsWith("x-trino-");
    }

    private static Response responseWithHeaders(ResponseBuilder builder, ProxyResponse response)
    {
        response.getHeaders().names().forEach(name -> {
            if (isTrinoHeader(name) || name.equalsIgnoreCase(SET_COOKIE)) {
                builder.header(name, response.getHeaders().get(name));
            }
        });
        return builder.build();
    }

    private static byte[] rewriteResponse(byte[] input, Function<String, String> uriRewriter)
    {
        try {
            JsonParser parser = JSON_FACTORY.createParser(input);
            ByteArrayOutputStream out = new ByteArrayOutputStream(input.length * 2);
            JsonGenerator generator = JSON_FACTORY.createGenerator(out);

            JsonToken token = parser.nextToken();
            if (token != START_OBJECT) {
                throw invalidJson("bad start token: " + token);
            }
            generator.copyCurrentEvent(parser);

            while (true) {
                token = parser.nextToken();
                if (token == null) {
                    throw invalidJson("unexpected end of stream");
                }

                if (token == END_OBJECT) {
                    generator.copyCurrentEvent(parser);
                    break;
                }

                if (token == FIELD_NAME) {
                    String name = parser.getValueAsString();
                    if (!"nextUri".equals(name) && !"partialCancelUri".equals(name)) {
                        generator.copyCurrentStructure(parser);
                        continue;
                    }

                    token = parser.nextToken();
                    if (token != VALUE_STRING) {
                        throw invalidJson(format("bad %s token: %s", name, token));
                    }
                    String value = parser.getValueAsString();

                    value = uriRewriter.apply(value);
                    generator.writeStringField(name, value);
                    continue;
                }

                throw invalidJson("unexpected token: " + token);
            }

            token = parser.nextToken();
            if (token != null) {
                throw invalidJson("unexpected token after object close: " + token);
            }

            generator.close();
            return out.toByteArray();
        }
        catch (IOException e) {
            throw new ProxyException(e);
        }
    }

    private static IOException invalidJson(String message)
    {
        return new IOException("Invalid JSON response from remote Trino server: " + message);
    }

    private static byte[] loadSharedSecret(File file)
    {
        try {
            return Base64.getMimeDecoder().decode(readAllBytes(file.toPath()));
        }
        catch (IOException | IllegalArgumentException e) {
            throw new RuntimeException("Failed to load shared secret file: " + file, e);
        }
    }

    public static class ProxyResponse
    {
        private final Headers headers;
        private final byte[] body;

        ProxyResponse(Headers headers, byte[] body)
        {
            this.headers = requireNonNull(headers, "headers is null");
            this.body = requireNonNull(body, "body is null");
        }

        public Headers getHeaders()
        {
            return headers;
        }

        public byte[] getBody()
        {
            return body;
        }
    }
}
