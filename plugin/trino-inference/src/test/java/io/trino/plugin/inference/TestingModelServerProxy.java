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

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.node.testing.TestingNodeModule;
import jakarta.servlet.Servlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static java.util.Objects.requireNonNull;

/**
 * Proxy in front of a model server, which acts like an authenticating proxy, a restarting server, a slow server, or a gateway that serves the model server under a path:
 * it can require an HTTP header, fail requests with HTTP status 503, reject large requests with HTTP status 413, delay inference requests,
 * and serve the model server only under a path prefix.
 */
public final class TestingModelServerProxy
        implements Closeable
{
    private final URI target;
    private final String pathPrefix;
    private final HttpClient httpClient = new JettyHttpClient();
    private final LifeCycleManager lifeCycleManager;
    private final URI uri;
    private final AtomicReference<Optional<RequiredHeader>> requiredHeader = new AtomicReference<>(Optional.empty());
    private final AtomicInteger failingRequests = new AtomicInteger();
    private final AtomicInteger maxRequestBodySize = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicReference<Duration> inferenceRequestDelay = new AtomicReference<>(Duration.ZERO);

    public TestingModelServerProxy(URI target)
    {
        this(target, "");
    }

    /**
     * @param pathPrefix the path under which the proxy serves the model server.
     *         The proxy removes the prefix from forwarded requests, and fails requests outside the path with HTTP status 404
     */
    public TestingModelServerProxy(URI target, String pathPrefix)
    {
        this.target = requireNonNull(target, "target is null");
        this.pathPrefix = requireNonNull(pathPrefix, "pathPrefix is null");
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new TestingHttpServerModule("testing-model-server-proxy"),
                binder -> binder.bind(Servlet.class).toInstance(new ProxyServlet()));
        Injector injector = app
                .doNotInitializeLogging()
                .initialize();
        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        uri = URI.create(injector.getInstance(TestingHttpServer.class).getBaseUrl() + pathPrefix);
    }

    public URI getUri()
    {
        return uri;
    }

    public void requireHeader(String name, String value)
    {
        requiredHeader.set(Optional.of(new RequiredHeader(name, value)));
    }

    public void doNotRequireHeader()
    {
        requiredHeader.set(Optional.empty());
    }

    /**
     * Fails the next requests with HTTP status 503.
     */
    public void failNextRequests(int count)
    {
        failingRequests.set(count);
    }

    /**
     * Rejects requests with a body larger than the size with HTTP status 413.
     */
    public void limitRequestBodySize(int bytes)
    {
        maxRequestBodySize.set(bytes);
    }

    /**
     * Delays the inference requests, but not the requests for model metadata.
     */
    public void delayInferenceRequests(Duration delay)
    {
        inferenceRequestDelay.set(requireNonNull(delay, "delay is null"));
    }

    @Override
    public void close()
    {
        httpClient.close();
        lifeCycleManager.stop();
    }

    private record RequiredHeader(String name, String value) {}

    private class ProxyServlet
            extends HttpServlet
    {
        @Override
        protected void service(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            String path = request.getRequestURI();
            if (!path.startsWith(pathPrefix + "/")) {
                response.sendError(404, "Not Found");
                return;
            }
            Optional<RequiredHeader> header = requiredHeader.get();
            if (header.isPresent() && !header.get().value().equals(request.getHeader(header.get().name()))) {
                // like authenticating proxies, include the challenge that HTTP requires for status 401
                response.setHeader("WWW-Authenticate", "Bearer realm=\"model-server\"");
                response.sendError(401, "Unauthorized");
                return;
            }
            if (failingRequests.getAndUpdate(count -> Math.max(count - 1, 0)) > 0) {
                response.sendError(503, "Service Unavailable");
                return;
            }
            byte[] body = request.getInputStream().readAllBytes();
            if (body.length > maxRequestBodySize.get()) {
                response.sendError(413, "Content Too Large");
                return;
            }
            if (path.endsWith("/infer")) {
                try {
                    Thread.sleep(inferenceRequestDelay.get());
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
            }

            Request.Builder forward = new Request.Builder()
                    .setMethod(request.getMethod())
                    .setUri(target.resolve(path.substring(pathPrefix.length())));
            if (body.length > 0) {
                forward.setBodyGenerator(createStaticBodyGenerator(body));
            }
            StringResponse forwarded = httpClient.execute(forward.build(), createStringResponseHandler());
            response.setStatus(forwarded.getStatusCode());
            response.setContentType("application/json");
            response.getWriter().write(forwarded.getBody());
        }
    }
}
