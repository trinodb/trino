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
package io.trino.plugin.base.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.TheServlet;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.node.testing.TestingNodeModule;

import javax.servlet.Servlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public class TestingHttpServer
        implements Closeable
{
    private final LifeCycleManager lifeCycleManager;
    private final URI baseUri;

    public TestingHttpServer()
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new TestingHttpServerModule(),
                binder -> {
                    binder.bind(new TypeLiteral<Map<String, String>>() {}).annotatedWith(TheServlet.class).toInstance(ImmutableMap.of());
                    binder.bind(Servlet.class).annotatedWith(TheServlet.class).toInstance(new TestingHttpServlet());
                });

        Injector injector = app
                .doNotInitializeLogging()
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        HttpServerInfo httpServerInfo = injector.getInstance(HttpServerInfo.class);
        baseUri = httpServerInfo.getHttpUri();
    }

    @Override
    public void close()
            throws IOException
    {
        lifeCycleManager.stop();
    }

    public URI resource(String path)
    {
        return baseUri.resolve(path);
    }

    private static class TestingHttpServlet
            extends HttpServlet
    {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            byte[] responseAsBytes = Files.readAllBytes(Paths.get(request.getPathInfo()));
            ByteSource.wrap(responseAsBytes).copyTo(response.getOutputStream());
        }
    }
}
