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
package io.trino.plugin.example;

import com.google.common.io.Resources;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.node.testing.TestingNodeModule;
import jakarta.servlet.Servlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.net.URI;
import java.net.URL;

public class ExampleHttpServer
{
    private final LifeCycleManager lifeCycleManager;
    private final URI baseUri;

    public ExampleHttpServer()
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new TestingHttpServerModule(),
                new ExampleHttpServerModule());

        Injector injector = app
                .doNotInitializeLogging()
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        baseUri = injector.getInstance(TestingHttpServer.class).getBaseUrl();
    }

    public void stop()
    {
        lifeCycleManager.stop();
    }

    public URI resolve(String s)
    {
        return baseUri.resolve(s);
    }

    private static class ExampleHttpServerModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(Servlet.class).toInstance(new ExampleHttpServlet());
        }
    }

    private static class ExampleHttpServlet
            extends HttpServlet
    {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            URL dataUrl = Resources.getResource(TestExampleClient.class, request.getPathInfo());
            Resources.asByteSource(dataUrl).copyTo(response.getOutputStream());
        }
    }
}
