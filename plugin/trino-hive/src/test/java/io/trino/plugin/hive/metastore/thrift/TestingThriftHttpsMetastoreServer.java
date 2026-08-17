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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.node.testing.TestingNodeModule;
import io.trino.hive.thrift.metastore.ThriftHiveMetastore;
import jakarta.servlet.Servlet;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class TestingThriftHttpsMetastoreServer
        implements Closeable
{
    private final LifeCycleManager lifeCycleManager;
    private final URI httpsUri;

    public TestingThriftHttpsMetastoreServer(
            TestingThriftHttpMetastoreServer.TestingThriftRequestsHandler handler,
            Consumer<jakarta.servlet.http.HttpServletRequest> requestInterceptor)
    {
        ThriftHiveMetastore.Iface mockThriftHandler = TestingThriftHttpMetastoreServer.proxyHandler(handler);
        var processor = new ThriftHiveMetastore.Processor<>(mockThriftHandler);
        Servlet servlet = new TestingThriftHttpMetastoreServer.TestingThriftHttpServlet(
                processor,
                new org.apache.thrift.protocol.TBinaryProtocol.Factory(),
                requestInterceptor);

        String keystorePath = requireNonNull(
                Resources.getResource("thrift-http-metastore-https/server.pem"),
                "Missing test keystore resource").getPath();

        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new TestingHttpServerModule("testing", 0),
                binder -> binder.bind(Servlet.class).toInstance(servlet))
                .setRequiredConfigurationProperties(ImmutableMap.<String, String>builder()
                        .put("http-server.http.enabled", "false")
                        .put("http-server.https.enabled", "true")
                        .put("http-server.https.keystore.path", keystorePath)
                        .put("http-server.https.keystore.key", "")
                        .buildOrThrow());

        Injector injector = app
                .doNotInitializeLogging()
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        HttpServerInfo httpServerInfo = injector.getInstance(HttpServerInfo.class);
        httpsUri = httpServerInfo.getHttpsUri();
    }

    public URI getHttpsUri()
    {
        return httpsUri;
    }

    @Override
    public void close()
            throws IOException
    {
        lifeCycleManager.stop();
    }
}
