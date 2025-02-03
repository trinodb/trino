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

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.node.testing.TestingNodeModule;
import io.trino.hive.thrift.metastore.Database;
import io.trino.hive.thrift.metastore.NoSuchObjectException;
import io.trino.hive.thrift.metastore.ThriftHiveMetastore;
import jakarta.servlet.Servlet;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServlet;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.function.Consumer;

import static com.google.common.reflect.Reflection.newProxy;

public class TestingThriftHttpMetastoreServer
        implements Closeable
{
    private final TestingThriftHttpServlet thriftHttpServlet;
    private final LifeCycleManager lifeCycleManager;
    private final URI baseUri;

    public TestingThriftHttpMetastoreServer(TestingThriftRequestsHandler handler, Consumer<HttpServletRequest> requestInterceptor)
    {
        ThriftHiveMetastore.Iface mockThriftHandler = proxyHandler(handler);
        TProcessor processor = new ThriftHiveMetastore.Processor<>(mockThriftHandler);
        thriftHttpServlet = new TestingThriftHttpServlet(processor, new TBinaryProtocol.Factory(), requestInterceptor);
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new TestingHttpServerModule(),
                binder -> {
                    binder.bind(Servlet.class).toInstance(thriftHttpServlet);
                });

        Injector injector = app
                .doNotInitializeLogging()
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        HttpServerInfo httpServerInfo = injector.getInstance(HttpServerInfo.class);
        baseUri = httpServerInfo.getHttpUri();
    }

    private static ThriftHiveMetastore.Iface proxyHandler(TestingThriftRequestsHandler handler)
    {
        return newProxy(ThriftHiveMetastore.Iface.class, (_, method, args) -> switch (method.getName()) {
            case "getAllDatabases" -> handler.getAllDatabases();
            case "getDatabase" -> handler.getDatabase(args[0].toString());
            case "getTables" -> handler.getTables(args[0].toString(), args[1].toString());
            case "getTablesByType" -> handler.getTablesByType(args[0].toString(), args[1].toString(), args[2].toString());
            default -> throw new UnsupportedOperationException();
        });
    }

    public int getPort()
    {
        return baseUri.getPort();
    }

    @Override
    public void close()
            throws IOException
    {
        lifeCycleManager.stop();
    }

    private static class TestingThriftHttpServlet
            extends TServlet
    {
        private final Consumer<HttpServletRequest> requestInterceptor;

        public TestingThriftHttpServlet(
                TProcessor processor,
                TProtocolFactory protocolFactory,
                Consumer<HttpServletRequest> requestInterceptor)
        {
            super(processor, protocolFactory);
            this.requestInterceptor = requestInterceptor;
        }

        @Override
        protected void doPost(HttpServletRequest request,
                HttpServletResponse response)
                throws ServletException, IOException
        {
            requestInterceptor.accept(request);
            super.doPost(request, response);
        }
    }

    public interface TestingThriftRequestsHandler
    {
        List<String> getAllDatabases();

        Database getDatabase(String name)
                throws NoSuchObjectException;

        List<String> getTables(String databaseName, String pattern);

        List<String> getTablesByType(String databaseName, String pattern, String tableType);
    }
}
