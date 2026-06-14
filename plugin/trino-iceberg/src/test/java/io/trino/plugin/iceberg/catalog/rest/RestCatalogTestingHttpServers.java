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
package io.trino.plugin.iceberg.catalog.rest;

import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.ServerFeature;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.node.NodeInfo;
import org.apache.iceberg.rest.RESTCatalogServlet;

import java.io.IOException;

/**
 * Helpers for building a {@link TestingHttpServer} that hosts a
 * {@link RESTCatalogServlet} (or subclass) on an arbitrary local port.
 * Shared by the Iceberg REST catalog tests to avoid duplicating HTTP
 * server configuration.
 */
public final class RestCatalogTestingHttpServers
{
    private RestCatalogTestingHttpServers() {}

    public static TestingHttpServer create(RESTCatalogServlet servlet)
            throws IOException
    {
        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig config = new HttpServerConfig()
                .setHttpPort(0)
                .setMinThreads(4)
                .setMaxThreads(8)
                .setHttpAcceptorThreads(4)
                .setHttpAcceptQueueSize(10)
                .setHttpEnabled(true);
        HttpServerInfo httpServerInfo = new HttpServerInfo(config, nodeInfo);
        return new TestingHttpServer("rest-catalog", httpServerInfo, nodeInfo, config, servlet, ServerFeature.builder()
                // Required due to URIs like: HEAD /v1/namespaces/level_1%1Flevel_2
                .withLegacyUriCompliance(true)
                .build());
    }
}
