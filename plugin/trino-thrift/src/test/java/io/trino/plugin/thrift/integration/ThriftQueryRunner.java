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
package io.trino.plugin.thrift.integration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.server.DriftServer;
import io.airlift.drift.server.DriftService;
import io.airlift.drift.server.stats.NullMethodInvocationStatsFactory;
import io.airlift.drift.transport.netty.server.DriftNettyServerConfig;
import io.airlift.drift.transport.netty.server.DriftNettyServerTransport;
import io.airlift.drift.transport.netty.server.DriftNettyServerTransportFactory;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.thrift.ThriftPlugin;
import io.trino.plugin.thrift.server.ThriftIndexedTpchService;
import io.trino.plugin.thrift.server.ThriftTpchService;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.stream.Collectors.joining;

public final class ThriftQueryRunner
{
    private static final ThriftCodecManager CODEC_MANAGER = new ThriftCodecManager();

    private ThriftQueryRunner() {}

    // TODO convert to builder
    public static QueryRunner createThriftQueryRunner(int thriftServers, boolean enableIndexJoin)
            throws Exception
    {
        return createThriftQueryRunner(thriftServers, enableIndexJoin, Map.of());
    }

    // TODO convert to builder
    private static QueryRunner createThriftQueryRunner(int thriftServers, boolean enableIndexJoin, Map<String, String> coordinatorProperties)
            throws Exception
    {
        List<DriftServer> servers = null;
        try {
            servers = startThriftServers(thriftServers, enableIndexJoin);
            return createThriftQueryRunnerInternal(servers, coordinatorProperties);
        }
        catch (Throwable t) {
            if (servers != null) {
                for (DriftServer server : servers) {
                    server.shutdown();
                }
            }
            throw t;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        Map<String, String> coordinatorProperties = ImmutableMap.of("http-server.http.port", "8080");
        QueryRunner queryRunner = createThriftQueryRunner(3, true, coordinatorProperties);
        Logger log = Logger.get(ThriftQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }

    static List<DriftServer> startThriftServers(int thriftServers, boolean enableIndexJoin)
    {
        List<DriftServer> servers = new ArrayList<>(thriftServers);
        for (int i = 0; i < thriftServers; i++) {
            ThriftTpchService service = enableIndexJoin ? new ThriftIndexedTpchService() : new ThriftTpchService();
            DriftServer server = new DriftServer(
                    new DriftNettyServerTransportFactory(new DriftNettyServerConfig()),
                    CODEC_MANAGER,
                    new NullMethodInvocationStatsFactory(),
                    ImmutableSet.of(new DriftService(service)),
                    ImmutableSet.of());
            server.start();
            servers.add(server);
        }
        return servers;
    }

    private static QueryRunner createThriftQueryRunnerInternal(List<DriftServer> servers, Map<String, String> coordinatorProperties)
            throws Exception
    {
        String addresses = servers.stream()
                .map(server -> "localhost:" + driftServerPort(server))
                .collect(joining(","));

        Session defaultSession = testSessionBuilder()
                .setCatalog("thrift")
                .setSchema("tiny")
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .setCoordinatorProperties(coordinatorProperties)
                .registerResources(servers.stream().map(server -> (AutoCloseable) server::shutdown).toList())
                .build();

        queryRunner.installPlugin(new ThriftPlugin());
        Map<String, String> connectorProperties = ImmutableMap.<String, String>builder()
                .put("trino.thrift.client.addresses", addresses)
                .put("trino.thrift.client.connect-timeout", "30s")
                .put("trino-thrift.lookup-requests-concurrency", "2")
                .buildOrThrow();
        queryRunner.createCatalog("thrift", "trino_thrift", connectorProperties);

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        return queryRunner;
    }

    static int driftServerPort(DriftServer server)
    {
        return ((DriftNettyServerTransport) server.getServerTransport()).getPort();
    }
}
