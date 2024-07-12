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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.server.DriftServer;
import io.airlift.drift.server.DriftService;
import io.airlift.drift.server.stats.NullMethodInvocationStatsFactory;
import io.airlift.drift.transport.netty.server.DriftNettyServerConfig;
import io.airlift.drift.transport.netty.server.DriftNettyServerTransport;
import io.airlift.drift.transport.netty.server.DriftNettyServerTransportFactory;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.thrift.ThriftPlugin;
import io.trino.plugin.thrift.server.ThriftIndexedTpchService;
import io.trino.plugin.thrift.server.ThriftTpchService;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.stream.Collectors.joining;

public final class ThriftQueryRunner
{
    private static final ThriftCodecManager CODEC_MANAGER = new ThriftCodecManager();

    private ThriftQueryRunner() {}

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        QueryRunner queryRunner = builder(startThriftServers(3, true))
                .addCoordinatorProperty("http-server.http.port", "8080")
                .build();
        Logger log = Logger.get(ThriftQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }

    static StartedServers startThriftServers(int thriftServers, boolean enableIndexJoin)
    {
        List<DriftServer> servers = new ArrayList<>(thriftServers);
        List<AutoCloseable> resources = new ArrayList<>(thriftServers);
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
            resources.add(service);
        }
        return new StartedServers(servers, resources);
    }

    public static Builder builder(StartedServers startedServers)
    {
        return new Builder()
                .addConnectorProperty("trino.thrift.client.addresses", startedServers.servers.stream()
                        .map(server -> "localhost:" + driftServerPort(server))
                        .collect(joining(",")))
                .addConnectorProperty("trino.thrift.client.connect-timeout", "30s")
                .addConnectorProperty("trino-thrift.lookup-requests-concurrency", "2");
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();

        private Builder()
        {
            super(testSessionBuilder()
                    .setCatalog("thrift")
                    .setSchema("tiny")
                    .build());
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperty(String key, String value)
        {
            this.connectorProperties.put(key, value);
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new ThriftPlugin());
                queryRunner.createCatalog("thrift", "trino_thrift", connectorProperties);

                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    static int driftServerPort(DriftServer server)
    {
        return ((DriftNettyServerTransport) server.getServerTransport()).getPort();
    }

    public record StartedServers(List<DriftServer> servers, List<AutoCloseable> resources)
    {
        public StartedServers
        {
            servers = ImmutableList.copyOf(servers);
            resources = ImmutableList.copyOf(resources);
        }
    }
}
