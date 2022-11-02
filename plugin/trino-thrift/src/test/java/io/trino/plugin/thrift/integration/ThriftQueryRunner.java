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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
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
import io.trino.cost.StatsCalculator;
import io.trino.execution.FailureInjector.InjectedFailureType;
import io.trino.metadata.FunctionBundle;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.SessionPropertyManager;
import io.trino.plugin.thrift.ThriftPlugin;
import io.trino.plugin.thrift.server.ThriftIndexedTpchService;
import io.trino.plugin.thrift.server.ThriftTpchService;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.ErrorType;
import io.trino.spi.Plugin;
import io.trino.spi.type.TypeManager;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.sql.analyzer.QueryExplainer;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingAccessControlManager;
import io.trino.testing.TestingGroupProvider;
import io.trino.transaction.TransactionManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class ThriftQueryRunner
{
    public static final ThriftCodecManager CODEC_MANAGER = new ThriftCodecManager();

    private ThriftQueryRunner() {}

    public static QueryRunner createThriftQueryRunner(int thriftServers, boolean enableIndexJoin, Map<String, String> properties)
            throws Exception
    {
        List<DriftServer> servers = null;
        DistributedQueryRunner runner = null;
        try {
            servers = startThriftServers(thriftServers, enableIndexJoin);
            runner = createThriftQueryRunnerInternal(servers, properties);
            return new ThriftQueryRunnerWithServers(runner, servers);
        }
        catch (Throwable t) {
            closeAllSuppress(t, runner);
            // runner might be null, so closing servers explicitly
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
        Map<String, String> properties = ImmutableMap.of("http-server.http.port", "8080");
        ThriftQueryRunnerWithServers queryRunner = (ThriftQueryRunnerWithServers) createThriftQueryRunner(3, true, properties);
        Thread.sleep(10);
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

    private static DistributedQueryRunner createThriftQueryRunnerInternal(List<DriftServer> servers, Map<String, String> properties)
            throws Exception
    {
        String addresses = servers.stream()
                .map(server -> "localhost:" + driftServerPort(server))
                .collect(joining(","));

        Session defaultSession = testSessionBuilder()
                .setCatalog("thrift")
                .setSchema("tiny")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .setExtraProperties(properties)
                .build();

        queryRunner.installPlugin(new ThriftPlugin());
        Map<String, String> connectorProperties = ImmutableMap.<String, String>builder()
                .put("trino.thrift.client.addresses", addresses)
                .put("trino.thrift.client.connect-timeout", "30s")
                .put("trino-thrift.lookup-requests-concurrency", "2")
                .buildOrThrow();
        queryRunner.createCatalog("thrift", "trino-thrift", connectorProperties);

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        return queryRunner;
    }

    static int driftServerPort(DriftServer server)
    {
        return ((DriftNettyServerTransport) server.getServerTransport()).getPort();
    }

    /**
     * Wraps QueryRunner and a list of ThriftServers to clean them up together.
     */
    private static class ThriftQueryRunnerWithServers
            implements QueryRunner
    {
        private DistributedQueryRunner source;
        private List<DriftServer> thriftServers;

        private ThriftQueryRunnerWithServers(DistributedQueryRunner source, List<DriftServer> thriftServers)
        {
            this.source = requireNonNull(source, "source is null");
            this.thriftServers = ImmutableList.copyOf(requireNonNull(thriftServers, "thriftServers is null"));
        }

        public TestingTrinoServer getCoordinator()
        {
            return source.getCoordinator();
        }

        @Override
        public void close()
        {
            try (Closer closer = Closer.create()) {
                if (thriftServers != null) {
                    for (DriftServer server : thriftServers) {
                        closer.register(server::shutdown);
                    }
                    thriftServers = null;
                }
                if (source != null) {
                    closer.register(source);
                    source = null;
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int getNodeCount()
        {
            return source.getNodeCount();
        }

        @Override
        public Session getDefaultSession()
        {
            return source.getDefaultSession();
        }

        @Override
        public TransactionManager getTransactionManager()
        {
            return source.getTransactionManager();
        }

        @Override
        public Metadata getMetadata()
        {
            return source.getMetadata();
        }

        @Override
        public TypeManager getTypeManager()
        {
            return source.getTypeManager();
        }

        @Override
        public QueryExplainer getQueryExplainer()
        {
            return source.getQueryExplainer();
        }

        @Override
        public SessionPropertyManager getSessionPropertyManager()
        {
            return source.getSessionPropertyManager();
        }

        @Override
        public FunctionManager getFunctionManager()
        {
            return source.getFunctionManager();
        }

        @Override
        public SplitManager getSplitManager()
        {
            return source.getSplitManager();
        }

        @Override
        public PageSourceManager getPageSourceManager()
        {
            return source.getPageSourceManager();
        }

        @Override
        public NodePartitioningManager getNodePartitioningManager()
        {
            return source.getNodePartitioningManager();
        }

        @Override
        public StatsCalculator getStatsCalculator()
        {
            return source.getStatsCalculator();
        }

        @Override
        public TestingGroupProvider getGroupProvider()
        {
            return source.getGroupProvider();
        }

        @Override
        public TestingAccessControlManager getAccessControl()
        {
            return source.getAccessControl();
        }

        @Override
        public MaterializedResult execute(String sql)
        {
            return source.execute(sql);
        }

        @Override
        public MaterializedResult execute(Session session, String sql)
        {
            return source.execute(session, sql);
        }

        @Override
        public List<QualifiedObjectName> listTables(Session session, String catalog, String schema)
        {
            return source.listTables(session, catalog, schema);
        }

        @Override
        public boolean tableExists(Session session, String table)
        {
            return source.tableExists(session, table);
        }

        @Override
        public void installPlugin(Plugin plugin)
        {
            source.installPlugin(plugin);
        }

        @Override
        public void addFunctions(FunctionBundle functionBundle)
        {
            source.addFunctions(functionBundle);
        }

        @Override
        public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
        {
            source.createCatalog(catalogName, connectorName, properties);
        }

        @Override
        public Lock getExclusiveLock()
        {
            return source.getExclusiveLock();
        }

        @Override
        public void injectTaskFailure(
                String traceToken,
                int stageId,
                int partitionId,
                int attemptId,
                InjectedFailureType injectionType,
                Optional<ErrorType> errorType)
        {
            source.injectTaskFailure(traceToken, stageId, partitionId, attemptId, injectionType, errorType);
        }

        @Override
        public void loadExchangeManager(String name, Map<String, String> properties)
        {
            source.loadExchangeManager(name, properties);
        }
    }
}
