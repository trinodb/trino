/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import com.google.common.collect.ImmutableMap;
import com.starburstdata.presto.plugin.hive.StarburstHivePlugin;
import com.starburstdata.presto.plugin.postgresql.StarburstPostgreSqlPlugin;
import com.starburstdata.presto.server.StarburstEngineQueryRunner;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.postgresql.TestingPostgreSqlServer;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.SystemAccessControl;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public final class StargateQueryRunner
{
    private StargateQueryRunner() {}

    private static DistributedQueryRunner createRemoteStarburstQueryRunner(Optional<SystemAccessControl> systemAccessControl)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            Session session = testSessionBuilder()
                    // Require explicit table qualification or custom session.
                    .setCatalog("unspecified_catalog")
                    .setSchema("unspecified_schema")
                    .build();
            DistributedQueryRunner.Builder<?> queryRunnerBuilder = StarburstEngineQueryRunner.builder(session)
                    .setNodeCount(1); // 1 is perfectly enough until we do parallel Stargate connector

            systemAccessControl.ifPresent(queryRunnerBuilder::setSystemAccessControl);

            queryRunner = queryRunnerBuilder.build();
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            return queryRunner;
        }
        catch (Exception e) {
            throw closeAllSuppress(e, queryRunner);
        }
    }

    private static void addMemoryToRemoteStarburstQueryRunner(
            DistributedQueryRunner queryRunner,
            Iterable<TpchTable<?>> requiredTablesInMemoryConnector)
            throws Exception
    {
        try {
            queryRunner.installPlugin(new TestingMemoryPlugin());
            queryRunner.createCatalog("memory", "memory");

            queryRunner.execute("CREATE SCHEMA memory.tiny");
            Session tpchSetupSession = testSessionBuilder()
                    .setCatalog("memory")
                    .setSchema("tiny")
                    .build();
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, tpchSetupSession, requiredTablesInMemoryConnector);
        }
        catch (Exception e) {
            throw closeAllSuppress(e, queryRunner);
        }
    }

    private static void addHiveToRemoteStarburstQueryRunner(
            DistributedQueryRunner queryRunner,
            Path hiveCatalog,
            Iterable<TpchTable<?>> requiredTablesInHiveConnector)
            throws Exception
    {
        try {
            queryRunner.installPlugin(new StarburstHivePlugin());
            queryRunner.createCatalog("hive", "hive", ImmutableMap.of(
                    "hive.metastore", "file",
                    "hive.metastore.catalog.dir", hiveCatalog.toUri().toString(),
                    "hive.security", "allow-all"));

            queryRunner.execute("CREATE SCHEMA hive.tiny");
            Session tpchSetupSession = testSessionBuilder()
                    .setCatalog("hive")
                    .setSchema("tiny")
                    .build();
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, tpchSetupSession, requiredTablesInHiveConnector);
        }
        catch (Exception e) {
            throw closeAllSuppress(e, queryRunner);
        }
    }

    private static void addPostgreSqlToRemoteStarburstQueryRunner(
            DistributedQueryRunner queryRunner,
            TestingPostgreSqlServer server,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> requiredTablesInPostgreSqlConnector)
            throws Exception
    {
        try {
            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", server.getJdbcUrl());
            connectorProperties.putIfAbsent("connection-user", server.getUser());
            connectorProperties.putIfAbsent("connection-password", server.getPassword());
            connectorProperties.putIfAbsent("postgresql.include-system-tables", "true");

            server.execute("CREATE SCHEMA tiny");

            queryRunner.installPlugin(new StarburstPostgreSqlPlugin());
            queryRunner.createCatalog("postgresql", "postgresql", connectorProperties);

            Session tpchSetupSession = testSessionBuilder()
                    .setCatalog("postgresql")
                    .setSchema("tiny")
                    .build();
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, tpchSetupSession, requiredTablesInPostgreSqlConnector);
        }
        catch (Exception e) {
            throw closeAllSuppress(e, queryRunner);
        }
    }

    public static DistributedQueryRunner createRemoteStarburstQueryRunnerWithMemory(
            Iterable<TpchTable<?>> requiredTablesInMemoryConnector,
            Optional<SystemAccessControl> systemAccessControl)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createRemoteStarburstQueryRunner(systemAccessControl);
        addMemoryToRemoteStarburstQueryRunner(queryRunner, requiredTablesInMemoryConnector);
        return queryRunner;
    }

    public static DistributedQueryRunner createRemoteStarburstQueryRunnerWithHive(
            Path hiveCatalog,
            Iterable<TpchTable<?>> requiredTablesInHiveConnector,
            Optional<SystemAccessControl> systemAccessControl)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createRemoteStarburstQueryRunner(systemAccessControl);
        addHiveToRemoteStarburstQueryRunner(queryRunner, hiveCatalog, requiredTablesInHiveConnector);
        return queryRunner;
    }

    public static DistributedQueryRunner createRemoteStarburstQueryRunnerWithPostgreSql(
            TestingPostgreSqlServer server,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> requiredTablesInPostgreSqlConnector,
            Optional<SystemAccessControl> systemAccessControl)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createRemoteStarburstQueryRunner(systemAccessControl);
        addPostgreSqlToRemoteStarburstQueryRunner(queryRunner, server, connectorProperties, requiredTablesInPostgreSqlConnector);
        return queryRunner;
    }

    private static DistributedQueryRunner createStargateQueryRunner(
            boolean enableWrites,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("p2p_remote")
                .setSchema("tiny")
                .build();

        DistributedQueryRunner queryRunner = null;
        try {
            DistributedQueryRunner.Builder<?> builder = StarburstEngineQueryRunner.builder(session);
            extraProperties.forEach(builder::addExtraProperty);
            queryRunner = builder.build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            queryRunner.installPlugin(new JmxPlugin());
            queryRunner.createCatalog("jmx", "jmx");

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-user", "p2p");

            queryRunner.installPlugin(new TestingStargatePlugin(enableWrites));
            queryRunner.createCatalog("p2p_remote", "stargate", connectorProperties);

            return queryRunner;
        }
        catch (Exception e) {
            throw closeAllSuppress(e, queryRunner);
        }
    }

    private static String stargateConnectionUrl(DistributedQueryRunner stargateQueryRunner, String catalog)
    {
        return connectionUrl(stargateQueryRunner.getCoordinator().getBaseUrl(), catalog);
    }

    private static String connectionUrl(URI trinoUri, String catalog)
    {
        verify(Objects.equals(trinoUri.getScheme(), "http"), "Unsupported scheme: %s", trinoUri.getScheme());
        verify(trinoUri.getUserInfo() == null, "Unsupported user info: %s", trinoUri.getUserInfo());
        verify(Objects.equals(trinoUri.getPath(), ""), "Unsupported path: %s", trinoUri.getPath());
        verify(trinoUri.getQuery() == null, "Unsupported query: %s", trinoUri.getQuery());
        verify(trinoUri.getFragment() == null, "Unsupported fragment: %s", trinoUri.getFragment());

        return format("jdbc:trino://%s/%s", trinoUri.getAuthority(), catalog);
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        DistributedQueryRunner stargateQueryRunner = StarburstEngineQueryRunner.builder(testSessionBuilder()
                        .setCatalog("unspecified_catalog")
                        .setSchema("unspecified_schema")
                        .build())
                .setExtraProperties(Map.of("http-server.http.port", "8081"))
                .setNodeCount(1)// 1 is perfectly enough until we do parallel Stargate connector
                .build();

        stargateQueryRunner.installPlugin(new TpchPlugin());
        stargateQueryRunner.createCatalog("tpch", "tpch");

        addMemoryToRemoteStarburstQueryRunner(
                stargateQueryRunner,
                TpchTable.getTables());

        TestingPostgreSqlServer postgreSqlServer = new TestingPostgreSqlServer();
        addPostgreSqlToRemoteStarburstQueryRunner(
                stargateQueryRunner,
                postgreSqlServer,
                Map.of("connection-url", postgreSqlServer.getJdbcUrl()),
                TpchTable.getTables());

        Path tempDir = createTempDirectory("HiveCatalog");
        addHiveToRemoteStarburstQueryRunner(
                stargateQueryRunner,
                tempDir,
                TpchTable.getTables());

        DistributedQueryRunner queryRunner = builder(stargateQueryRunner, "memory")
                .enableWrites()
                .withExtraProperties(Map.of("http-server.http.port", "8080"))
                .build();
        queryRunner.createCatalog(
                "p2p_remote_postgresql",
                "stargate",
                Map.of(
                        "connection-user", "p2p",
                        "connection-url", stargateConnectionUrl(stargateQueryRunner, "postgresql")));
        queryRunner.createCatalog(
                "p2p_remote_hive",
                "stargate",
                Map.of(
                        "connection-user", "p2p",
                        "connection-url", stargateConnectionUrl(stargateQueryRunner, "hive")));

        Logger log = Logger.get(StargateQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\nRemote Starburst: %s\n====", stargateQueryRunner.getCoordinator().getBaseUrl());
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }

    public static Builder builder(DistributedQueryRunner remoteStarburst, String catalog)
    {
        return new Builder(remoteStarburst, catalog);
    }

    public static class Builder
    {
        private boolean enableWrites;
        private Map<String, String> connectorProperties = emptyMap();
        private Map<String, String> coordinatorProperties = emptyMap();
        private Map<String, String> extraProperties = emptyMap();

        private Builder(DistributedQueryRunner remoteStarburst, String catalog)
        {
            withConnectorProperties(ImmutableMap.of("connection-url", stargateConnectionUrl(remoteStarburst, catalog)));
        }

        public Builder enableWrites()
        {
            enableWrites = true;
            return this;
        }

        public Builder withConnectorProperties(Map<String, String> connectorProperties)
        {
            this.connectorProperties = updateProperties(this.connectorProperties, connectorProperties);
            return this;
        }

        public Builder withCoordinatorProperties(Map<String, String> coordinatorProperties)
        {
            this.coordinatorProperties = updateProperties(this.coordinatorProperties, coordinatorProperties);
            return this;
        }

        public Builder withExtraProperties(Map<String, String> coordinatorProperties)
        {
            this.extraProperties = updateProperties(this.extraProperties, coordinatorProperties);
            return this;
        }

        public Builder withStarburstStorage(JdbcDatabaseContainer<?> starburstStorage)
        {
            return withCoordinatorProperties(ImmutableMap.of(
                    "insights.jdbc.url", starburstStorage.getJdbcUrl(),
                    "insights.jdbc.user", starburstStorage.getUsername(),
                    "insights.jdbc.password", starburstStorage.getPassword()));
        }

        public Builder withManagedStatistics()
        {
            return withCoordinatorProperties(ImmutableMap.of("starburst.managed-statistics.enabled", "true"))
                    .withConnectorProperties(ImmutableMap.of(
                            "statistics.enabled", "false", // disable collecting native jdbc stats
                            "internal-communication.shared-secret", "internal-shared-secret", // This is required for the internal communication in the managed statistics
                            "managed-statistics.enabled", "true"));
        }

        public DistributedQueryRunner build()
                throws Exception
        {
            return createStargateQueryRunner(enableWrites, extraProperties, connectorProperties);
        }

        private static Map<String, String> updateProperties(Map<String, String> properties, Map<String, String> update)
        {
            return ImmutableMap.<String, String>builder()
                    .putAll(requireNonNull(properties, "properties is null"))
                    .putAll(requireNonNull(update, "update is null"))
                    .buildOrThrow();
        }
    }
}
