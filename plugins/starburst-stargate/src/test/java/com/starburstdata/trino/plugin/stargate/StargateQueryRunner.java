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
import com.starburstdata.presto.testing.StarburstDistributedQueryRunner;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.postgresql.TestingPostgreSqlServer;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.SystemAccessControl;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

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
            DistributedQueryRunner.Builder queryRunnerBuilder = StarburstDistributedQueryRunner.builder(session)
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

    public static DistributedQueryRunner createStargateQueryRunner(
            boolean enableWrites,
            Map<String, String> connectorProperties)
            throws Exception
    {
        return createStargateQueryRunner(enableWrites, Map.of(), connectorProperties);
    }

    public static DistributedQueryRunner createStargateQueryRunner(
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
            DistributedQueryRunner.Builder builder = StarburstDistributedQueryRunner.builder(session);
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

    public static String stargateConnectionUrl(DistributedQueryRunner stargateQueryRunner, String catalog)
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
        DistributedQueryRunner stargateQueryRunner = createRemoteStarburstQueryRunner(Optional.empty());

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

        DistributedQueryRunner queryRunner = createStargateQueryRunner(
                true,
                Map.of("http-server.http.port", "8080"),
                Map.of("connection-url", stargateConnectionUrl(stargateQueryRunner, "memory")));
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
}
