/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.prestoconnector;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.starburstdata.presto.plugin.postgresql.StarburstPostgreSqlPlugin;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.prestosql.Session;
import io.prestosql.plugin.hive.HiveHadoop2Plugin;
import io.prestosql.plugin.jmx.JmxPlugin;
import io.prestosql.plugin.memory.MemoryPlugin;
import io.prestosql.plugin.postgresql.TestingPostgreSqlServer;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.tpch.TpchTable;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Verify.verify;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.QueryAssertions.copyTpchTables;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public final class PrestoConnectorQueryRunner
{
    private PrestoConnectorQueryRunner() {}

    private static DistributedQueryRunner createRemotePrestoQueryRunner(Map<String, String> extraProperties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            Session session = testSessionBuilder()
                    // Require explicit table qualification or custom session.
                    .setCatalog("unspecified_catalog")
                    .setSchema("unspecified_schema")
                    .build();
            queryRunner = DistributedQueryRunner.builder(session)
                    .setNodeCount(1) // 1 is perfectly enough until we do parallel Presto Connector
                    .setExtraProperties(extraProperties)
                    .build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            return queryRunner;
        }
        catch (Exception e) {
            throw closeAllSuppress(e, queryRunner);
        }
    }

    private static void addMemoryToRemotePrestoQueryRunner(
            DistributedQueryRunner queryRunner,
            Iterable<TpchTable<?>> requiredTablesInMemoryConnector)
            throws Exception
    {
        try {
            queryRunner.installPlugin(new MemoryPlugin());
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

    private static void addHiveToRemotePrestoQueryRunner(
            DistributedQueryRunner queryRunner,
            File tmpDir,
            Iterable<TpchTable<?>> requiredTablesInHiveConnector)
            throws Exception
    {
        try {
            queryRunner.installPlugin(new HiveHadoop2Plugin());
            queryRunner.createCatalog("hive", "hive-hadoop2", ImmutableMap.of(
                    "hive.metastore", "file",
                    "hive.metastore.catalog.dir", tmpDir.toURI().toString(),
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

    private static void addPostgreSqlToRemotePrestoQueryRunner(
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
            connectorProperties.putIfAbsent("allow-drop-table", "true");
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

    public static DistributedQueryRunner createRemotePrestoQueryRunnerWithMemory(
            Map<String, String> extraProperties,
            Iterable<TpchTable<?>> requiredTablesInMemoryConnector)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createRemotePrestoQueryRunner(extraProperties);
        addMemoryToRemotePrestoQueryRunner(queryRunner, requiredTablesInMemoryConnector);
        return queryRunner;
    }

    public static DistributedQueryRunner createRemotePrestoQueryRunnerWithHive(
            File tmpDir,
            Map<String, String> extraProperties,
            Iterable<TpchTable<?>> requiredTablesInHiveConnector)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createRemotePrestoQueryRunner(extraProperties);
        addHiveToRemotePrestoQueryRunner(queryRunner, tmpDir, requiredTablesInHiveConnector);
        return queryRunner;
    }

    public static DistributedQueryRunner createRemotePrestoQueryRunnerWithPostgreSql(
            TestingPostgreSqlServer server,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> requiredTablesInPostgreSqlConnector)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createRemotePrestoQueryRunner(extraProperties);
        addPostgreSqlToRemotePrestoQueryRunner(queryRunner, server, connectorProperties, requiredTablesInPostgreSqlConnector);
        return queryRunner;
    }

    public static DistributedQueryRunner createPrestoConnectorQueryRunner(
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
            queryRunner = DistributedQueryRunner.builder(session)
                    .setExtraProperties(extraProperties)
                    .build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            queryRunner.installPlugin(new JmxPlugin());
            queryRunner.createCatalog("jmx", "jmx");

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-user", "p2p");

            queryRunner.installPlugin(new TestingPrestoConnectorPlugin(enableWrites));
            queryRunner.createCatalog("p2p_remote", "starburst-remote", connectorProperties);

            return queryRunner;
        }
        catch (Exception e) {
            throw closeAllSuppress(e, queryRunner);
        }
    }

    public static String prestoConnectorConnectionUrl(DistributedQueryRunner remotePresto, String catalog)
    {
        return connectionUrl(remotePresto.getCoordinator().getBaseUrl(), catalog);
    }

    private static String connectionUrl(URI prestoUri, String catalog)
    {
        verify(Objects.equals(prestoUri.getScheme(), "http"), "Unsupported scheme: %s", prestoUri.getScheme());
        verify(prestoUri.getUserInfo() == null, "Unsupported user info: %s", prestoUri.getUserInfo());
        verify(Objects.equals(prestoUri.getPath(), ""), "Unsupported path: %s", prestoUri.getPath());
        verify(prestoUri.getQuery() == null, "Unsupported query: %s", prestoUri.getQuery());
        verify(prestoUri.getFragment() == null, "Unsupported fragment: %s", prestoUri.getFragment());

        return format("jdbc:presto://%s/%s", prestoUri.getAuthority(), catalog);
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner remotePresto = createRemotePrestoQueryRunner(Map.of());

        addMemoryToRemotePrestoQueryRunner(
                remotePresto,
                TpchTable.getTables());

        TestingPostgreSqlServer postgreSqlServer = new TestingPostgreSqlServer();
        addPostgreSqlToRemotePrestoQueryRunner(
                remotePresto,
                postgreSqlServer,
                Map.of("connection-url", postgreSqlServer.getJdbcUrl()),
                TpchTable.getTables());

        File tempDir = Files.createTempDir();
        addHiveToRemotePrestoQueryRunner(
                remotePresto,
                tempDir,
                TpchTable.getTables());

        DistributedQueryRunner queryRunner = createPrestoConnectorQueryRunner(
                true,
                Map.of("http-server.http.port", "8080"),
                Map.of(
                        "connection-url", prestoConnectorConnectionUrl(remotePresto, "memory"),
                        "allow-drop-table", "true"));
        queryRunner.createCatalog(
                "p2p_remote_postgresql",
                "starburst-remote",
                Map.of(
                        "connection-user", "p2p",
                        "connection-url", prestoConnectorConnectionUrl(remotePresto, "postgresql"),
                        "allow-drop-table", "true"));
        queryRunner.createCatalog(
                "p2p_remote_hive",
                "starburst-remote",
                Map.of(
                        "connection-user", "p2p",
                        "connection-url", prestoConnectorConnectionUrl(remotePresto, "hive"),
                        "allow-drop-table", "true"));

        Logger log = Logger.get(PrestoConnectorQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
