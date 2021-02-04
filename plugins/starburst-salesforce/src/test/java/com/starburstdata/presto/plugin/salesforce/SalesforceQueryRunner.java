/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.salesforce;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public final class SalesforceQueryRunner
{
    private static final Logger log = Logger.get(SalesforceQueryRunner.class);

    private SalesforceQueryRunner() {}

    public static Builder builder()
    {
        return new Builder();
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("salesforce")
                .setSchema("salesforce")
                .build();
    }

    private static DistributedQueryRunner createQueryRunner(Map<String, String> extraProperties, Map<String, String> connectorProperties, Iterable<TpchTable<?>> tables, boolean enableWrites)
            throws Exception
    {
        // Uncomment to drop the TPC-H tables from Salesforce
        // We typically do not want to do this (see comment below), but it is here if needed
        // dropTpchTables();

        // Copy tables first before creating the query runner
        // We need to enable writes to copy but the returned query runner will enable writes based on the given parameter
        // We also only copy the tables if they exist
        // Deleted tables from Salesforce are not actually deleted for 15 days
        // As the CI builds times, the sandbox would quickly fill up and then the builds will fail
        // We also don't want to hit our API limit, so instead we just create the tables once but will assert
        // all the data is in the tables each CI run
        copyTpchTablesIfNotExists(extraProperties, connectorProperties, tables);

        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession())
                    .setExtraProperties(extraProperties)
                    .build();

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("allow-drop-table", "true");

            queryRunner.installPlugin(new TestingSalesforcePlugin(enableWrites));
            queryRunner.createCatalog("salesforce", "salesforce", connectorProperties);

            queryRunner.installPlugin(new JmxPlugin());
            queryRunner.createCatalog("jmx", "jmx");

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static void dropTpchTables()
    {
        SalesforceConfig config = new SalesforceConfig()
                .setUser(requireNonNull(System.getProperty("salesforce.test.user1.username"), "salesforce.test.user1.username is not set"))
                .setPassword(requireNonNull(System.getProperty("salesforce.test.user1.password"), "salesforce.test.user1.password is not set"))
                .setSecurityToken(requireNonNull(System.getProperty("salesforce.test.user1.security-token"), "salesforce.test.user1.security-token is not set"))
                .setSandboxEnabled(true);

        String jdbcUrl = SalesforceConnectionFactory.getConnectionUrl(config);
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement()) {
            for (TpchTable<?> table : TpchTable.getTables()) {
                statement.execute("DROP TABLE IF EXISTS " + table.getTableName() + "__c");
            }
        }
        catch (SQLException throwables) {
            throw new RuntimeException("Failed to reset CData metadata cache", throwables);
        }
    }

    private static void copyTpchTablesIfNotExists(Map<String, String> extraProperties, Map<String, String> connectorProperties, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession())
                .setExtraProperties(extraProperties)
                .build()) {
            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("allow-drop-table", "true");

            queryRunner.installPlugin(new TestingSalesforcePlugin(true));
            queryRunner.createCatalog("salesforce", "salesforce", connectorProperties);

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            // We copy and assert in two steps as tables are only copied if they exist
            copyTpchTablesIfNotExists(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);

            // Assert tables are all loaded
            assertTablesAreLoaded(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);
        }
    }

    private static void copyTpchTablesIfNotExists(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables)
    {
        log.info("Loading data from %s.%s...", sourceCatalog, sourceSchema);
        long startTime = System.nanoTime();
        for (TpchTable<?> table : tables) {
            QualifiedObjectName qualifiedTable = new QualifiedObjectName(sourceCatalog, sourceSchema, table.getTableName().toLowerCase(ENGLISH));
            copyTableIfNotExists(queryRunner, qualifiedTable, session);
        }

        log.info("Loading from %s.%s complete in %s", sourceCatalog, sourceSchema, nanosSince(startTime).toString(SECONDS));
    }

    private static void copyTableIfNotExists(QueryRunner queryRunner, QualifiedObjectName table, Session session)
    {
        // Check if the table exists rather than running CREATE TABLE IF NOT EXISTS because the Salesforce table name has the suffix
        // The SQL query would fail because the e.g. 'customer' table doesn't exist, but then you get an error
        // trying to create the same 'customer__c' object in Salesforce
        if (!queryRunner.tableExists(session, table.getObjectName() + "__c")) {
            long start = System.nanoTime();
            log.info("Running import for %s", table.getObjectName());
            @Language("SQL") String sql = format("CREATE TABLE %s AS SELECT * FROM %s", table.getObjectName(), table);
            long rows = (Long) queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0);
            log.info("Imported %s rows for %s in %s", rows, table.getObjectName(), nanosSince(start).convertToMostSuccinctTimeUnit());
        }
        else {
            log.info("Table %s already exists, skipping import", table.getObjectName());
        }
    }

    private static void assertTablesAreLoaded(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables)
    {
        log.info("Asserting tables in %s.%s...", sourceCatalog, sourceSchema);
        for (TpchTable<?> table : tables) {
            String sourceTable = table.getTableName().toLowerCase(ENGLISH);
            QualifiedObjectName qualifiedTable = new QualifiedObjectName(sourceCatalog, sourceSchema, sourceTable);
            log.info("Running assertion for %s", qualifiedTable.getObjectName());
            assertThat(queryRunner.execute(session, "SELECT count(*) FROM " + qualifiedTable).getOnlyValue())
                    .as("Table is not loaded properly: %s", qualifiedTable)
                    .isEqualTo(queryRunner.execute(session, "SELECT count(*) FROM " + qualifiedTable.getObjectName() + "__c").getOnlyValue());
        }
    }

    public static class Builder
    {
        private Iterable<TpchTable<?>> tables = TpchTable.getTables();
        private Map<String, String> connectorProperties;
        private Map<String, String> extraProperties;
        private boolean enableWrites;

        public Builder()
        {
            connectorProperties = ImmutableMap.<String, String>builder()
                    .put("salesforce.user", requireNonNull(System.getProperty("salesforce.test.user1.username"), "salesforce.test.user1.username is not set"))
                    .put("salesforce.password", requireNonNull(System.getProperty("salesforce.test.user1.password"), "salesforce.test.user1.password is not set"))
                    .put("salesforce.security-token", requireNonNull(System.getProperty("salesforce.test.user1.security-token"), "salesforce.test.user1.security-token is not set"))
                    .put("salesforce.enable-sandbox", "true")
                    .build();
            extraProperties = ImmutableMap.of();
        }

        public Builder addConnectorProperties(Map<String, String> properties)
        {
            connectorProperties = updateProperties(connectorProperties, properties);
            return this;
        }

        public Builder addExtraProperties(Map<String, String> properties)
        {
            extraProperties = updateProperties(extraProperties, properties);
            return this;
        }

        public Builder setTables(Iterable<TpchTable<?>> tables)
        {
            this.tables = requireNonNull(tables, "tables is null");
            return this;
        }

        public Builder enableWrites()
        {
            this.enableWrites = true;
            return this;
        }

        public Builder enableDriverLogging()
        {
            addConnectorProperties(ImmutableMap.of("salesforce.driver-logging.enabled", "true"));
            return this;
        }

        public DistributedQueryRunner build()
                throws Exception
        {
            return createQueryRunner(
                    extraProperties,
                    connectorProperties,
                    tables,
                    enableWrites);
        }

        private static Map<String, String> updateProperties(Map<String, String> properties, Map<String, String> update)
        {
            return ImmutableMap.<String, String>builder()
                    .putAll(requireNonNull(properties, "properties is null"))
                    .putAll(requireNonNull(update, "update is null"))
                    .build();
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        DistributedQueryRunner queryRunner = SalesforceQueryRunner.builder()
                .enableWrites()
                .addExtraProperties(ImmutableMap.of("http-server.http.port", "8080"))
                .build();

        Logger log = Logger.get(SalesforceQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
