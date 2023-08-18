/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.salesforce;

import com.google.common.collect.ImmutableMap;
import com.starburstdata.trino.plugins.license.LicenseManager;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchColumn;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

public final class SalesforceQueryRunner
{
    public static final LicenseManager NOOP_LICENSE_MANAGER = () -> true;

    private static final Logger log = Logger.get(SalesforceQueryRunner.class);

    private SalesforceQueryRunner() {}

    public static Builder builder()
    {
        return new Builder();
    }

    public static Session createSession(String catalogName)
    {
        return testSessionBuilder()
                .setCatalog(catalogName)
                .setSchema("salesforce")
                .build();
    }

    private static DistributedQueryRunner createQueryRunner(
            Map<String, String> extraProperties,
            String catalogName,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables,
            boolean enableWrites)
            throws Exception
    {
        // Copy tables first before creating the query runner
        // We need to enable writes to copy but the returned query runner will enable writes based on the given parameter
        // We also only copy the tables if they exist
        // Deleted tables from Salesforce are not actually deleted for 15 days
        // As the CI builds times, the sandbox would quickly fill up and then the builds will fail
        // We also don't want to hit our API limit, so instead we just create the tables once but will assert
        // all the data is in the tables each CI run
        copyTpchTablesIfNotExists(extraProperties, catalogName, connectorProperties, tables);

        DistributedQueryRunner queryRunner = null;
        try {
            DistributedQueryRunner.Builder<?> builder = DistributedQueryRunner.builder(createSession(catalogName));
            extraProperties.forEach(builder::addExtraProperty);
            queryRunner = builder.build();

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));

            queryRunner.installPlugin(new TestingSalesforcePlugin(enableWrites));
            queryRunner.createCatalog(catalogName, "salesforce", connectorProperties);

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

    private static void truncateTable(String tableName)
    {
        SalesforcePasswordConfig passwordConfig = new SalesforcePasswordConfig()
                .setUser(requireNonNull(System.getProperty("salesforce.test.basic.auth.user"), "salesforce.test.basic.auth.user is not set"))
                .setPassword(requireNonNull(System.getProperty("salesforce.test.basic.auth.password"), "salesforce.test.basic.auth.password is not set"))
                .setSecurityToken(requireNonNull(System.getProperty("salesforce.test.basic.auth.security-token"), "salesforce.test.basic.auth.security-token is not set"));

        SalesforceConfig config = new SalesforceConfig()
                .setSandboxEnabled(true);

        // Query Salesforce to get all of the Id columns for the rows and insert them into a temp table
        // which stores the data in-memory in the driver
        // Then use DELETE FROM the temp table to issue batched deletes to Salesforce
        String jdbcUrl = new SalesforceModule.PasswordConnectionUrlProvider(config, passwordConfig).get();
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement();
                PreparedStatement preparedStatement = connection.prepareStatement(format("INSERT INTO %s__c#TEMP (Id) VALUES (?)", tableName));
                ResultSet results = statement.executeQuery(format("SELECT Id FROM %s__c", tableName))) {
            boolean hasData = false;
            while (results.next()) {
                hasData = true;
                preparedStatement.setObject(1, results.getObject(1));
                preparedStatement.execute();
            }

            if (hasData) {
                statement.execute(format("DELETE FROM %s__c WHERE EXISTS SELECT Id FROM %s__c#TEMP", tableName, tableName));
            }

            // Assert the table is empty
            try (ResultSet countResults = statement.executeQuery(format("SELECT COUNT(*) FROM %s__c", tableName))) {
                countResults.next();
                int numRows = countResults.getInt(1);
                assertThat(numRows)
                        .as(format("Table %s has %s rows but expected 0", tableName, numRows))
                        .isEqualTo(0);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Error truncating table", e);
        }
    }

    private static void copyTpchTablesIfNotExists(Map<String, String> extraProperties, String catalogName, Map<String, String> connectorProperties, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner.Builder<?> builder = DistributedQueryRunner.builder(createSession(catalogName));
        extraProperties.forEach(builder::addExtraProperty);
        try (DistributedQueryRunner queryRunner = builder.build()) {
            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));

            // If the redirection properties are included with redirectionDisabled(createSession()), then queryRunner.tableExists fails in copyTableIfNotExists
            // We get an error from the SessionPropertyManager.getConnectorSessionPropertyMetadata "Unknown catalog: salesforce"
            // Not sure if something is missing or this error is unique to this connector since we check for table existence before creating it
            for (String redirectionProperty : connectorProperties.keySet().stream().filter(key -> key.startsWith("redirection")).collect(Collectors.toList())) {
                connectorProperties.remove(redirectionProperty);
            }

            queryRunner.installPlugin(new TestingSalesforcePlugin(true));
            queryRunner.createCatalog(catalogName, "salesforce", connectorProperties);

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            copyTpchTablesIfNotExists(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(catalogName), tables);
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
            copyTableIfNotExists(queryRunner, table, qualifiedTable, session);
        }

        log.info("Loading from %s.%s complete in %s", sourceCatalog, sourceSchema, nanosSince(startTime).toString(SECONDS));
    }

    private static void copyTableIfNotExists(QueryRunner queryRunner, TpchTable<?> table, QualifiedObjectName qualifiedTable, Session session)
    {
        // Check if the table exists rather than running CREATE TABLE IF NOT EXISTS because the Salesforce table name has the suffix
        // The SQL query would fail because the e.g. 'customer' table doesn't exist, but then you get an error
        // trying to create the same 'customer__c' object in Salesforce

        // We assert the row count if it does exist to check if it is loaded correctly
        // If not, the table is truncated and then re-loaded

        @Language("SQL") String sql;
        if (!queryRunner.tableExists(session, qualifiedTable.getObjectName() + "__c")) {
            log.info("Table %s does not exist, running CTAS", qualifiedTable.getObjectName());
            sql = format("CREATE TABLE %s AS SELECT * FROM %s", qualifiedTable.getObjectName(), qualifiedTable);
        }
        else {
            log.info("Table %s exists, checking row count", qualifiedTable.getObjectName());
            long expectedCount = (long) queryRunner.execute(session, "SELECT count(*) FROM " + qualifiedTable).getOnlyValue();
            long actualCount = (long) queryRunner.execute(session, "SELECT count(*) FROM " + qualifiedTable.getObjectName() + "__c").getOnlyValue();

            if (expectedCount == actualCount) {
                log.info("Table %s already exists and is loaded correctly", qualifiedTable.getObjectName());
                return;
            }

            log.info("Table %s exists, truncating table and reloading data", qualifiedTable.getObjectName());
            truncateTable(qualifiedTable.getObjectName().toLowerCase(ENGLISH));

            String columnDefinition = table.getColumns().stream().map(TpchColumn::getSimplifiedColumnName).collect(joining("__c, ", "", "__c"));
            String columnMappings = table.getColumns().stream().map(TpchColumn::getSimplifiedColumnName).map(name -> format("%s AS %s__c", name, name)).collect(joining(", "));
            sql = format("INSERT INTO %s__c (%s) SELECT %s FROM %s", qualifiedTable.getObjectName(), columnDefinition, columnMappings, qualifiedTable);
        }

        // Run either the CREATE or INSERT and assert that it is loaded correctly, failing if it is not
        long start = System.nanoTime();
        log.info("Running import for %s", qualifiedTable.getObjectName());
        long rows = (Long) queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0);
        log.info("Imported %s rows for %s in %s", rows, qualifiedTable.getObjectName(), nanosSince(start).convertToMostSuccinctTimeUnit());

        log.info("Running assertion for %s", qualifiedTable.getObjectName());
        assertThat(queryRunner.execute(session, "SELECT count(*) FROM " + qualifiedTable).getOnlyValue())
                .as("Table is not loaded properly: %s", qualifiedTable)
                .isEqualTo(queryRunner.execute(session, "SELECT count(*) FROM " + qualifiedTable.getObjectName() + "__c").getOnlyValue());
    }

    public static class Builder
    {
        private Iterable<TpchTable<?>> tables = TpchTable.getTables();
        private String catalogName = "salesforce";
        private Map<String, String> connectorProperties;
        private Map<String, String> extraProperties;
        private boolean enableWrites;

        public Builder()
        {
            connectorProperties = ImmutableMap.<String, String>builder()
                    .put("salesforce.user", requireNonNull(System.getProperty("salesforce.test.basic.auth.user"), "salesforce.test.basic.auth.user is not set"))
                    .put("salesforce.password", requireNonNull(System.getProperty("salesforce.test.basic.auth.password"), "salesforce.test.basic.auth.password is not set"))
                    .put("salesforce.security-token", requireNonNull(System.getProperty("salesforce.test.basic.auth.security-token"), "salesforce.test.basic.auth.security-token is not set"))
                    .put("salesforce.enable-sandbox", "true")
                    .buildOrThrow();
            extraProperties = ImmutableMap.of();
        }

        public Builder setCatalogName(String catalogName)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            return this;
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
                    catalogName,
                    connectorProperties,
                    tables,
                    enableWrites);
        }

        private static Map<String, String> updateProperties(Map<String, String> properties, Map<String, String> update)
        {
            return ImmutableMap.<String, String>builder()
                    .putAll(requireNonNull(properties, "properties is null"))
                    .putAll(requireNonNull(update, "update is null"))
                    .buildOrThrow();
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
