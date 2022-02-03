/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.dynamodb;

import com.google.common.collect.ImmutableMap;
import com.starburstdata.presto.testing.StarburstDistributedQueryRunner;
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

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.starburstdata.presto.redirection.AbstractTableScanRedirectionTest.redirectionDisabled;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public final class DynamoDbQueryRunner
{
    private static final Logger log = Logger.get(DynamoDbQueryRunner.class);

    private static final Map<TpchTable<?>, KeyDefinition> TPCH_KEY_DEFINITIONS;

    static {
        TPCH_KEY_DEFINITIONS = ImmutableMap.<TpchTable<?>, KeyDefinition>builder()
                .put(TpchTable.CUSTOMER, new KeyDefinition("custkey"))
                .put(TpchTable.LINE_ITEM, new KeyDefinition("orderkey", "linenumber"))
                .put(TpchTable.NATION, new KeyDefinition("nationkey"))
                .put(TpchTable.ORDERS, new KeyDefinition("orderkey"))
                .put(TpchTable.PART, new KeyDefinition("partkey"))
                .put(TpchTable.PART_SUPPLIER, new KeyDefinition("partkey", "suppkey"))
                .put(TpchTable.REGION, new KeyDefinition("regionkey"))
                .put(TpchTable.SUPPLIER, new KeyDefinition("suppkey"))
                .build();
    }

    private DynamoDbQueryRunner()
    {
    }

    public static Builder builder(String dynamoDbUrl, File schemaDirectory)
    {
        return new Builder(dynamoDbUrl, schemaDirectory);
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("dynamodb")
                .setSchema("amazondynamodb")
                .build();
    }

    private static DistributedQueryRunner createQueryRunner(Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables,
            boolean enableWrites)
            throws Exception
    {
        // Create QueryRunner with writes enabled to create TPC-H tables
        DistributedQueryRunner.Builder builder = StarburstDistributedQueryRunner.builder(createSession());
        extraProperties.forEach(builder::addExtraProperty);
        try (DistributedQueryRunner queryRunner = builder.build()) {
            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            queryRunner.installPlugin(new TestingDynamoDbPlugin(true));
            queryRunner.createCatalog("dynamodb", "dynamodb", connectorProperties);
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, redirectionDisabled(createSession()), tables);
        }

        // Create query runner to be returned with given enableWrites flag
        DistributedQueryRunner queryRunner = null;
        try {
            builder = StarburstDistributedQueryRunner.builder(createSession());
            extraProperties.forEach(builder::addExtraProperty);
            queryRunner = builder.build();

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));

            queryRunner.installPlugin(new TestingDynamoDbPlugin(enableWrites));
            queryRunner.createCatalog("dynamodb", "dynamodb", connectorProperties);

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

    public static void copyTpchTables(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables)
    {
        log.info("Loading data from %s.%s...", sourceCatalog, sourceSchema);
        long startTime = System.nanoTime();
        for (TpchTable<?> table : tables) {
            copyTable(queryRunner, sourceCatalog, sourceSchema, session, table);
        }

        log.info("Loading from %s.%s complete in %s", sourceCatalog, sourceSchema, nanosSince(startTime).toString(SECONDS));
    }

    public static void copyTable(QueryRunner queryRunner, String sourceCatalog, String sourceSchema, Session session, TpchTable<?> tpchTable)
    {
        QualifiedObjectName table = new QualifiedObjectName(sourceCatalog, sourceSchema, tpchTable.getTableName().toLowerCase(ENGLISH));

        long start = System.nanoTime();
        log.info("Running import for %s", table.getObjectName());
        @Language("SQL") String sql = format("CREATE TABLE IF NOT EXISTS %s WITH (%s) AS SELECT * FROM %s", table.getObjectName(), getCreateTableProperties(tpchTable), table);
        long rows = (Long) queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0);
        log.info("Imported %s rows for %s in %s", rows, table.getObjectName(), nanosSince(start).convertToMostSuccinctTimeUnit());

        assertThat(queryRunner.execute(session, "SELECT count(*) FROM " + table).getOnlyValue())
                .as("Table is not loaded properly: %s", table)
                .isEqualTo(queryRunner.execute(session, "SELECT count(*) FROM " + table.getObjectName()).getOnlyValue());
    }

    private static String getCreateTableProperties(TpchTable<?> table)
    {
        KeyDefinition keyDefinition = requireNonNull(TPCH_KEY_DEFINITIONS.get(table), format("No key definitions for table %s, update the map", table.getTableName()));
        String sql = format("%s = '%s'", DynamoDbTableProperties.PARTITION_KEY_ATTRIBUTE, keyDefinition.partitionKeyAttributeName);
        if (keyDefinition.sortKeyAttributeName.isPresent()) {
            sql = sql + format(", %s = '%s'", DynamoDbTableProperties.SORT_KEY_ATTRIBUTE, keyDefinition.sortKeyAttributeName.get());
        }

        return sql;
    }

    public static class Builder
    {
        private Iterable<TpchTable<?>> tables = TpchTable.getTables();
        private Map<String, String> connectorProperties;
        private Map<String, String> extraProperties;
        private boolean enableWrites;

        public Builder(String dynamoDbUrl, File schemaDirectory)
        {
            requireNonNull(dynamoDbUrl, "dynamoDbUrl is null");
            requireNonNull(schemaDirectory, "schemaDirectory is null");
            connectorProperties = ImmutableMap.<String, String>builder()
                    .put("dynamodb.aws-access-key", "access-key")
                    .put("dynamodb.aws-secret-key", "secret-key")
                    .put("dynamodb.aws-region", "us-east-2")
                    .put("dynamodb.schema-directory", schemaDirectory.getAbsolutePath())
                    .put("dynamodb.endpoint-url", dynamoDbUrl)
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

        public Builder setFirstColumnAsPrimaryKeyEnabled(boolean value)
        {
            addConnectorProperties(ImmutableMap.of("dynamodb.first-column-as-primary-key-enabled", Boolean.toString(value)));
            return this;
        }

        public Builder enableDriverLogging()
        {
            addConnectorProperties(ImmutableMap.of("dynamodb.driver-logging.enabled", "true"));
            return this;
        }

        public Builder enableWrites()
        {
            this.enableWrites = true;
            return this;
        }

        public DistributedQueryRunner build()
                throws Exception
        {
            return createQueryRunner(extraProperties, connectorProperties, tables, enableWrites);
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

        try (TestingDynamoDbServer server = new TestingDynamoDbServer()) {
            DistributedQueryRunner queryRunner = DynamoDbQueryRunner.builder(server.getEndpointUrl(), server.getSchemaDirectory())
                    .addExtraProperties(ImmutableMap.of("http-server.http.port", "8080"))
                    .build();

            Logger log = Logger.get(DynamoDbQueryRunner.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    private static class KeyDefinition
    {
        final String partitionKeyAttributeName;
        final Optional<String> sortKeyAttributeName;

        public KeyDefinition(String partitionKeyAttributeName)
        {
            this(partitionKeyAttributeName, null);
        }

        public KeyDefinition(String partitionKeyAttributeName, String sortKeyAttributeName)
        {
            this.partitionKeyAttributeName = requireNonNull(partitionKeyAttributeName, "sortKeyAttributeName is null");
            this.sortKeyAttributeName = Optional.ofNullable(sortKeyAttributeName);
        }
    }
}
