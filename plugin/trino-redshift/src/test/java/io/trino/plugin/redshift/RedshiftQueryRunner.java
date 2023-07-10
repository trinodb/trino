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
package io.trino.plugin.redshift;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.jdbi.v3.core.HandleCallback;
import org.jdbi.v3.core.HandleConsumer;
import org.jdbi.v3.core.Jdbi;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTable;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.assertj.core.api.Assertions.assertThat;

public final class RedshiftQueryRunner
{
    private static final Logger log = Logger.get(RedshiftQueryRunner.class);
    private static final String JDBC_ENDPOINT = requireSystemProperty("test.redshift.jdbc.endpoint");
    static final String JDBC_USER = requireSystemProperty("test.redshift.jdbc.user");
    static final String JDBC_PASSWORD = requireSystemProperty("test.redshift.jdbc.password");
    private static final String S3_TPCH_TABLES_ROOT = requireSystemProperty("test.redshift.s3.tpch.tables.root");
    private static final String IAM_ROLE = requireSystemProperty("test.redshift.iam.role");

    private static final String TEST_DATABASE = "testdb";
    private static final String TEST_CATALOG = "redshift";
    static final String TEST_SCHEMA = "test_schema";

    static final String JDBC_URL = "jdbc:redshift://" + JDBC_ENDPOINT + TEST_DATABASE;

    private static final String CONNECTOR_NAME = "redshift";
    private static final String TPCH_CATALOG = "tpch";

    private static final String GRANTED_USER = "alice";
    private static final String NON_GRANTED_USER = "bob";

    private RedshiftQueryRunner() {}

    public static DistributedQueryRunner createRedshiftQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createRedshiftQueryRunner(
                createSession(),
                extraProperties,
                Map.of(),
                connectorProperties,
                tables,
                queryRunner -> {});
    }

    public static DistributedQueryRunner createRedshiftQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> coordinatorProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables,
            Consumer<QueryRunner> additionalSetup)
            throws Exception
    {
        return createRedshiftQueryRunner(
                createSession(),
                extraProperties,
                coordinatorProperties,
                connectorProperties,
                tables,
                additionalSetup);
    }

    public static DistributedQueryRunner createRedshiftQueryRunner(
            Session session,
            Map<String, String> extraProperties,
            Map<String, String> coordinatorProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables,
            Consumer<QueryRunner> additionalSetup)
            throws Exception
    {
        DistributedQueryRunner runner = DistributedQueryRunner.builder(session)
                .setExtraProperties(extraProperties)
                .setCoordinatorProperties(coordinatorProperties)
                .setAdditionalSetup(additionalSetup)
                .build();
        try {
            runner.installPlugin(new TpchPlugin());
            runner.createCatalog(TPCH_CATALOG, "tpch", Map.of());

            Map<String, String> properties = new HashMap<>(connectorProperties);
            properties.putIfAbsent("connection-url", JDBC_URL);
            properties.putIfAbsent("connection-user", JDBC_USER);
            properties.putIfAbsent("connection-password", JDBC_PASSWORD);

            runner.installPlugin(new RedshiftPlugin());
            runner.createCatalog(TEST_CATALOG, CONNECTOR_NAME, properties);

            executeInRedshiftWithRetry("CREATE SCHEMA IF NOT EXISTS " + TEST_SCHEMA);
            createUserIfNotExists(NON_GRANTED_USER, JDBC_PASSWORD);
            createUserIfNotExists(GRANTED_USER, JDBC_PASSWORD);

            executeInRedshiftWithRetry(format("GRANT ALL PRIVILEGES ON DATABASE %s TO %s", TEST_DATABASE, GRANTED_USER));
            executeInRedshiftWithRetry(format("GRANT ALL PRIVILEGES ON SCHEMA %s TO %s", TEST_SCHEMA, GRANTED_USER));

            provisionTables(session, runner, tables);

            // This step is necessary for product tests
            executeInRedshiftWithRetry(format("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA %s TO %s", TEST_SCHEMA, GRANTED_USER));
        }
        catch (Throwable e) {
            closeAllSuppress(e, runner);
            throw e;
        }
        return runner;
    }

    private static Session createSession()
    {
        return createSession(GRANTED_USER);
    }

    private static Session createSession(String user)
    {
        return testSessionBuilder()
                .setCatalog(TEST_CATALOG)
                .setSchema(TEST_SCHEMA)
                .setIdentity(Identity.ofUser(user))
                .build();
    }

    private static void createUserIfNotExists(String user, String password)
    {
        try {
            executeInRedshift("CREATE USER " + user + " PASSWORD " + "'" + password + "'");
        }
        catch (Exception e) {
            // if user already exists, swallow the exception
            if (!e.getMessage().matches(".*user \"" + user + "\" already exists.*")) {
                throw e;
            }
        }
    }

    private static void executeInRedshiftWithRetry(String sql)
    {
        Failsafe.with(RetryPolicy.builder()
                        .handleIf(e -> e.getMessage().matches(".* concurrent transaction .*"))
                        .withDelay(Duration.ofSeconds(10))
                        .withMaxRetries(3)
                        .build())
                .run(() -> executeInRedshift(sql));
    }

    public static void executeInRedshift(String sql, Object... parameters)
    {
        executeInRedshift(handle -> handle.execute(sql, parameters));
    }

    public static <E extends Exception> void executeInRedshift(HandleConsumer<E> consumer)
            throws E
    {
        executeWithRedshift(consumer.asCallback());
    }

    public static <T, E extends Exception> T executeWithRedshift(HandleCallback<T, E> callback)
            throws E
    {
        return Jdbi.create(JDBC_URL, JDBC_USER, JDBC_PASSWORD).withHandle(callback);
    }

    private static synchronized void provisionTables(Session session, QueryRunner queryRunner, Iterable<TpchTable<?>> tables)
    {
        Set<String> existingTables = queryRunner.listTables(session, session.getCatalog().orElseThrow(), session.getSchema().orElseThrow())
                .stream()
                .map(QualifiedObjectName::getObjectName)
                .collect(toUnmodifiableSet());

        Streams.stream(tables)
                .map(table -> table.getTableName().toLowerCase(ENGLISH))
                .filter(name -> !existingTables.contains(name))
                .forEach(name -> copyFromS3(queryRunner, session, name));

        for (TpchTable<?> tpchTable : tables) {
            verifyLoadedDataHasSameSchema(session, queryRunner, tpchTable);
        }
    }

    private static void copyFromS3(QueryRunner queryRunner, Session session, String name)
    {
        String s3Path = format("%s/%s/%s/%s/", S3_TPCH_TABLES_ROOT, TPCH_CATALOG, TINY_SCHEMA_NAME, name);
        log.info("Creating table %s in Redshift copying from %s", name, s3Path);

        // Create table in ephemeral Redshift cluster with no data
        String createTableSql = format("CREATE TABLE %s.%s.%s AS ", session.getCatalog().orElseThrow(), session.getSchema().orElseThrow(), name) +
                format("SELECT * FROM %s.%s.%s WITH NO DATA", TPCH_CATALOG, TINY_SCHEMA_NAME, name);
        queryRunner.execute(session, createTableSql);

        // Copy data from S3 bucket to ephemeral Redshift
        String copySql = "COPY " + TEST_SCHEMA + "." + name +
                " FROM '" + s3Path + "'" +
                " IAM_ROLE '" + IAM_ROLE + "'" +
                " FORMAT PARQUET";
        executeInRedshiftWithRetry(copySql);
    }

    private static void copyFromTpchCatalog(QueryRunner queryRunner, Session session, String name)
    {
        // This function exists in case we need to copy data from the TPCH catalog rather than S3,
        // such as moving to a new AWS account or if the schema changes. We can swap this method out for
        // copyFromS3 in provisionTables and then export the data again to S3.
        copyTable(queryRunner, TPCH_CATALOG, TINY_SCHEMA_NAME, name, session);
    }

    private static void verifyLoadedDataHasSameSchema(Session session, QueryRunner queryRunner, TpchTable<?> tpchTable)
    {
        // We want to verify that the loaded data has the same schema as if we created a fresh table from the TPC-H catalog
        // If this assertion fails, we may need to recreate the Redshift tables from the TPC-H catalog and unload the data to S3
        try {
            long expectedCount = (long) queryRunner.execute("SELECT count(*) FROM " + format("%s.%s.%s", TPCH_CATALOG, TINY_SCHEMA_NAME, tpchTable.getTableName())).getOnlyValue();
            long actualCount = (long) queryRunner.execute(
                    "SELECT count(*) FROM " + format(
                            "%s.%s.%s",
                            session.getCatalog().orElseThrow(),
                            session.getSchema().orElseThrow(),
                            tpchTable.getTableName())).getOnlyValue();

            if (expectedCount != actualCount) {
                throw new RuntimeException(format("Table %s is not loaded correctly. Expected %s rows got %s", tpchTable.getTableName(), expectedCount, actualCount));
            }

            log.info("Checking column types on table %s", tpchTable.getTableName());
            MaterializedResult expectedColumns = queryRunner.execute(format("DESCRIBE %s.%s.%s", TPCH_CATALOG, TINY_SCHEMA_NAME, tpchTable.getTableName()));
            MaterializedResult actualColumns = queryRunner.execute("DESCRIBE " + tpchTable.getTableName());
            assertThat(actualColumns).containsExactlyElementsOf(expectedColumns);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to assert columns for TPC-H table " + tpchTable.getTableName(), e);
        }
    }

    /**
     * Get the named system property, throwing an exception if it is not set.
     */
    private static String requireSystemProperty(String property)
    {
        return requireNonNull(System.getProperty(property), property + " is not set");
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        DistributedQueryRunner queryRunner = createRedshiftQueryRunner(
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                ImmutableList.of());

        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
