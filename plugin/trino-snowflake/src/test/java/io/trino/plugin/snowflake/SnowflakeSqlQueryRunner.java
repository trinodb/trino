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
package io.trino.plugin.snowflake;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class SnowflakeSqlQueryRunner
{
    private static final String TPCH_SCHEMA = "tpch";
    private static final Optional<String> catalog = Optional.of("");
    private static final Optional<String> warehouse = Optional.of("");
    private static final String JDBC_URL = "jdbc:snowflake://account.snowflakecomputing.com";
    private static final String USER = "";
    private static final String PASSWORD = "";
    private static final String SCHEMA = "";

    private SnowflakeSqlQueryRunner()
    {
    }

    public static String getJdbcUrl()
    {
        return JDBC_URL;
    }

    public static Properties getProperties()
    {
        Properties properties = new Properties();
        properties.put("user", USER);
        properties.put("password", PASSWORD);
        properties.put("db", catalog.get());
        properties.put("schema", SCHEMA);
        properties.put("warehouse", warehouse.get());
        return properties;
    }

    public static DistributedQueryRunner createSnowflakeSqlQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables,
            Optional<String> catalog,
            Optional<String> warehouse)
            throws Exception
    {
        return createSnowflakeSqlQueryRunner(extraProperties, new HashMap<>(),
                connectorProperties, tables, runner -> {}, catalog,
                warehouse);
    }

    private static String getResourcePath(String resourceName)
    {
        return requireNonNull(SnowflakeSqlQueryRunner.class.getClassLoader().getResource(resourceName), "Resource does not exist: " + resourceName).getPath();
    }

    public static DistributedQueryRunner createSnowflakeSqlQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> coordinatorProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables,
            Consumer<QueryRunner> moreSetup,
            Optional<String> catalog,
            Optional<String> warehouse)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            DistributedQueryRunner.Builder<?> builder = DistributedQueryRunner.builder(createSession())
                    .setExtraProperties(extraProperties)
                    .setCoordinatorProperties(coordinatorProperties)
                    .setAdditionalSetup(moreSetup);
            queryRunner = builder.build();
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            // note: additional copy via ImmutableList so that if fails on nulls
            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", JDBC_URL);
            //
            //?db=DATAPIPES&warehouse=JATIN_WAREHOUSE

            connectorProperties.putIfAbsent("connection-user", USER);
            connectorProperties.putIfAbsent("connection-password", PASSWORD);
            connectorProperties.putIfAbsent("snowflake.catalog", catalog.orElse("public"));
            connectorProperties.putIfAbsent("snowflake.warehouse", warehouse.orElseThrow(() -> new IllegalStateException("Warehouse param is needed")));

            queryRunner.installPlugin(new SnowflakePlugin());
            queryRunner.createCatalog("snowflake",
                    "snowflake", connectorProperties);
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("snowflake")
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public static DistributedQueryRunner createSnowflakeSqlQueryRunner(Optional<String> catalog,
            Optional<String> warehouse)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createSnowflakeSqlQueryRunner(
                ImmutableMap.of("http-server.http.port", "8080",
                        "hide-inaccessible-columns", "true"),
                ImmutableMap.of(),
                TpchTable.getTables(),
                catalog, warehouse);

        queryRunner.installPlugin(new JmxPlugin());
        queryRunner.createCatalog("jmx", "jmx");
        return queryRunner;
    }

    public static void main(String[] args)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createSnowflakeSqlQueryRunner(
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                TpchTable.getTables(),
                catalog, warehouse);

        queryRunner.installPlugin(new JmxPlugin());
        queryRunner.createCatalog("jmx", "jmx");

        Logger log = Logger.get(SnowflakeSqlQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }

    public static Optional<String> getCatalog()
    {
        return catalog;
    }

    public static Optional<String> getWarehouse()
    {
        return warehouse;
    }

    public static Connection getConnection()
            throws SQLException
    {
        Connection connection = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
        Statement statement = connection.createStatement();
        execute(statement, "USE DATABASE " + getCatalog().get());
        execute(statement, "USE WAREHOUSE " + warehouse.get());
        execute(statement, "ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT='JSON'");
        statement.close();
        return connection;
    }

    public static void setup()
    {
        try {
            Connection connection = getConnection();
            connection.setAutoCommit(true);

            Statement statement = connection.createStatement();
            execute(statement, "CREATE DATABASE IF NOT EXISTS " + getCatalog().get());
            execute(statement, "USE DATABASE " + getCatalog().get());
            execute(statement, "CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
            execute(statement, "DROP TABLE IF EXISTS " + SCHEMA + ".testing_suite");
            execute(statement, "DROP TABLE IF EXISTS public.testing_suite_1");
            execute(statement, "DROP TABLE IF EXISTS public.testing_suite_2");
            execute(statement, "CREATE TABLE IF NOT EXISTS " + SCHEMA + ".testing_suite(id int,name varchar,country varchar)");
            execute(statement, "CREATE TABLE IF NOT EXISTS public.testing_suite_1(id int,name varchar,country varchar)");
            execute(statement, "CREATE TABLE IF NOT EXISTS public.testing_suite_2(id int,name varchar,country varchar)");
            execute(statement, "USE WAREHOUSE " + warehouse.get());
            List<String> names = Arrays.asList("Tom Brady", "Ron", "John", "Hype",
                    "Brian", "Gabe", "Ray");
            List<String> countries = Arrays.asList("IN", "AU", "NZ", "SG");
            SecureRandom random = new SecureRandom();

            PreparedStatement testingSuite =
                    connection.prepareStatement("INSERT INTO " + SCHEMA + ".testing_suite values(?,?,?)");
            PreparedStatement publicTestingSuite1 =
                    connection.prepareStatement("INSERT INTO public.testing_suite_1 values(?,?,?)");
            PreparedStatement publicTestingSuite2 =
                    connection.prepareStatement("INSERT INTO public.testing_suite_2 values(?,?,?)");
            for (int i = 0; i < 100; i++) {
                String name = names.get(random.nextInt(names.size() - 1));
                String country = countries.get(random.nextInt(countries.size() - 1));
                testingSuite.setInt(1, i);
                testingSuite.setString(2, name);
                testingSuite.setString(3, country);
                testingSuite.addBatch();

                publicTestingSuite1.setInt(1, i);
                publicTestingSuite1.setString(2, name);
                if (i > 20 && i < 30) {
                    publicTestingSuite1.setString(3, "SG");
                }
                else {
                    publicTestingSuite1.setString(3, country);
                }
                publicTestingSuite1.addBatch();

                publicTestingSuite2.setInt(1, i);
                publicTestingSuite2.setString(2, name);
                publicTestingSuite2.setString(3, country);
                publicTestingSuite2.addBatch();
            }
            testingSuite.executeBatch();
            publicTestingSuite1.executeBatch();
            publicTestingSuite2.executeBatch();
            statement.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static ResultSet execute(Statement statement, String query)
            throws SQLException
    {
        System.out.println("Executing " + query);
        return statement.executeQuery(query);
    }

    public static String getSchema()
    {
        return SCHEMA;
    }
}
