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
package snowflake;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.base.security.FileBasedSystemAccessControl;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.SystemAccessControl;
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
import java.util.function.Consumer;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class SnowflakeSqlQueryRunner
{
    private static final String TPCH_SCHEMA = "tpch";
    private static final Optional<String> catalog = Optional.of("DATAPIPES_RBAC_TESTING");
    private static final Optional<String> warehouse = Optional.of("JATIN_WAREHOUSE");
    private static final String JDBC_URL = "jdbc:snowflake://BO78973.ap-southeast-1.snowflakecomputing.com";
    private static final String USER = "JATINYADAV";
    private static final String PASSWORD = "pass";
    private static final String SCHEMA = "junit";

    private SnowflakeSqlQueryRunner() {}

    public static DistributedQueryRunner createSnowflakeSqlQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables,
            Optional<String> catalog,
            Optional<String> warehouse,
            Optional<String> ruleFile)
            throws Exception
    {
        return createSnowflakeSqlQueryRunner(extraProperties, Map.of(), connectorProperties, tables, runner -> {}, catalog,
                warehouse, ruleFile);
    }

    private static String getResourcePath(String resourceName)
    {
        return requireNonNull(SnowflakeSqlQueryRunner.class.getClassLoader().getResource(resourceName), "Resource does not exist: " + resourceName).getPath();
    }

    private static SystemAccessControl newFileBasedSystemAccessControl(String resourceName)
    {
        return newFileBasedSystemAccessControl(ImmutableMap.of("security.config-file", getResourcePath(resourceName)));
    }

    private static SystemAccessControl newFileBasedSystemAccessControl(ImmutableMap<String, String> config)
    {
        return new FileBasedSystemAccessControl.Factory().create(config);
    }

    public static DistributedQueryRunner createSnowflakeSqlQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> coordinatorProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables,
            Consumer<QueryRunner> moreSetup,
            Optional<String> catalog,
            Optional<String> warehouse,
            Optional<String> rulesFile)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            DistributedQueryRunner.Builder<?> builder = DistributedQueryRunner.builder(createSession())
                    .setExtraProperties(extraProperties)
                    .setCoordinatorProperties(coordinatorProperties)
                    .setAdditionalSetup(moreSetup);
            if (rulesFile.isPresent()) {
                builder.setSystemAccessControls(Arrays.asList(
                        newFileBasedSystemAccessControl(rulesFile.get())
                ));
            }
            queryRunner = builder.build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            // note: additional copy via ImmutableList so that if fails on nulls
            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", JDBC_URL);
            //?db=DATAPIPES&warehouse=JATIN_WAREHOUSE
            connectorProperties.putIfAbsent("connection-user", USER);
            connectorProperties.putIfAbsent("connection-password", PASSWORD);
            connectorProperties.putIfAbsent(SnowflakeConfig.SNOWFLAKE_CATALOG, catalog.orElse("public"));
            connectorProperties.putIfAbsent(SnowflakeConfig.SNOWFLAKE_WAREHOUSE, warehouse.orElseThrow(() -> new IllegalStateException("Warehouse param is needed")));
            connectorProperties.putIfAbsent(SnowflakeConfig.SNOWFLAKE_STAGE_NAME, "SNOWFLAKE_STAGE");

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
            Optional<String> warehouse,
            Optional<String> ruleFile)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createSnowflakeSqlQueryRunner(
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                TpchTable.getTables(),
                catalog, warehouse,
                ruleFile
        );

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
                catalog, warehouse,
                Optional.empty());

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

    public static void setup()
    {
        try {
            Connection connection = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
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
                    "Brian", "Gabe Itch", "Ray Piste");
            List<String> countries = Arrays.asList("IN", "AU", "NZ", "SG");
            SecureRandom random = new SecureRandom();

            PreparedStatement testingSuite =
                    connection.prepareStatement("INSERT INTO " + SCHEMA + ".testing_suite values(?,?,?)");
            PreparedStatement public_testingSuite1 =
                    connection.prepareStatement("INSERT INTO public.testing_suite_1 values(?,?,?)");
            PreparedStatement public_testingSuite2 =
                    connection.prepareStatement("INSERT INTO public.testing_suite_2 values(?,?,?)");
            for (int i = 0; i < 100; i++) {
                String name = names.get(random.nextInt(names.size() - 1));
                String country = countries.get(random.nextInt(countries.size() - 1));
                testingSuite.setInt(1, i);
                testingSuite.setString(2, name);
                testingSuite.setString(3, country);
                testingSuite.addBatch();

                public_testingSuite1.setInt(1, i);
                public_testingSuite1.setString(2, name);
                if (i > 20 && i < 30) {
                    public_testingSuite1.setString(3, "SG");
                }
                else {
                    public_testingSuite1.setString(3, country);
                }
                public_testingSuite1.addBatch();

                public_testingSuite2.setInt(1, i);
                public_testingSuite2.setString(2, name);
                public_testingSuite2.setString(3, country);
                public_testingSuite2.addBatch();
            }
            testingSuite.executeBatch();
            public_testingSuite1.executeBatch();
            public_testingSuite2.executeBatch();
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
