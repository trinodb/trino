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
package io.trino.tests.product.mysql;

import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.trino.TrinoContainer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * MySQL product test environment with Trino configured to connect to MySQL.
 * <p>
 * This environment provides:
 * <ul>
 *   <li>MySQL 8.0 container with test database</li>
 *   <li>Trino container with mysql and jmx catalogs</li>
 *   <li>Pre-loaded test tables (workers_mysql, datatype_mysql, real_table_mysql)</li>
 * </ul>
 */
public class MySqlEnvironment
        extends ProductTestEnvironment
{
    private Network network;
    private MySQLContainer<?> mysql;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return; // Already started
        }

        network = Network.newNetwork();

        mysql = new MySQLContainer<>("mysql:8.0")
                .withNetwork(network)
                .withNetworkAliases("mysql")
                .withDatabaseName("test")
                .withUsername("test")
                .withPassword("test");
        mysql.start();

        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("mysql", Map.of(
                        "connector.name", "mysql",
                        "connection-url", "jdbc:mysql://mysql:3306",
                        "connection-user", "test",
                        "connection-password", "test"))
                .withCatalog("jmx", Map.of(
                        "connector.name", "jmx"))
                .build();
        trino.start();

        try {
            TrinoProductTestContainer.waitForClusterReady(trino);
        }
        catch (SQLException | InterruptedException e) {
            throw new RuntimeException("Failed to wait for Trino cluster", e);
        }

        createTestTables();
    }

    private void createTestTables()
    {
        try (Connection conn = mysql.createConnection("");
                Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE workers_mysql (
                        id_employee INT,
                        first_name VARCHAR(32),
                        last_name VARCHAR(32),
                        date_of_employment DATE,
                        department TINYINT(1),
                        id_department INT,
                        name VARCHAR(32),
                        salary INT
                    )
                    """);

            stmt.execute("""
                    INSERT INTO workers_mysql VALUES
                    (null, null, null, null, null, 1, 'Marketing', 4000),
                    (2, 'Ann', 'Turner', '2000-05-28', 2, 2, 'R&D', 5000),
                    (3, 'Martin', 'Smith', '2000-05-28', 2, 2, 'R&D', 5000),
                    (null, null, null, null, null, 3, 'Finance', 3000),
                    (4, 'Joana', 'Donne', '2002-04-05', 4, 4, 'IT', 4000),
                    (5, 'Kate', 'Grant', '2001-04-06', 5, 5, 'HR', 2000),
                    (6, 'Christopher', 'Johnson', '2001-04-06', 5, 5, 'HR', 2000),
                    (null, null, null, null, null, 6, 'PR', 3000),
                    (7, 'George', 'Cage', '2003-10-09', 7, 7, 'CustomerService', 2300),
                    (8, 'Jacob', 'Brown', '2003-10-09', 8, 8, 'Production', 2400),
                    (9, 'John', 'Black', '2004-05-09', 9, 9, 'Quality', 3400),
                    (null, null, null, null, null, 10, 'Sales', 3500),
                    (10, 'Charlie', 'Page', '2000-11-12', 11, null, null, null),
                    (1, 'Mary', 'Parker', '1999-04-03', 12, null, null, null)
                    """);

            stmt.execute("""
                    CREATE TABLE datatype_mysql (
                        c_bigint BIGINT,
                        c_double DOUBLE,
                        c_varchar VARCHAR(100),
                        c_date DATE,
                        c_timestamp TIMESTAMP NULL,
                        c_boolean BOOLEAN
                    )
                    """);

            stmt.execute("""
                    INSERT INTO datatype_mysql VALUES
                    (12, 12.25, 'String1', '1999-01-08', '1999-01-08 02:05:06', true),
                    (25, 55.52, 'test', '1952-01-05', '1989-01-08 04:05:06', false),
                    (964, 0.245, 'Again', '1936-02-08', '2005-01-09 04:05:06', false),
                    (100, 12.25, 'testing', '1949-07-08', '2002-01-07 01:05:06', true),
                    (100, 99.8777, 'AGAIN', '1987-04-09', '2010-01-02 04:03:06', true),
                    (5252, 12.25, 'sample', '1987-04-09', '2010-01-02 04:03:06', true),
                    (100, 9.8777, 'STRING1', '1923-04-08', '2010-01-02 05:09:06', true),
                    (8996, 98.8777, 'again', '1987-04-09', '2010-01-02 04:03:06', false),
                    (100, 12.8788, 'string1', '1922-04-02', '2010-01-02 02:05:06', true),
                    (5748, 67.87, 'sample', '1987-04-06', '2010-01-02 04:03:06', true),
                    (5748, 67.87, 'Sample', '1987-04-06', '2010-01-02 04:03:06', true),
                    (5748, 67.87, 'sample', '1987-04-06', '2010-01-02 04:03:06', true),
                    (5748, 67.87, 'sample', '1987-04-06', '2010-01-02 04:03:06', true),
                    (5000, 67.87, 'testing', null, '2010-01-02 04:03:06', null),
                    (6000, null, null, '1987-04-06', null, true),
                    (null, 98.52, null, null, null, true)
                    """);

            stmt.execute("""
                    CREATE TABLE real_table_mysql (
                        id_employee INT,
                        salary REAL,
                        bonus FLOAT,
                        tip FLOAT(2),
                        tip2 FLOAT(30)
                    )
                    """);

            stmt.execute("""
                    INSERT INTO real_table_mysql VALUES
                    (null, 4000.10889, 217.646, 348.6542, 50.49),
                    (2, 100.97, 0.8104, 0.438, 0.58),
                    (null, null, null, null, null)
                    """);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to create test tables", e);
        }
    }

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino);
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino, user);
    }

    /**
     * Creates a direct connection to MySQL for test setup/teardown.
     */
    public Connection createMySqlConnection()
            throws SQLException
    {
        return mysql.createConnection("");
    }

    @Override
    protected void afterEachTest()
            throws Exception
    {
        resetTestSchema();
    }

    @Override
    public String getTrinoJdbcUrl()
    {
        return trino.getJdbcUrl();
    }

    @Override
    public boolean isRunning()
    {
        return trino != null && trino.isRunning();
    }

    @Override
    protected void doClose()
    {
        if (trino != null) {
            trino.close();
            trino = null;
        }
        if (mysql != null) {
            mysql.close();
            mysql = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }

    private void resetTestSchema()
            throws SQLException
    {
        try (Connection connection = createMySqlConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("SET FOREIGN_KEY_CHECKS = 0");
            List<String> tables = new ArrayList<>();
            try (ResultSet rs = statement.executeQuery("SHOW TABLES")) {
                while (rs.next()) {
                    tables.add(rs.getString(1));
                }
            }
            for (String table : tables) {
                statement.execute("DROP TABLE IF EXISTS `" + table + "`");
            }
            statement.execute("SET FOREIGN_KEY_CHECKS = 1");
        }
        createTestTables();
    }
}
