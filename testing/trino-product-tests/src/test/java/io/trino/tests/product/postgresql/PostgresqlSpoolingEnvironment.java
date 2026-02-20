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
package io.trino.tests.product.postgresql;

import io.trino.testing.containers.Minio;
import io.trino.testing.containers.TrinoProductTestContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.trino.TrinoContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * PostgreSQL product test environment with result spooling via MinIO.
 * <p>
 * This environment provides:
 * <ul>
 *   <li>PostgreSQL 11 container (oldest supported version)</li>
 *   <li>MinIO container for result spooling</li>
 *   <li>Trino container with postgresql catalog and spooling enabled</li>
 *   <li>Pre-loaded test tables (workers_psql, datatype_psql, real_table_psql)</li>
 * </ul>
 */
public class PostgresqlSpoolingEnvironment
        extends PostgresqlEnvironment
{
    private static final String SPOOLING_BUCKET = "spooling";

    private Network network;
    private PostgreSQLContainer<?> postgresql;
    private Minio minio;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return; // Already started
        }

        network = Network.newNetwork();

        postgresql = new PostgreSQLContainer<>("postgres:11")
                .withNetwork(network)
                .withNetworkAliases("postgresql")
                .withDatabaseName("test")
                .withUsername("test")
                .withPassword("test");
        postgresql.start();

        minio = Minio.builder()
                .withNetwork(network)
                .withHostName("minio")
                .build();
        minio.start();
        minio.createBucket(SPOOLING_BUCKET);

        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("postgresql", Map.of(
                        "connector.name", "postgresql",
                        "connection-url", "jdbc:postgresql://postgresql:5432/test",
                        "connection-user", "test",
                        "connection-password", "test"))
                .build();

        // Add spooling configuration
        String spoolingManagerConfig = """
                spooling-manager.name=filesystem
                fs.s3.enabled=true
                fs.location=s3://%s/
                s3.endpoint=http://minio:%d/
                s3.region=%s
                s3.aws-access-key=%s
                s3.aws-secret-key=%s
                s3.path-style-access=true
                fs.segment.ttl=5m
                fs.segment.pruning.interval=15s
                fs.segment.pruning.batch-size=250
                fs.segment.encryption=false
                """.formatted(
                SPOOLING_BUCKET,
                Minio.MINIO_API_PORT,
                Minio.MINIO_REGION,
                Minio.MINIO_ROOT_USER,
                Minio.MINIO_ROOT_PASSWORD);

        trino.withCopyToContainer(
                Transferable.of(spoolingManagerConfig),
                "/etc/trino/spooling-manager.properties");

        // Enable spooling in config.properties by adding an init script
        String enableSpoolingScript = """
                #!/bin/bash
                echo "protocol.spooling.enabled=true" >> /etc/trino/config.properties
                echo "protocol.spooling.shared-secret-key=jxTKysfCBuMZtFqUf8UJDQ1w9ez8rynEJsJqgJf66u0=" >> /etc/trino/config.properties
                echo "protocol.spooling.retrieval-mode=storage" >> /etc/trino/config.properties
                """;
        trino.withCopyToContainer(
                Transferable.of(enableSpoolingScript, 0755),
                "/docker/presto-init.d/enable_spooling.sh");

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
        try (Connection conn = postgresql.createConnection("");
                Statement stmt = conn.createStatement()) {
            // Create workers_psql table
            stmt.execute("""
                    CREATE TABLE workers_psql (
                        id_employee INT,
                        first_name VARCHAR(32),
                        last_name VARCHAR(32),
                        date_of_employment DATE,
                        department INT,
                        id_department INT,
                        name VARCHAR(32),
                        salary INT
                    )
                    """);

            stmt.execute("""
                    INSERT INTO workers_psql VALUES
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

            // Create datatype_psql table
            stmt.execute("""
                    CREATE TABLE datatype_psql (
                        c_bigint BIGINT,
                        c_double DOUBLE PRECISION,
                        c_varchar VARCHAR(100),
                        c_date DATE,
                        c_timestamp TIMESTAMP,
                        c_boolean BOOLEAN
                    )
                    """);

            stmt.execute("""
                    INSERT INTO datatype_psql VALUES
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

            // Create real_table_psql table
            stmt.execute("""
                    CREATE TABLE real_table_psql (
                        id_employee INT,
                        salary REAL,
                        bonus FLOAT,
                        tip FLOAT(2),
                        tip2 FLOAT(40)
                    )
                    """);

            stmt.execute("""
                    INSERT INTO real_table_psql VALUES
                    (null, 4000.10889, 100.59889, 11.456789, 96.42),
                    (2, 100.97, 0.94, 0.8895, 73.658),
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

    @Override
    public Connection createPostgresqlConnection()
            throws SQLException
    {
        return postgresql.createConnection("");
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
        if (minio != null) {
            minio.close();
            minio = null;
        }
        if (postgresql != null) {
            postgresql.close();
            postgresql = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }
}
