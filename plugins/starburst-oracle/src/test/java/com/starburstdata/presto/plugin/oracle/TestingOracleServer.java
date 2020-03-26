/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.docker.DockerContainer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

import static java.lang.String.format;

public final class TestingOracleServer
{
    public static final String USER = "presto_test_user";
    public static final String PASSWORD = "testsecret";

    private static DockerContainer dockerContainer = DockerContainer.forImage("docker-proxy.aws.starburstdata.com:5001/oracledb:12.2.0.1-ee")
            .setPorts(1521)
            .setEnvironment(ImmutableMap.of(
                    "ORACLE_SID", "testdbsid",
                    "ORACLE_PDB", "testdb",
                    "ORACLE_PWD", "secret"))
            .setHealthCheck((docker) -> executeInOracle(getJdbcUrl(docker), "SELECT username FROM all_users"))
            .setHostname("oracle-master")
            .setVolumes(ImmutableMap.of(
                    getResource("krb/server/sqlnet.ora").toString(), "/opt/oracle/oradata/dbconfig/testdbsid/sqlnet.ora",
                    getResource("krb/server/oracle_oracle-master.keytab").toString(), "/etc/server.keytab",
                    getResource("krb/krb5.conf").toString(), "/etc/krb5.conf"))
            .start();

    public static String getJdbcUrl()
    {
        return getJdbcUrl(dockerContainer::getHostPort);
    }

    private static String getJdbcUrl(DockerContainer.HostPortProvider hostPortProvider)
    {
        return format("jdbc:oracle:thin:@localhost:%s/testdb", hostPortProvider.getHostPort(1521));
    }

    public static void executeInOracle(String sql)
    {
        executeInOracle(getJdbcUrl(), sql);
    }

    private static void executeInOracle(String jdbcUrl, String sql)
    {
        executeInOracle(jdbcUrl, connection -> {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(sql);
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static void executeInOracle(Consumer<Connection> connectionCallback)
    {
        executeInOracle(getJdbcUrl(), connectionCallback);
    }

    private static void executeInOracle(String jdbcUrl, Consumer<Connection> connectionCallback)
    {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, USER, PASSWORD)) {
            connectionCallback.accept(connection);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static Path getResource(String resource)
    {
        return Paths.get(OracleQueryRunner.class.getClassLoader().getResource(resource).getPath());
    }

    private TestingOracleServer() {}
}
