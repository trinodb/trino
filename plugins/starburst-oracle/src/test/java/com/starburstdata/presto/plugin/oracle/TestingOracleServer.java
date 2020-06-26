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

import com.github.dockerjava.api.model.Ulimit;
import com.google.common.collect.ImmutableMap;
import org.testcontainers.containers.OracleContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.function.Consumer;

import static java.lang.String.format;
import static org.testcontainers.containers.BindMode.READ_ONLY;

public final class TestingOracleServer
{
    private static final OracleContainer CONTAINER = new CustomOracleContainer(
            "harbor.starburstdata.net/testing/oracledb:12.2.0.1-ee")
            .withUsername(OracleTestUsers.USER)
            .withPassword(OracleTestUsers.PASSWORD)
            .withEnv("ORACLE_SID", "testdbsid")
            .withEnv("ORACLE_PDB", "testdb")
            .withEnv("ORACLE_PWD", "secret")
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName("oracle-master"))
            .withClasspathResourceMapping("krb/server/sqlnet.ora", "/opt/oracle/oradata/dbconfig/testdbsid/sqlnet.ora", READ_ONLY)
            .withClasspathResourceMapping("krb/server/oracle_oracle-master.keytab", "/etc/server.keytab", READ_ONLY)
            .withClasspathResourceMapping("krb/krb5.conf", "/etc/krb5.conf", READ_ONLY)
            // Recommended ulimits for running Oracle on Linux
            // https://docs.oracle.com/en/database/oracle/oracle-database/12.2/ladbi/checking-resource-limits-for-oracle-software-installation-users.html
            .withCreateContainerCmdModifier(cmd ->
                    cmd.withUlimits(new Ulimit("nofile", 1024, 65536)));

    static {
        CONTAINER.start();
    }

    public static String getJdbcUrl()
    {
        return CONTAINER.getJdbcUrl();
    }

    public static Map<String, String> connectionProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("connection-url", getJdbcUrl())
                .put("connection-user", OracleTestUsers.USER)
                .put("connection-password", OracleTestUsers.PASSWORD)
                .build();
    }

    public static void executeInOracle(String sql)
    {
        executeInOracle(connection -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql);
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static void executeInOracle(Consumer<Connection> connectionCallback)
    {
        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), OracleTestUsers.USER, OracleTestUsers.PASSWORD)) {
            connectionCallback.accept(connection);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static AutoCloseable withTable(String tableName, String tableDefinition)
    {
        executeInOracle(format("CREATE TABLE %s %s", tableName, tableDefinition));
        return () -> executeInOracle(format("DROP TABLE %s", tableName));
    }

    public static AutoCloseable withView(String tableName, String tableDefinition)
    {
        executeInOracle(format("CREATE VIEW %s AS %s", tableName, tableDefinition));
        return () -> executeInOracle(format("DROP VIEW %s", tableName));
    }

    public static AutoCloseable withSynonym(String tableName, String tableDefinition)
    {
        executeInOracle(format("CREATE SYNONYM %s FOR %s", tableName, tableDefinition));
        return () -> executeInOracle(format("DROP SYNONYM %s", tableName));
    }

    private TestingOracleServer() {}

    private static class CustomOracleContainer
            extends OracleContainer
    {
        public CustomOracleContainer(String dockerImageName)
        {
            super(dockerImageName);
        }

        @Override
        public String getJdbcUrl()
        {
            // this URL does not contain credentials
            return format("jdbc:oracle:thin:@localhost:%s/testdb", CONTAINER.getOraclePort());
        }
    }
}
