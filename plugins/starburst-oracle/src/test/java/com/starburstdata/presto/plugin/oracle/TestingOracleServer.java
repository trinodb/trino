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

import org.testcontainers.containers.OracleContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

import static java.lang.String.format;
import static org.testcontainers.containers.BindMode.READ_ONLY;

public final class TestingOracleServer
{
    public static final String USER = "presto_test_user";
    public static final String PASSWORD = "testsecret";

    private static final OracleContainer CONTAINER = new CustomOracleContainer(
            "docker-proxy.aws.starburstdata.com:5001/oracledb:12.2.0.1-ee")
            .withUsername(USER)
            .withPassword(PASSWORD)
            .withEnv("ORACLE_SID", "testdbsid")
            .withEnv("ORACLE_PDB", "testdb")
            .withEnv("ORACLE_PWD", "secret")
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName("oracle-master"))
            .withClasspathResourceMapping("krb/server/sqlnet.ora", "/opt/oracle/oradata/dbconfig/testdbsid/sqlnet.ora", READ_ONLY)
            .withClasspathResourceMapping("krb/server/oracle_oracle-master.keytab", "/etc/server.keytab", READ_ONLY)
            .withClasspathResourceMapping("krb/krb5.conf", "/etc/krb5.conf", READ_ONLY);

    static {
        CONTAINER.start();
    }

    public static String getJdbcUrl()
    {
        return CONTAINER.getJdbcUrl();
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
        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), USER, PASSWORD)) {
            connectionCallback.accept(connection);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
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
