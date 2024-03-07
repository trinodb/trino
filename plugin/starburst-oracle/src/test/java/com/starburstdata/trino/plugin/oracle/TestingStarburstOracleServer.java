/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.oracle;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Ulimit;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import io.trino.testing.SharedResource;
import io.trino.testing.SharedResource.Lease;
import io.trino.testing.containers.junit.ReportLeakedContainers;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import org.intellij.lang.annotations.Language;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.function.Consumer;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestingStarburstOracleServer
        implements AutoCloseable
{
    // testcontainers starts this image when the `withAccessToHost` feature is enabled.
    private static final String TEST_CONTAINERS_SSHD_IMAGE_NAME = "testcontainers/sshd";
    private static final SharedResource<TestingStarburstOracleServer> instance = new SharedResource<>(TestingStarburstOracleServer::new);

    private final Closer closer = Closer.create();
    private final OracleContainer container;

    public static Lease<TestingStarburstOracleServer> getInstance()
            throws Exception
    {
        return instance.getInstanceLease();
    }

    private TestingStarburstOracleServer()
    {
        this(new CustomOracleContainer(
                DockerImageName.parse("843985043183.dkr.ecr.us-east-2.amazonaws.com/testing/oracledb:19.3.0-ee").asCompatibleSubstituteFor("gvenzl/oracle-xe"))
                .withUsername(OracleTestUsers.USER)
                .withPassword(OracleTestUsers.PASSWORD)
                .withEnv("ORACLE_SID", "testdbsid")
                .withEnv("ORACLE_PDB", "testdb")
                .withEnv("ORACLE_PWD", "secret")
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName("oracle-master"))
                // Needed to avoid some networking issues between the client and the server
                .withAccessToHost(true)
                // Recommended ulimits for running Oracle on Linux
                // https://docs.oracle.com/en/database/oracle/oracle-database/12.2/ladbi/checking-resource-limits-for-oracle-software-installation-users.html
                .withCreateContainerCmdModifier(cmd ->
                        requireNonNull(cmd.getHostConfig()).withUlimits(ImmutableList.of(new Ulimit("nofile", 1024L, 65536L)))));
    }

    protected TestingStarburstOracleServer(OracleContainer oracleContainer)
    {
        this.container = oracleContainer;
        closer.register(container::stop);

        try {
            this.container.start();
            // Ignore container "leaks" since we're using a shared singleton across several tests.
            ReportLeakedContainers.ignoreContainerId(container.getContainerId());
            // Containers using `withAccessToHost` cause testcontainers to start a sshd container. Testcontainers is responsible for cleaning it up.
            @SuppressWarnings("resource")
            DockerClient client = DockerClientFactory.lazyClient();
            client.listContainersCmd()
                    .exec()
                    .stream()
                    .filter(container -> container.getImage().contains(TEST_CONTAINERS_SSHD_IMAGE_NAME))
                    .forEach(container -> ReportLeakedContainers.ignoreContainerId(container.getId()));
        }
        catch (Exception e) {
            closeAllSuppress(e, this);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
            throws Exception
    {
        closer.close();
    }

    public SqlExecutor getSqlExecutor()
    {
        return new SqlExecutor()
        {
            @Override
            public boolean supportsMultiRowInsert()
            {
                return false;
            }

            @Override
            public void execute(String sql)
            {
                executeInOracle(sql);
            }
        };
    }

    public String getJdbcUrl()
    {
        return container.getJdbcUrl();
    }

    public Map<String, String> connectionProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("connection-url", getJdbcUrl())
                .put("connection-user", OracleTestUsers.USER)
                .put("connection-password", OracleTestUsers.PASSWORD)
                .buildOrThrow();
    }

    public void executeInOracle(@Language("SQL") String sql)
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

    public void executeInOracle(Consumer<Connection> connectionCallback)
    {
        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), OracleTestUsers.USER, OracleTestUsers.PASSWORD)) {
            connectionCallback.accept(connection);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @deprecated Use {@link TestTable} instead.
     */
    @Deprecated
    public AutoCloseable withTable(String tableName, String tableDefinition)
    {
        executeInOracle(format("CREATE TABLE %s %s", tableName, tableDefinition));
        return () -> executeInOracle(format("DROP TABLE %s", tableName));
    }

    /**
     * @deprecated Use {@link TestView} instead.
     */
    @Deprecated
    public AutoCloseable withView(String tableName, String tableDefinition)
    {
        executeInOracle(format("CREATE VIEW %s AS %s", tableName, tableDefinition));
        return () -> executeInOracle(format("DROP VIEW %s", tableName));
    }

    public AutoCloseable withSynonym(String tableName, String tableDefinition)
    {
        executeInOracle(format("CREATE SYNONYM %s FOR %s", tableName, tableDefinition));
        return () -> executeInOracle(format("DROP SYNONYM %s", tableName));
    }

    public void gatherStatisticsInOracle(String tableName)
    {
        executeInOracle(connection -> {
            try (CallableStatement statement = connection.prepareCall("{CALL DBMS_STATS.GATHER_TABLE_STATS(?, ?)}")) {
                statement.setString(1, OracleTestUsers.USER);
                statement.setString(2, tableName);
                statement.execute();
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected static class CustomOracleContainer
            extends OracleContainer
    {
        public CustomOracleContainer(DockerImageName dockerImageName)
        {
            super(dockerImageName);
        }

        @Override
        public String getJdbcUrl()
        {
            // this URL does not contain credentials
            return format("jdbc:oracle:thin:@localhost:%s/testdb", this.getOraclePort());
        }
    }
}
