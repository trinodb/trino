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
package io.trino.plugin.oracle;

import io.airlift.log.Logger;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.RetryingConnectionFactory;
import io.trino.plugin.jdbc.credential.StaticCredentialProvider;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import oracle.jdbc.OracleDriver;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.utility.MountableFile;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.temporal.ChronoUnit;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.containers.TestContainers.startOrReuse;
import static java.lang.String.format;

public class TestingOracleServer
        extends OracleContainer
        implements Closeable
{
    private static final Logger log = Logger.get(TestingOracleServer.class);
    private static final RetryPolicy<Object> CONTAINER_RETRY_POLICY = new RetryPolicy<>()
            .withBackoff(1, 5, ChronoUnit.SECONDS)
            .withMaxAttempts(5)
            .onRetry(event -> log.warn(
                    "Container initialization failed on attempt %s, will retry. Exception: %s",
                    event.getAttemptCount(),
                    event.getLastFailure().getMessage()));

    private static final String TEST_TABLESPACE = "trino_test";

    public static final String TEST_USER = "trino_test";
    public static final String TEST_SCHEMA = TEST_USER; // schema and user is the same thing in Oracle
    public static final String TEST_PASS = "trino_test_password";

    public TestingOracleServer()
    {
        super("wnameless/oracle-xe-11g-r2");

        Failsafe.with(CONTAINER_RETRY_POLICY).run(this::createContainer);
    }

    private void createContainer()
            throws Exception
    {
        withCopyFileToContainer(MountableFile.forClasspathResource("init.sql"), "/docker-entrypoint-initdb.d/init.sql");

        Closeable cleanup = startOrReuse(this);
        try {
            setUpDatabase();
        }
        catch (Exception e) {
            closeAllSuppress(e, cleanup);
            throw e;
        }
    }

    private void setUpDatabase()
            throws Exception
    {
        try (Connection connection = getConnectionFactory().openConnection(SESSION);
                Statement statement = connection.createStatement()) {
            // this is added to allow more processes on database, otherwise the tests end up giving
            // ORA-12519, TNS:no appropriate service handler found
            // ORA-12505, TNS:listener does not currently know of SID given in connect descriptor
            // to fix this we have to change the number of processes of SPFILE
            statement.execute("ALTER SYSTEM SET processes=1000 SCOPE=SPFILE");
            statement.execute("ALTER SYSTEM SET disk_asynch_io = FALSE SCOPE = SPFILE");
        }

        execInContainer("/bin/bash", "/etc/init.d/oracle-xe", "restart");

        waitUntilContainerStarted();

        try (Connection connection = getConnectionFactory().openConnection(SESSION);
                Statement statement = connection.createStatement()) {
            log.info("Create tablespace");
            statement.execute(format("CREATE TABLESPACE %s DATAFILE 'test_db.dat' SIZE 100M ONLINE", TEST_TABLESPACE));
            statement.execute(format("CREATE USER %s IDENTIFIED BY %s DEFAULT TABLESPACE %s", TEST_USER, TEST_PASS, TEST_TABLESPACE));
            statement.execute(format("GRANT UNLIMITED TABLESPACE TO %s", TEST_USER));
            statement.execute(format("GRANT ALL PRIVILEGES TO %s", TEST_USER));
        }
    }

    @Override
    public String getJdbcUrl()
    {
        return "jdbc:oracle:thin:@" + getHost() + ":" + getOraclePort() + ":" + getSid();
    }

    public void execute(String sql)
    {
        execute(sql, TEST_USER, TEST_PASS);
    }

    public void execute(String sql, String user, String password)
    {
        try (Connection connection = getConnectionFactory(user, password).openConnection(SESSION);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private ConnectionFactory getConnectionFactory()
    {
        return getConnectionFactory(getUsername(), getPassword());
    }

    private ConnectionFactory getConnectionFactory(String username, String password)
    {
        DriverConnectionFactory connectionFactory = new DriverConnectionFactory(
                new OracleDriver(),
                new BaseJdbcConfig().setConnectionUrl(getJdbcUrl()),
                StaticCredentialProvider.of(username, password));
        return new RetryingConnectionFactory(connectionFactory);
    }

    @Override
    public void close()
    {
        stop();
    }
}
