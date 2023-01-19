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

import com.google.common.base.Joiner;
import com.google.common.io.Files;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.RetryingConnectionFactory;
import io.trino.plugin.jdbc.credential.StaticCredentialProvider;
import io.trino.testing.ResourcePresence;
import oracle.jdbc.OracleDriver;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.utility.MountableFile;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.temporal.ChronoUnit;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.lang.String.format;

public class TestingOracleServer
        implements Closeable
{
    private static final Logger log = Logger.get(TestingOracleServer.class);

    private static final RetryPolicy<Object> CONTAINER_RETRY_POLICY = RetryPolicy.builder()
            .withBackoff(1, 5, ChronoUnit.SECONDS)
            .withMaxAttempts(5)
            .onRetry(event -> log.warn(
                    "Container initialization failed on attempt %s, will retry. Exception: %s",
                    event.getAttemptCount(),
                    event.getLastException().getMessage()))
            .build();

    private static final String TEST_TABLESPACE = "trino_test";

    public static final String TEST_USER = "trino_test";
    public static final String TEST_SCHEMA = TEST_USER; // schema and user is the same thing in Oracle
    public static final String TEST_PASS = "trino_test_password";

    private final OracleContainer container;

    public TestingOracleServer()
    {
        container = Failsafe.with(CONTAINER_RETRY_POLICY).get(this::createContainer);
    }

    private OracleContainer createContainer()
    {
        OracleContainer container = new OracleContainer("gvenzl/oracle-xe:11.2.0.2-full")
                .withCopyFileToContainer(MountableFile.forClasspathResource("init.sql"), "/container-entrypoint-initdb.d/01-init.sql")
                .withCopyFileToContainer(MountableFile.forClasspathResource("restart.sh"), "/container-entrypoint-initdb.d/02-restart.sh")
                .withCopyFileToContainer(MountableFile.forHostPath(createConfigureScript()), "/container-entrypoint-initdb.d/03-create-users.sql")
                .usingSid();
        container.start();
        return container;
    }

    private Path createConfigureScript()
    {
        try {
            File tempFile = File.createTempFile("init-", ".sql");

            Files.write(Joiner.on("\n").join(
                    format("CREATE TABLESPACE %s DATAFILE 'test_db.dat' SIZE 100M ONLINE;", TEST_TABLESPACE),
                    format("CREATE USER %s IDENTIFIED BY %s DEFAULT TABLESPACE %s;", TEST_USER, TEST_PASS, TEST_TABLESPACE),
                    format("GRANT UNLIMITED TABLESPACE TO %s;", TEST_USER),
                    format("GRANT CREATE SESSION TO %s;", TEST_USER),
                    format("GRANT ALL PRIVILEGES TO %s;", TEST_USER)).getBytes(StandardCharsets.UTF_8), tempFile);

            return tempFile.toPath();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public String getJdbcUrl()
    {
        return container.getJdbcUrl();
    }

    public void execute(String sql)
    {
        execute(sql, TEST_USER, TEST_PASS);
    }

    public void execute(String sql, String user, String password)
    {
        try (Connection connection = getConnectionFactory(getJdbcUrl(), user, password).openConnection(SESSION);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private ConnectionFactory getConnectionFactory(String connectionUrl, String username, String password)
    {
        DriverConnectionFactory connectionFactory = new DriverConnectionFactory(
                new OracleDriver(),
                new BaseJdbcConfig().setConnectionUrl(connectionUrl),
                StaticCredentialProvider.of(username, password));
        return new RetryingConnectionFactory(connectionFactory);
    }

    @Override
    public void close()
    {
        container.stop();
    }

    @ResourcePresence
    public boolean isRunning()
    {
        return container.getContainerId() != null;
    }
}
