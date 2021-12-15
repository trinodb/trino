/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.saphana;

import com.starburstdata.presto.testing.testcontainers.SapHanaDockerInitializer;
import com.starburstdata.presto.testing.testcontainers.SapHanaJdbcContainer;
import io.airlift.log.Logger;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.Timeout;
import org.testcontainers.containers.GenericContainer;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static com.google.common.base.Throwables.getCausalChain;
import static com.starburstdata.presto.testing.TestContainersUtil.startOrReuse;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static java.time.temporal.ChronoUnit.MINUTES;

public final class TestingSapHanaServer
        implements Closeable
{
    private static final Logger log = Logger.get(TestingSapHanaServer.class);
    private static final RetryPolicy<TestingSapHanaServer> QUERY_EXECUTION_RETRY_POLICY = new RetryPolicy<TestingSapHanaServer>()
            .withBackoff(1, 5, ChronoUnit.SECONDS)
            .withMaxRetries(5)
            .handleIf(throwable -> getCausalChain(throwable).stream()
                    .anyMatch(SQLException.class::isInstance))
            .onRetry(event -> log.warn(
                    "Query failed on attempt %s, will retry. Exception: %s",
                    event.getAttemptCount(),
                    event.getLastFailure().getMessage()));
    private static final SapHanaDockerInitializer dockerInitializer = new SapHanaDockerInitializer(GenericContainer::addExposedPort);

    private final SapHanaJdbcContainer dockerContainer;
    private Closeable cleanup;

    public static TestingSapHanaServer create()
    {
        return Failsafe.with(QUERY_EXECUTION_RETRY_POLICY.copy(), Timeout.of(Duration.of(12, MINUTES)))
                .get(() -> {
                    TestingSapHanaServer server = new TestingSapHanaServer();
                    try {
                        server.start();
                        return server;
                    }
                    catch (Exception e) {
                        throw closeAllSuppress(e, server);
                    }
                });
    }

    private TestingSapHanaServer()
    {
        dockerContainer = new SapHanaJdbcContainer();
        dockerInitializer.apply(dockerContainer);
    }

    private void start()
    {
        cleanup = startOrReuse(dockerContainer);
    }

    @Override
    public void close()
    {
        try {
            cleanup.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void execute(String sql)
    {
        try (Connection connection = dockerContainer.createConnection("");
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException | RuntimeException e) {
            throw new RuntimeException("Failed to execute statement: " + sql, e);
        }
    }

    public void executeWithRetry(String sql)
    {
        Failsafe.with(QUERY_EXECUTION_RETRY_POLICY.copy())
                .run(() -> execute(sql));
    }

    public String getJdbcUrl()
    {
        return dockerContainer.getJdbcUrl();
    }

    public String getUser()
    {
        return dockerContainer.getUsername();
    }

    public String getPassword()
    {
        return dockerContainer.getPassword();
    }
}
