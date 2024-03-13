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

import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForwardingConnection;
import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class StarburstOracleKeepAliveConnectionFactory
        implements ConnectionFactory
{
    private static final Logger log = Logger.get(StarburstOracleKeepAliveConnectionFactory.class);
    private final ConnectionFactory delegate;
    private final Duration interval;
    private final ScheduledExecutorService executorService;

    public StarburstOracleKeepAliveConnectionFactory(ConnectionFactory delegate, Duration interval)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.interval = requireNonNull(interval, "interval is null");
        this.executorService = Executors.newSingleThreadScheduledExecutor(threadsNamed("oracle-keep-alive-%d"));
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        return new KeepAliveConnection(delegate.openConnection(session), executorService, interval);
    }

    @Override
    public void close()
            throws SQLException
    {
        try {
            delegate.close();
        }
        finally {
            executorService.shutdownNow();
        }
    }

    public static class KeepAliveConnection
            extends ForwardingConnection
    {
        private static final int CONSECUTIVE_RETRY_FAILURES_LIMIT = 5;
        private int retryCount;
        private ScheduledFuture<?> future;
        private final Connection delegate;

        public KeepAliveConnection(Connection delegate, ScheduledExecutorService executorService, Duration interval)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.future = executorService.scheduleAtFixedRate(this::keepConnectionAlive, interval.toMillis(), interval.toMillis(), MILLISECONDS);
        }

        private void keepConnectionAlive()
        {
            try {
                // Validate the connection to check whether it's still alive
                delegate().isValid(0);
                retryCount = 0;
            }
            catch (SQLException e) {
                retryCount++;
                log.warn("Could not validate connection: %s, try %d", e.getMessage(), retryCount);
                if (retryCount >= CONSECUTIVE_RETRY_FAILURES_LIMIT) {
                    future.cancel(true);
                    log.error("Aborting keep alive checks");
                }
            }
        }

        @Override
        public void close()
                throws SQLException
        {
            synchronized (this) {
                if (future != null) {
                    future.cancel(true);
                    future = null;
                }
            }

            delegate().close();
        }

        @Override
        protected Connection delegate()
                throws SQLException
        {
            return delegate;
        }
    }
}
