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
package io.trino.plugin.jdbc.keepalive;

import io.airlift.units.Duration;
import io.trino.plugin.jdbc.ForwardingConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class KeepAliveConnection
        extends ForwardingConnection
{
    private final Connection delegate;
    private ScheduledFuture<?> future;

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
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
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
