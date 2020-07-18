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
package io.prestosql.plugin.jdbc;

import io.airlift.units.Duration;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.FailsafeException;
import net.jodah.failsafe.RetryPolicy;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.temporal.ChronoUnit;

import static java.util.Objects.requireNonNull;

public class RetryableConnectionFactory
        implements ConnectionFactory
{
    private final ConnectionFactory delegate;
    private final long delay;
    private final long maxDelay;
    private final int maxRetries;
    private final double delayFactor;

    @Inject
    public RetryableConnectionFactory(@ForBaseJdbc ConnectionFactory delegate, RetryJdbcConfig config)
    {
        this(delegate, config.getDelay(), config.getMaxDelay(), config.getMaxRetries(), config.getDelayFactor());
    }

    public RetryableConnectionFactory(ConnectionFactory delegate, Duration delay, Duration maxDelay, int maxRetries, double delayFactor)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.delay = requireNonNull(delay).toMillis();
        this.maxDelay = requireNonNull(maxDelay).toMillis();
        this.maxRetries = maxRetries;
        this.delayFactor = delayFactor;
    }

    @Override
    public Connection openConnection(JdbcIdentity identity) throws SQLException
    {
        RetryPolicy<Object> retryPolicy = new RetryPolicy<>()
                .handle(SQLException.class)
                .withBackoff(delay, maxDelay, ChronoUnit.MILLIS, delayFactor)
                .withMaxRetries(maxRetries)
                .withJitter(0.2);

        try {
            return Failsafe.with(retryPolicy).get(() -> delegate.openConnection(identity));
        }
        catch (FailsafeException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void close()
            throws SQLException
    {
        delegate.close();
    }
}
