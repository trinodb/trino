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
package io.trino.plugin.jdbc;

import com.google.common.base.Throwables;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedRunnable;
import dev.failsafe.function.CheckedSupplier;
import io.trino.plugin.jdbc.jmx.StatisticsAwareConnectionFactory;
import io.trino.plugin.jdbc.jmx.StatisticsAwareJdbcClient;
import io.trino.spi.TrinoException;

import java.sql.SQLTransientException;
import java.time.Duration;
import java.util.Set;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.SECONDS;

public class RetryingModule
        extends AbstractModule
{
    @Override
    public void configure()
    {
        bind(ConnectionFactory.class).annotatedWith(ForRetrying.class).to(Key.get(StatisticsAwareConnectionFactory.class)).in(Scopes.SINGLETON);
        bind(RetryingConnectionFactory.class).in(Scopes.SINGLETON);
        bind(JdbcClient.class).annotatedWith(ForRetrying.class).to(Key.get(StatisticsAwareJdbcClient.class)).in(Scopes.SINGLETON);
        bind(RetryingJdbcClient.class).in(Scopes.SINGLETON);
        newSetBinder(binder(), RetryStrategy.class).addBinding().to(OnSqlTransientExceptionRetryStrategy.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public RetryPolicy<Object> createRetryPolicy(Set<RetryStrategy> retryStrategies)
    {
        return RetryPolicy.builder()
                .withMaxDuration(Duration.of(30, SECONDS))
                .withMaxAttempts(5)
                .withBackoff(50, 5_000, MILLIS, 4)
                .handleIf(throwable -> isExceptionRecoverable(retryStrategies, throwable))
                .abortOn(TrinoException.class)
                .build();
    }

    public static <T> T retry(RetryPolicy<Object> policy, CheckedSupplier<T> supplier)
    {
        return Failsafe.with(policy)
                .get(supplier);
    }

    public static void retry(RetryPolicy<Object> policy, CheckedRunnable runnable)
    {
        Failsafe.with(policy)
                .run(runnable);
    }

    private static boolean isExceptionRecoverable(Set<RetryStrategy> retryStrategies, Throwable throwable)
    {
        return retryStrategies.stream()
                .anyMatch(retryStrategy -> retryStrategy.isExceptionRecoverable(throwable));
    }

    private static class OnSqlTransientExceptionRetryStrategy
            implements RetryStrategy
    {
        @Override
        public boolean isExceptionRecoverable(Throwable exception)
        {
            return Throwables.getCausalChain(exception).stream()
                    .anyMatch(SQLTransientException.class::isInstance);
        }
    }
}
