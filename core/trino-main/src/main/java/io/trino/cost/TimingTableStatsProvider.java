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
package io.trino.cost;

import io.airlift.concurrent.SetThreadName;
import io.trino.Session;
import io.trino.metadata.TableHandle;
import io.trino.spi.statistics.TableStatistics;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class TimingTableStatsProvider
        implements TableStatsProvider
{
    private final TableStatsProvider delegate;
    private final Session session;
    private final ExecutorService executor;
    private final long timeoutMillis;

    public TimingTableStatsProvider(TableStatsProvider delegate, Session session, ExecutorService executor, long timeoutMillis)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.session = requireNonNull(session, "session is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public TableStatistics getTableStatistics(TableHandle tableHandle)
    {
        Future<TableStatistics> future = executor.submit(() -> {
            try (SetThreadName ignored = new SetThreadName("Query-%s", session.getQueryId())) {
                return delegate.getTableStatistics(tableHandle);
            }
        });
        try {
            return future.get(timeoutMillis, MILLISECONDS);
        }
        catch (InterruptedException e) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted", e);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        catch (TimeoutException e) {
            future.cancel(true);
            return TableStatistics.empty();
        }
    }
}
