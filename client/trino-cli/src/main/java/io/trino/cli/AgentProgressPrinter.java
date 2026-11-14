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
package io.trino.cli;

import io.trino.client.QueryStatusInfo;
import io.trino.client.StatementStats;

import java.io.Closeable;
import java.io.PrintStream;
import java.util.OptionalDouble;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Supplier;
import java.util.logging.Logger;

import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.FINE;

/**
 * Periodically prints a plain line about the progress of a query, for programs that read the
 * standard error of the CLI. Unlike {@link StatusPrinter} it does not need a terminal.
 */
final class AgentProgressPrinter
        implements Closeable
{
    private static final Logger log = Logger.getLogger(AgentProgressPrinter.class.getName());

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "agent-progress-printer");
        thread.setDaemon(true);
        return thread;
    });

    private final Supplier<QueryStatusInfo> status;
    private final PrintStream out;
    private final ScheduledFuture<?> task;

    /**
     * Starts printing after the first interval, which is also the time between lines.
     */
    AgentProgressPrinter(Supplier<QueryStatusInfo> status, PrintStream out, long intervalMillis)
    {
        this.status = requireNonNull(status, "status is null");
        this.out = requireNonNull(out, "out is null");
        this.task = executor.scheduleWithFixedDelay(this::print, intervalMillis, intervalMillis, MILLISECONDS);
    }

    @Override
    public void close()
    {
        task.cancel(false);
        executor.shutdownNow();
        try {
            executor.awaitTermination(1, SECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static String describe(QueryStatusInfo info)
    {
        StatementStats stats = info.getStats();
        StringBuilder line = new StringBuilder("progress: ")
                .append(format("query_id=%s state=%s", info.getId(), stats.getState()));
        OptionalDouble percentage = stats.getProgressPercentage();
        if (percentage.isPresent()) {
            // cap at 99%, because a query that is still running should not say it is done
            line.append(format(" progress=%d%%", (int) min(99, percentage.orElseThrow())));
        }
        line.append(format(
                " elapsed=%ds rows=%d bytes=%d",
                stats.getElapsedTimeMillis() / 1000,
                stats.getProcessedRows(),
                stats.getProcessedBytes()));
        return line.toString();
    }

    private void print()
    {
        try {
            out.println(describe(status.get()));
        }
        catch (RuntimeException e) {
            // never let the progress end the query, and keep trying
            log.log(FINE, "error printing progress", e);
        }
    }
}
