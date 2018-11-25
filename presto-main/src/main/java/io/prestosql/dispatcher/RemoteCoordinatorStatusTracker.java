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
package io.prestosql.dispatcher;

import com.google.common.collect.ObjectArrays;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.event.client.ServiceUnavailableException;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;

import java.io.EOFException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RemoteCoordinatorStatusTracker
{
    private static final Logger log = Logger.get(RemoteCoordinatorStatusTracker.class);

    private final Consumer<CoordinatorStatus> coordinatorStatusListener;
    private final ScheduledExecutorService updateScheduledExecutor;
    private final Executor callbackExecutor;

    private final Duration updateInterval;

    private final RemoteCoordinatorClient remoteCoordinatorClient;

    @GuardedBy("this")
    private boolean running;

    @GuardedBy("this")
    private ScheduledFuture<?> scheduledFuture;

    @GuardedBy("this")
    private ListenableFuture<CoordinatorStatus> statusFuture;

    public RemoteCoordinatorStatusTracker(
            RemoteCoordinatorClient remoteCoordinatorClient,
            Consumer<CoordinatorStatus> coordinatorStatusListener,
            Duration updateInterval,
            HttpClient httpClient,
            Executor callbackExecutor,
            ScheduledExecutorService updateScheduledExecutor,
            JsonCodec<CoordinatorStatus> coordinatorStatusCodec)
    {
        this.remoteCoordinatorClient = remoteCoordinatorClient;
        this.coordinatorStatusListener = requireNonNull(coordinatorStatusListener, "remoteQueryInfoListener is null");
        this.updateScheduledExecutor = requireNonNull(updateScheduledExecutor, "updateScheduledExecutor is null");
        this.callbackExecutor = requireNonNull(callbackExecutor, "executor is null");
        this.updateInterval = requireNonNull(updateInterval, "updateInterval is null");
    }

    public synchronized void start()
    {
        if (running) {
            // already running
            return;
        }
        running = true;
        scheduleUpdate();
    }

    public synchronized void stop()
    {
        running = false;
        if (statusFuture != null) {
            statusFuture.cancel(true);
            statusFuture = null;
        }
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
    }

    private synchronized void scheduleUpdate()
    {
        scheduledFuture = updateScheduledExecutor.scheduleWithFixedDelay(this::sendNextRequest, 0, updateInterval.toMillis(), MILLISECONDS);
    }

    private synchronized void sendNextRequest()
    {
        if (!running) {
            return;
        }

        // if the previous request didn't finish, so cancel it
        if (statusFuture != null) {
            statusFuture.cancel(true);
            statusFuture = null;
        }

        statusFuture = remoteCoordinatorClient.getStatus(updateInterval);
        Futures.addCallback(statusFuture, new FutureCallback<CoordinatorStatus>()
        {
            @Override
            public void onSuccess(CoordinatorStatus coordinatorStatus)
            {
                coordinatorStatusListener.accept(coordinatorStatus);
            }

            @Override
            public void onFailure(Throwable t)
            {
                if (t instanceof CancellationException) {
                    return;
                }
                logError(t, "Error updating coordinator state at %s", remoteCoordinatorClient.getCoordinatorLocation().getUri("https"));
            }
        }, callbackExecutor);
    }

    private static void logError(Throwable t, String format, Object... args)
    {
        if (isExpectedError(t)) {
            log.error(format + ": %s", ObjectArrays.concat(args, t));
        }
        else {
            log.error(t, format, args);
        }
    }

    private static boolean isExpectedError(Throwable t)
    {
        while (t != null) {
            if ((t instanceof SocketException) ||
                    (t instanceof SocketTimeoutException) ||
                    (t instanceof EOFException) ||
                    (t instanceof TimeoutException) ||
                    (t instanceof ServiceUnavailableException)) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }
}
