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
package io.trino.server;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.jaxrs.AsyncResponseHandler;
import jakarta.servlet.AsyncContext;
import jakarta.servlet.AsyncEvent;
import jakarta.servlet.AsyncListener;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.TimeoutHandler;
import jakarta.ws.rs.core.Context;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.jaxrs.AsyncResponseHandler.bindAsyncResponse;
import static java.util.Objects.requireNonNull;

public class DisconnectionAwareAsyncResponse
        implements AsyncResponse
{
    // Guards against calling AsyncResponse methods when client is no longer interested in consuming a response
    private final AtomicBoolean terminated = new AtomicBoolean();

    private final AsyncContext asyncContext;
    private final AsyncResponse delegate;
    private final AtomicReference<ListenableFuture<?>> cancellableFuture = new AtomicReference<>(null);

    public DisconnectionAwareAsyncResponse(@Context HttpServletRequest request, AsyncResponse delegate)
    {
        requireNonNull(request, "request is null");
        requireNonNull(delegate, "delegate is null");
        verify(request.isAsyncStarted(), "AsyncContext is not started, did you forget @Suspended?");

        this.delegate = delegate;
        this.asyncContext = request.getAsyncContext();

        request.getAsyncContext().addListener(new AsyncListener()
        {
            @Override
            public void onComplete(AsyncEvent event) {}

            @Override
            public void onTimeout(AsyncEvent event)
            {
                terminate();
            }

            @Override
            public void onError(AsyncEvent event)
            {
                if (wasRequestTerminated(event.getThrowable())) {
                    terminate();
                }
            }

            @Override
            public void onStartAsync(AsyncEvent event) {}
        });
    }

    public DisconnectionAwareAsyncResponse withCancellableFuture(ListenableFuture<?> future)
    {
        checkState(cancellableFuture.compareAndSet(null, future), "Cancellable future already set");
        return this;
    }

    private void terminate()
    {
        if (terminated.compareAndSet(false, true)) {
            asyncContext.complete();
            ListenableFuture<?> future = cancellableFuture.getAndSet(null);
            if (future != null) {
                future.cancel(true);
            }
        }
    }

    @Override
    public boolean resume(Object response)
    {
        if (wasTerminated()) {
            return true;
        }

        if (terminated.compareAndSet(false, true)) {
            return delegate.resume(response);
        }

        return true;
    }

    @Override
    public boolean resume(Throwable response)
    {
        if (wasTerminated()) {
            return true;
        }
        if (terminated.compareAndSet(false, true)) {
            return delegate.resume(response);
        }
        return true;
    }

    @Override
    public boolean cancel()
    {
        if (wasTerminated()) {
            return true;
        }

        if (terminated.compareAndSet(false, true)) {
            return delegate.cancel();
        }
        return true;
    }

    @Override
    public boolean cancel(int retryAfter)
    {
        if (wasTerminated()) {
            return true;
        }
        if (terminated.compareAndSet(false, true)) {
            return delegate.cancel(retryAfter);
        }
        return true;
    }

    @Override
    public boolean cancel(Date retryAfter)
    {
        if (wasTerminated()) {
            return true;
        }
        if (terminated.compareAndSet(false, true)) {
            return delegate.cancel(retryAfter);
        }
        return true;
    }

    @Override
    public boolean isSuspended()
    {
        return delegate.isSuspended();
    }

    @Override
    public boolean isCancelled()
    {
        return delegate.isCancelled();
    }

    @Override
    public boolean isDone()
    {
        if (wasTerminated()) {
            return true;
        }
        return delegate.isDone();
    }

    @Override
    public boolean setTimeout(long time, TimeUnit unit)
    {
        return delegate.setTimeout(time, unit);
    }

    @Override
    public void setTimeoutHandler(TimeoutHandler handler)
    {
        delegate.setTimeoutHandler(asyncResponse -> {
            if (wasTerminated()) {
                return;
            }
            handler.handleTimeout(asyncResponse);
        });
    }

    @Override
    public Collection<Class<?>> register(Class<?> callback)
    {
        return delegate.register(callback);
    }

    @Override
    public Map<Class<?>, Collection<Class<?>>> register(Class<?> callback, Class<?>... callbacks)
    {
        return delegate.register(callback, callbacks);
    }

    @Override
    public Collection<Class<?>> register(Object callback)
    {
        return delegate.register(callback);
    }

    @Override
    public Map<Class<?>, Collection<Class<?>>> register(Object callback, Object... callbacks)
    {
        return delegate.register(callback, callbacks);
    }

    private boolean wasTerminated()
    {
        return terminated.get();
    }

    private static boolean wasRequestTerminated(Throwable throwable)
    {
        // Jetty's detected that client disconnected
        return throwable instanceof IOException;
    }

    public static AsyncResponseHandler bindDisconnectionAwareAsyncResponse(DisconnectionAwareAsyncResponse asyncResponse, ListenableFuture<?> futureResponse, Executor httpResponseExecutor)
    {
        return bindAsyncResponse(asyncResponse.withCancellableFuture(futureResponse), futureResponse, httpResponseExecutor);
    }
}
