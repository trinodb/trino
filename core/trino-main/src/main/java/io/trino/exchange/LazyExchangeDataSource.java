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
package io.trino.exchange;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.slice.Slice;
import io.trino.execution.TaskFailureListener;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.DirectExchangeClient;
import io.trino.operator.DirectExchangeClientSupplier;
import io.trino.operator.OperatorInfo;
import io.trino.operator.RetryPolicy;
import io.trino.spi.QueryId;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeManager;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static java.util.Objects.requireNonNull;

public class LazyExchangeDataSource
        implements ExchangeDataSource
{
    private final QueryId queryId;
    private final ExchangeId exchangeId;
    private final DirectExchangeClientSupplier directExchangeClientSupplier;
    private final LocalMemoryContext systemMemoryContext;
    private final TaskFailureListener taskFailureListener;
    private final RetryPolicy retryPolicy;
    private final ExchangeManagerRegistry exchangeManagerRegistry;

    private final SettableFuture<Void> initializationFuture = SettableFuture.create();
    private final AtomicReference<ExchangeDataSource> delegate = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    public LazyExchangeDataSource(
            QueryId queryId,
            ExchangeId exchangeId,
            DirectExchangeClientSupplier directExchangeClientSupplier,
            LocalMemoryContext systemMemoryContext,
            TaskFailureListener taskFailureListener,
            RetryPolicy retryPolicy,
            ExchangeManagerRegistry exchangeManagerRegistry)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.exchangeId = requireNonNull(exchangeId, "exchangeId is null");
        this.directExchangeClientSupplier = requireNonNull(directExchangeClientSupplier, "directExchangeClientSupplier is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        this.taskFailureListener = requireNonNull(taskFailureListener, "taskFailureListener is null");
        this.retryPolicy = requireNonNull(retryPolicy, "retryPolicy is null");
        this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
    }

    @Override
    public Slice pollPage()
    {
        ExchangeDataSource dataSource = delegate.get();
        if (dataSource == null) {
            return null;
        }
        return dataSource.pollPage();
    }

    @Override
    public boolean isFinished()
    {
        if (closed.get()) {
            return true;
        }
        ExchangeDataSource dataSource = delegate.get();
        if (dataSource == null) {
            return false;
        }
        return dataSource.isFinished();
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        if (closed.get()) {
            return immediateVoidFuture();
        }
        if (!initializationFuture.isDone()) {
            return nonCancellationPropagating(initializationFuture);
        }
        ExchangeDataSource dataSource = delegate.get();
        if (dataSource == null) {
            return immediateVoidFuture();
        }
        return dataSource.isBlocked();
    }

    @Override
    public void addInput(ExchangeInput input)
    {
        boolean initialized = false;
        synchronized (this) {
            if (closed.get()) {
                return;
            }
            ExchangeDataSource dataSource = delegate.get();
            if (dataSource == null) {
                if (input instanceof DirectExchangeInput) {
                    DirectExchangeClient client = directExchangeClientSupplier.get(queryId, exchangeId, systemMemoryContext, taskFailureListener, retryPolicy);
                    dataSource = new DirectExchangeDataSource(client);
                }
                else if (input instanceof SpoolingExchangeInput) {
                    ExchangeManager exchangeManager = exchangeManagerRegistry.getExchangeManager();
                    dataSource = new SpoolingExchangeDataSource(exchangeManager.createSource(), systemMemoryContext);
                }
                else {
                    throw new IllegalArgumentException("Unexpected input: " + input);
                }
                delegate.set(dataSource);
                initialized = true;
            }
            dataSource.addInput(input);
        }

        if (initialized) {
            initializationFuture.set(null);
        }
    }

    @Override
    public synchronized void noMoreInputs()
    {
        if (closed.get()) {
            return;
        }
        ExchangeDataSource dataSource = delegate.get();
        if (dataSource != null) {
            dataSource.noMoreInputs();
        }
        else {
            // to unblock when no splits are provided (and delegate hasn't been created)
            close();
        }
    }

    @Override
    public OperatorInfo getInfo()
    {
        ExchangeDataSource dataSource = delegate.get();
        if (dataSource == null) {
            return null;
        }
        return dataSource.getInfo();
    }

    @Override
    public void close()
    {
        synchronized (this) {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            ExchangeDataSource dataSource = delegate.get();
            if (dataSource != null) {
                dataSource.close();
            }
        }
        initializationFuture.set(null);
    }
}
