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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceHandleSource;
import io.trino.spi.exchange.ExchangeSourceHandleSource.ExchangeSourceHandleBatch;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;

public final class Exchanges
{
    private Exchanges() {}

    public static ListenableFuture<List<ExchangeSourceHandle>> getAllSourceHandles(ExchangeSourceHandleSource handleSource)
    {
        return new AbstractFuture<List<ExchangeSourceHandle>>()
        {
            private final ImmutableList.Builder<ExchangeSourceHandle> handles = ImmutableList.builder();
            private CompletableFuture<ExchangeSourceHandleBatch> nextBatchFuture;

            private synchronized AbstractFuture<List<ExchangeSourceHandle>> process()
            {
                if (isDone()) {
                    return this;
                }
                try {
                    checkState(nextBatchFuture == null || nextBatchFuture.isDone(), "nextBatchFuture is expected to be done");
                    nextBatchFuture = handleSource.getNextBatch();
                    nextBatchFuture.whenComplete((result, failure) -> {
                        if (failure != null) {
                            setException(failure);
                            handleSource.close();
                            return;
                        }
                        handles.addAll(result.handles());
                        if (result.lastBatch()) {
                            set(handles.build());
                            handleSource.close();
                            return;
                        }
                        process();
                    });
                }
                catch (Throwable t) {
                    setException(t);
                    handleSource.close();
                }
                return this;
            }

            @Override
            protected synchronized void interruptTask()
            {
                if (nextBatchFuture != null) {
                    nextBatchFuture.cancel(true);
                    nextBatchFuture = null;
                }
                handleSource.close();
            }
        }.process();
    }
}
