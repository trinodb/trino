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
package io.trino.plugin.exasol;

import io.trino.spi.connector.SourcePage;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

final class BufferedSourcePageQueue
{
    private final BlockingQueue<SourcePage> queue;
    private final AtomicReference<CompletableFuture<Void>> dataAvailableFuture = new AtomicReference<>(new CompletableFuture<>());
    private final Map<Integer, ProducerState> producerStates;

    BufferedSourcePageQueue(int producerCount)
    {
        queue = new LinkedBlockingQueue<>(Math.max(8, producerCount * 4));
        producerStates = IntStream.range(0, producerCount)
                .boxed()
                .collect(ConcurrentHashMap::new, (states, producerId) -> states.put(producerId, ProducerState.active()), ConcurrentHashMap::putAll);
    }

    void add(SourcePage page)
            throws InterruptedException
    {
        queue.put(page);
        signalDataAvailable();
    }

    void finish(int producerId)
    {
        producerStates.compute(producerId, (_, currentState) -> requireTrackedProducer(producerId, currentState).finish());
        signalDataAvailable();
    }

    void fail(int producerId, Throwable throwable)
    {
        producerStates.compute(producerId, (_, currentState) -> requireTrackedProducer(producerId, currentState).fail(throwable));
        signalDataAvailable();
    }

    Throwable getFailure()
    {
        return producerStates.values().stream()
                .map(ProducerState::failure)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    SourcePage poll()
    {
        SourcePage page = queue.poll();
        if (page != null) {
            signalDataAvailable();
        }
        return page;
    }

    int size()
    {
        return queue.size();
    }

    boolean isEmpty()
    {
        return queue.isEmpty();
    }

    CompletableFuture<?> isBlocked()
    {
        if (!queue.isEmpty() || getFailure() != null || allProducersFinished()) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> blocked = dataAvailableFuture.get();
        if (blocked.isDone()) {
            CompletableFuture<Void> newBlocked = new CompletableFuture<>();
            if (dataAvailableFuture.compareAndSet(blocked, newBlocked)) {
                blocked = newBlocked;
            }
            else {
                blocked = dataAvailableFuture.get();
            }
        }

        if (!queue.isEmpty() || getFailure() != null || allProducersFinished()) {
            blocked.complete(null);
            return CompletableFuture.completedFuture(null);
        }
        return blocked;
    }

    private boolean allProducersFinished()
    {
        return producerStates.values().stream().allMatch(ProducerState::isTerminal);
    }

    boolean isFinished()
    {
        boolean producersFinished = allProducersFinished();
        boolean queueEmpty = queue.isEmpty();
        boolean noFailure = getFailure() == null;
        boolean finished = queueEmpty && noFailure && producersFinished;
        return finished;
    }

    private void signalDataAvailable()
    {
        dataAvailableFuture.get().complete(null);
    }

    private static ProducerState requireTrackedProducer(int producerId, ProducerState currentState)
    {
        if (currentState == null) {
            throw new IllegalArgumentException("Unknown producer: " + producerId);
        }
        return currentState;
    }

    private record ProducerState(boolean finished, Throwable failure)
    {
        private static ProducerState active()
        {
            return new ProducerState(false, null);
        }

        private ProducerState finish()
        {
            return new ProducerState(true, failure);
        }

        private ProducerState fail(Throwable throwable)
        {
            return new ProducerState(true, throwable);
        }

        private boolean isTerminal()
        {
            return finished;
        }
    }
}
