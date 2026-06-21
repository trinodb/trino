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

import io.trino.spi.Page;
import io.trino.spi.connector.SourcePage;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestBufferedSourcePageQueue
{
    @Test
    void testUnblocksWhenPageIsAdded()
            throws InterruptedException
    {
        BufferedSourcePageQueue queue = new BufferedSourcePageQueue(1);
        CompletableFuture<?> blocked = queue.isBlocked();
        SourcePage page = SourcePage.create(new Page(1));

        assertThat(blocked.isDone()).isFalse();

        queue.add(page);

        assertThat(blocked.isDone()).isTrue();
        assertThat(queue.poll()).isSameAs(page);
        assertThat(queue.isFinished()).isFalse();
    }

    @Test
    void testReportsImmediateAvailabilityWhenQueueHasData()
            throws InterruptedException
    {
        BufferedSourcePageQueue queue = new BufferedSourcePageQueue(1);
        SourcePage page = SourcePage.create(new Page(1));

        assertThat(queue.size()).isZero();
        assertThat(queue.isEmpty()).isTrue();

        queue.add(page);

        assertThat(queue.size()).isEqualTo(1);
        assertThat(queue.isEmpty()).isFalse();
        assertThat(queue.isBlocked().isDone()).isTrue();
    }

    @Test
    void testAddBlocksWhenQueueIsFull()
            throws Exception
    {
        BufferedSourcePageQueue queue = new BufferedSourcePageQueue(1);
        int queueCapacity = 8;
        for (int i = 0; i < queueCapacity; i++) {
            queue.add(SourcePage.create(new Page(1)));
        }
        SourcePage nextPage = SourcePage.create(new Page(1));

        try (var executor = Executors.newSingleThreadExecutor()) {
            Future<?> blockedAdd = executor.submit(() -> {
                try {
                    queue.add(nextPage);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });

            TimeUnit.MILLISECONDS.sleep(100);
            assertThat(blockedAdd.isDone()).isFalse();

            assertThat(queue.poll()).isNotNull();
            blockedAdd.get();

            while (queue.size() > 1) {
                assertThat(queue.poll()).isNotNull();
            }
            assertThat(queue.poll()).isSameAs(nextPage);
        }
    }

    @Test
    void testFinishesAfterAllProducersCompleteAndQueueIsDrained()
            throws InterruptedException
    {
        BufferedSourcePageQueue queue = new BufferedSourcePageQueue(2);
        SourcePage page = SourcePage.create(new Page(1));

        queue.add(page);
        queue.finish(0);
        queue.finish(1);

        assertThat(queue.isFinished()).isFalse();
        assertThat(queue.poll()).isSameAs(page);
        assertThat(queue.isFinished()).isTrue();
        assertThat(queue.isBlocked().isDone()).isTrue();
    }

    @Test
    void testBlockedFutureIsRefreshedAfterPreviousFutureCompletes()
            throws InterruptedException
    {
        BufferedSourcePageQueue queue = new BufferedSourcePageQueue(1);
        CompletableFuture<?> initialBlocked = queue.isBlocked();

        queue.add(SourcePage.create(new Page(1)));
        queue.poll();

        CompletableFuture<?> refreshedBlocked = queue.isBlocked();

        assertThat(initialBlocked.isDone()).isTrue();
        assertThat(refreshedBlocked).isNotSameAs(initialBlocked);
        assertThat(refreshedBlocked.isDone()).isFalse();
    }

    @Test
    void testIsBlockedReturnsCompletedFutureWhenAllProducersFinished()
    {
        BufferedSourcePageQueue queue = new BufferedSourcePageQueue(1);

        queue.finish(0);

        assertThat(queue.isBlocked().isDone()).isTrue();
        assertThat(queue.isFinished()).isTrue();
    }

    @Test
    void testFinishesWhenNoResultPageIsProduced()
    {
        BufferedSourcePageQueue queue = new BufferedSourcePageQueue(1);

        queue.finish(0);

        assertThat(queue.poll()).isNull();
        assertThat(queue.isEmpty()).isTrue();
        assertThat(queue.isFinished()).isTrue();
    }

    @Test
    void testFailureIsReportedAndUnblocksConsumers()
    {
        BufferedSourcePageQueue queue = new BufferedSourcePageQueue(1);
        CompletableFuture<?> blocked = queue.isBlocked();
        RuntimeException failure = new RuntimeException("boom");

        queue.fail(0, failure);

        assertThat(blocked.isDone()).isTrue();
        assertThat(queue.getFailure()).isSameAs(failure);
        assertThat(queue.isFinished()).isFalse();
        assertThat(queue.poll()).isNull();
    }

    @Test
    void testRejectsUnknownProducer()
    {
        BufferedSourcePageQueue queue = new BufferedSourcePageQueue(1);

        assertThatThrownBy(() -> queue.finish(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unknown producer: 1");
    }
}
