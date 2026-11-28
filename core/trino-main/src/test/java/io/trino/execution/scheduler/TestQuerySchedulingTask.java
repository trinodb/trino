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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.concurrent.MoreFutures.getDone;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static org.assertj.core.api.Assertions.assertThat;

public class TestQuerySchedulingTask
{
    @Test
    public void testStageWakeupsCoalesce()
    {
        Queue<Runnable> workers = new ArrayDeque<>();
        SettableFuture<Void> firstStage = SettableFuture.create();
        SettableFuture<Void> secondStage = SettableFuture.create();
        AtomicInteger passes = new AtomicInteger();
        List<Optional<Throwable>> finishes = new ArrayList<>();
        ListenableFuture<Void> completion = QuerySchedulingTask.submit(workers::add, () -> {
            if (passes.incrementAndGet() == 1) {
                return Optional.of(whenAnyComplete(ImmutableList.of(firstStage, secondStage)));
            }
            return Optional.empty();
        }, finishes::add);

        workers.remove().run();
        assertThat(workers).isEmpty();
        assertThat(finishes).isEmpty();

        secondStage.set(null);
        firstStage.set(null);
        assertThat(workers).hasSize(1);
        workers.remove().run();
        assertThat(workers).isEmpty();
        assertThat(passes.get()).isEqualTo(2);
        assertThat(finishes).containsExactly(Optional.empty());
        assertThat(getDone(completion)).isNull();
    }

    @Test
    public void testShutdownWhileWaiting()
    {
        Queue<Runnable> workers = new ArrayDeque<>();
        SettableFuture<Void> shutdown = SettableFuture.create();
        List<Optional<Throwable>> finishes = new ArrayList<>();
        RejectedExecutionException rejection = new RejectedExecutionException();
        ListenableFuture<Void> completion = QuerySchedulingTask.submit(task -> {
            if (shutdown.isDone()) {
                throw rejection;
            }
            workers.add(task);
        }, () -> Optional.of(shutdown), finishes::add);

        workers.remove().run();
        shutdown.set(null);
        assertThat(finishes).containsExactly(Optional.of(rejection));
        assertThat(getDone(completion)).isNull();
        assertThat(workers).isEmpty();
    }
}
