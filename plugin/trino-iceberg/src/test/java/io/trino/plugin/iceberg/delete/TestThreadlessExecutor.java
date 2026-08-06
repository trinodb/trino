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
package io.trino.plugin.iceberg.delete;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestThreadlessExecutor
{
    // in seconds
    private static final long TIMEOUT = 5;

    @Test
    void testTasksRunOnlyInRunPending()
    {
        ThreadlessExecutor executor = new ThreadlessExecutor();
        List<String> ran = new ArrayList<>();

        executor.execute(() -> ran.add("first"));
        executor.execute(() -> ran.add("second"));
        assertThat(ran).isEmpty();

        executor.runPending();
        assertThat(ran).containsExactly("first", "second");
    }

    @Test
    void testTasksSubmittedFromAnotherThreadRunOnTheCallingThread()
            throws Exception
    {
        ThreadlessExecutor executor = new ThreadlessExecutor();
        AtomicReference<Thread> taskThread = new AtomicReference<>();

        Thread submitter = new Thread(() -> executor.execute(() -> taskThread.set(Thread.currentThread())));
        submitter.start();
        submitter.join(SECONDS.toMillis(TIMEOUT));
        assertThat(taskThread.get()).isNull();

        executor.runPending();
        assertThat(taskThread.get()).isSameAs(Thread.currentThread());
    }

    @Test
    void testTaskAvailableCompletesWhenTaskIsQueued()
            throws Exception
    {
        ThreadlessExecutor executor = new ThreadlessExecutor();

        ListenableFuture<Void> taskAvailable = executor.taskAvailable();
        assertThat(taskAvailable.isDone()).isFalse();

        Thread submitter = new Thread(() -> executor.execute(() -> {}));
        submitter.start();
        taskAvailable.get(TIMEOUT, SECONDS);

        // done until the queued task runs
        assertThat(executor.taskAvailable().isDone()).isTrue();
        executor.runPending();
        assertThat(executor.taskAvailable().isDone()).isFalse();
    }

    @Test
    void testTasksQueuedWhileRunningRunInTheSameCall()
    {
        ThreadlessExecutor executor = new ThreadlessExecutor();
        List<String> ran = new ArrayList<>();

        executor.execute(() -> {
            ran.add("outer");
            executor.execute(() -> ran.add("inner"));
        });
        executor.runPending();

        assertThat(ran).containsExactly("outer", "inner");
        assertThat(executor.taskAvailable().isDone()).isFalse();
    }

    @Test
    void testFailingTaskLeavesLaterTasksQueued()
    {
        ThreadlessExecutor executor = new ThreadlessExecutor();
        List<String> ran = new ArrayList<>();

        executor.execute(() -> {
            throw new RuntimeException("task failed");
        });
        executor.execute(() -> ran.add("later"));

        assertThatThrownBy(executor::runPending).hasMessage("task failed");
        assertThat(ran).isEmpty();

        executor.runPending();
        assertThat(ran).containsExactly("later");
    }

    @Test
    void testCloseDropsQueuedAndLaterTasks()
    {
        ThreadlessExecutor executor = new ThreadlessExecutor();
        List<String> ran = new ArrayList<>();
        ListenableFuture<Void> taskAvailable = executor.taskAvailable();

        executor.execute(() -> ran.add("queued"));
        executor.close();
        executor.execute(() -> ran.add("after close"));
        executor.runPending();

        assertThat(ran).isEmpty();
        // a split waiting for a task is released by close
        assertThat(taskAvailable.isDone()).isTrue();
        assertThat(executor.taskAvailable().isDone()).isTrue();
    }

    @Test
    void testCloseReleasesWaiter()
    {
        ThreadlessExecutor executor = new ThreadlessExecutor();
        List<ListenableFuture<Void>> waiters = ImmutableList.of(executor.taskAvailable(), executor.taskAvailable());

        executor.close();

        assertThat(waiters).allMatch(ListenableFuture::isDone);
    }
}
