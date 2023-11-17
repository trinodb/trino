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
import io.trino.client.NodeVersion;
import io.trino.execution.MockRemoteTaskFactory;
import io.trino.execution.NodeTaskMap.PartitionedSplitCountTracker;
import io.trino.execution.RemoteTask;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.metadata.InternalNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestFixedCountScheduler
{
    private ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "executor-%s"));
    private ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "scheduledExecutor-%s"));
    private final MockRemoteTaskFactory taskFactory;

    public TestFixedCountScheduler()
    {
        taskFactory = new MockRemoteTaskFactory(executor, scheduledExecutor);
    }

    @AfterAll
    public void destroyExecutor()
    {
        executor.shutdownNow();
        executor = null;
        scheduledExecutor.shutdown();
        scheduledExecutor = null;
    }

    @Test
    public void testSingleNode()
    {
        FixedCountScheduler nodeScheduler = new FixedCountScheduler(
                (node, partition) -> Optional.of(taskFactory.createTableScanTask(
                        new TaskId(new StageId("test", 1), 1, 0),
                        node, ImmutableList.of(),
                        new PartitionedSplitCountTracker(delta -> {}))),
                generateRandomNodes(1));

        ScheduleResult result = nodeScheduler.schedule();
        assertThat(result.isFinished()).isTrue();
        assertThat(result.getBlocked().isDone()).isTrue();
        assertThat(result.getNewTasks().size()).isEqualTo(1);
        assertThat(result.getNewTasks().iterator().next().getNodeId().equals("other 0")).isTrue();
    }

    @Test
    public void testMultipleNodes()
    {
        FixedCountScheduler nodeScheduler = new FixedCountScheduler(
                (node, partition) -> Optional.of(taskFactory.createTableScanTask(
                        new TaskId(new StageId("test", 1), 1, 0),
                        node, ImmutableList.of(),
                        new PartitionedSplitCountTracker(delta -> {}))),
                generateRandomNodes(5));

        ScheduleResult result = nodeScheduler.schedule();
        assertThat(result.isFinished()).isTrue();
        assertThat(result.getBlocked().isDone()).isTrue();
        assertThat(result.getNewTasks().size()).isEqualTo(5);
        assertThat(result.getNewTasks().stream().map(RemoteTask::getNodeId).collect(toImmutableSet()).size()).isEqualTo(5);
    }

    private static List<InternalNode> generateRandomNodes(int count)
    {
        return IntStream.range(0, count)
                .mapToObj(i -> new InternalNode("other " + i, URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false))
                .collect(toImmutableList());
    }
}
