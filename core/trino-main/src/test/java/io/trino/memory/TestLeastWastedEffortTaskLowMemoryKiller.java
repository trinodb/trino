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

package io.trino.memory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.stats.TDigest;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskState;
import io.trino.execution.TaskStatus;
import io.trino.execution.buffer.BufferState;
import io.trino.execution.buffer.OutputBufferInfo;
import io.trino.execution.buffer.OutputBufferStatus;
import io.trino.operator.TaskStats;
import io.trino.plugin.base.metrics.TDigestHistogram;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static io.trino.memory.LowMemoryKillerTestingUtils.taskId;
import static io.trino.memory.LowMemoryKillerTestingUtils.toNodeMemoryInfoList;
import static io.trino.memory.LowMemoryKillerTestingUtils.toRunningQueryInfoList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public class TestLeastWastedEffortTaskLowMemoryKiller
{
    private final LowMemoryKiller lowMemoryKiller = new LeastWastedEffortTaskLowMemoryKiller();

    @Test
    public void testMemoryPoolHasNoReservation()
    {
        int memoryPool = 12;
        Map<String, Map<String, Long>> queries = ImmutableMap.of(
                "q_1",
                ImmutableMap.of("n1", 0L, "n2", 0L, "n3", 0L, "n4", 0L, "n5", 0L));

        assertEquals(
                lowMemoryKiller.chooseTargetToKill(
                        toRunningQueryInfoList(queries),
                        toNodeMemoryInfoList(memoryPool, queries)),
                Optional.empty());
    }

    @Test
    public void testMemoryPoolNotBlocked()
    {
        int memoryPool = 12;
        Map<String, Map<String, Long>> queries = ImmutableMap.<String, Map<String, Long>>builder()
                .put("q_1", ImmutableMap.of("n1", 0L, "n2", 6L, "n3", 0L, "n4", 0L, "n5", 0L))
                .put("q_2", ImmutableMap.of("n1", 3L, "n2", 5L, "n3", 2L, "n4", 4L, "n5", 0L))
                .buildOrThrow();
        assertEquals(
                lowMemoryKiller.chooseTargetToKill(
                        toRunningQueryInfoList(queries),
                        toNodeMemoryInfoList(memoryPool, queries)),
                Optional.empty());
    }

    @Test
    public void testWillNotKillTaskForQueryWithoutTaskRetriesEnabled()
    {
        int memoryPool = 5;
        Map<String, Map<String, Long>> queries = ImmutableMap.<String, Map<String, Long>>builder()
                .put("q_1", ImmutableMap.of("n1", 0L, "n2", 0L, "n3", 2L))
                .put("q_2", ImmutableMap.of("n1", 3L, "n2", 5L, "n3", 2L))
                .buildOrThrow();
        Map<String, Map<String, Map<Integer, Long>>> tasks = ImmutableMap.<String, Map<String, Map<Integer, Long>>>builder()
                .put("q_1", ImmutableMap.of(
                        "n3", ImmutableMap.of(1, 5L)))
                .put("q_2", ImmutableMap.of(
                        "n1", ImmutableMap.of(
                                1, 1L,
                                2, 2L),
                        "n2", ImmutableMap.of(
                                3, 3L,
                                4, 1L,
                                5, 1L),
                        "n3", ImmutableMap.of(6, 2L)))
                .buildOrThrow();

        assertEquals(
                lowMemoryKiller.chooseTargetToKill(
                        toRunningQueryInfoList(queries),
                        toNodeMemoryInfoList(memoryPool, queries, tasks)),
                Optional.empty());
    }

    @Test
    public void testKillsBiggestTasksIfAllExecuteSameTime()
    {
        testKillsBiggestTasksIfAllExecuteSameTime(new Duration(0, SECONDS), new Duration(0, SECONDS)); // simulates no taskInfo entries
        testKillsBiggestTasksIfAllExecuteSameTime(new Duration(1, SECONDS), new Duration(1, SECONDS));
        testKillsBiggestTasksIfAllExecuteSameTime(new Duration(1000, SECONDS), new Duration(1000, SECONDS));
    }

    private void testKillsBiggestTasksIfAllExecuteSameTime(Duration scheduledTime, Duration blockedTime)
    {
        int memoryPool = 12;
        Map<String, Map<String, Long>> queries = ImmutableMap.<String, Map<String, Long>>builder()
                .put("q_1", ImmutableMap.of("n1", 0L, "n2", 8L, "n3", 0L, "n4", 0L, "n5", 0L))
                .put("q_2", ImmutableMap.of("n1", 3L, "n2", 5L, "n3", 2L, "n4", 4L, "n5", 0L))
                .put("q_3", ImmutableMap.of("n1", 0L, "n2", 0L, "n3", 11L, "n4", 0L, "n5", 0L))
                .buildOrThrow();

        Map<String, Map<String, Map<Integer, Long>>> tasks = ImmutableMap.<String, Map<String, Map<Integer, Long>>>builder()
                .put("q_1", ImmutableMap.of(
                        "n2", ImmutableMap.of(1, 8L)))
                .put("q_2", ImmutableMap.of(
                        "n1", ImmutableMap.of(
                                1, 1L,
                                2, 3L),
                        "n2", ImmutableMap.of(
                                3, 3L,
                                4, 1L,
                                5, 1L),
                        "n3", ImmutableMap.of(6, 2L),
                        "n4", ImmutableMap.of(
                                7, 2L,
                                8, 2L),
                        "n5", ImmutableMap.of()))
                .put("q_3", ImmutableMap.of(
                        "n3", ImmutableMap.of(1, 11L))) // should not be picked as n3 does not have task retries enabled
                .buildOrThrow();

        Map<String, Map<Integer, TaskInfo>> taskInfos;
        if (scheduledTime.toMillis() == 0 && blockedTime.toMillis() == 0) {
            taskInfos = ImmutableMap.of();
        }
        else {
            taskInfos = ImmutableMap.of(
                    "q_1", ImmutableMap.of(
                            1, buildTaskInfo(taskId("q_1", 1), TaskState.RUNNING, scheduledTime, blockedTime, false)),
                    "q_2", ImmutableMap.of(
                            1, buildTaskInfo(taskId("q_2", 1), TaskState.RUNNING, scheduledTime, blockedTime, false),
                            2, buildTaskInfo(taskId("q_2", 2), TaskState.RUNNING, scheduledTime, blockedTime, false),
                            3, buildTaskInfo(taskId("q_2", 3), TaskState.RUNNING, scheduledTime, blockedTime, false),
                            4, buildTaskInfo(taskId("q_2", 4), TaskState.RUNNING, scheduledTime, blockedTime, false),
                            5, buildTaskInfo(taskId("q_2", 5), TaskState.RUNNING, scheduledTime, blockedTime, false),
                            6, buildTaskInfo(taskId("q_2", 6), TaskState.RUNNING, scheduledTime, blockedTime, false),
                            7, buildTaskInfo(taskId("q_2", 7), TaskState.RUNNING, scheduledTime, blockedTime, false),
                            8, buildTaskInfo(taskId("q_2", 8), TaskState.RUNNING, scheduledTime, blockedTime, false)));
        }

        assertEquals(
                lowMemoryKiller.chooseTargetToKill(
                        toRunningQueryInfoList(queries, ImmutableSet.of("q_1", "q_2"), taskInfos),
                        toNodeMemoryInfoList(memoryPool, queries, tasks)),
                Optional.of(KillTarget.selectedTasks(
                        ImmutableSet.of(
                                taskId("q_1", 1),
                                taskId("q_2", 6)))));
    }

    @Test
    public void testKillsSmallerTaskIfWastedEffortRatioIsBetter()
    {
        int memoryPool = 8;
        Map<String, Map<String, Long>> queries = ImmutableMap.<String, Map<String, Long>>builder()
                .put("q_1", ImmutableMap.of("n1", 3L, "n2", 8L))
                .put("q_2", ImmutableMap.of("n1", 7L, "n2", 2L))
                .buildOrThrow();

        Map<String, Map<String, Map<Integer, Long>>> tasks = ImmutableMap.<String, Map<String, Map<Integer, Long>>>builder()
                .put("q_1", ImmutableMap.of(
                        "n1", ImmutableMap.of(1, 3L),
                        "n2", ImmutableMap.of(2, 8L)))
                .put("q_2", ImmutableMap.of(
                        "n1", ImmutableMap.of(
                                1, 1L,
                                2, 6L),
                        "n2", ImmutableMap.of(3, 2L)))
                .buildOrThrow();

        Map<String, Map<Integer, TaskInfo>> taskInfos = ImmutableMap.of(
                "q_1", ImmutableMap.of(
                        1, buildTaskInfo(taskId("q_1", 1), TaskState.RUNNING, new Duration(30, SECONDS), new Duration(30, SECONDS), false),
                        2, buildTaskInfo(taskId("q_1", 2), TaskState.RUNNING, new Duration(400, SECONDS), new Duration(200, SECONDS), false)),
                "q_2", ImmutableMap.of(
                        1, buildTaskInfo(taskId("q_2", 1), TaskState.RUNNING, new Duration(30, SECONDS), new Duration(30, SECONDS), false),
                        2, buildTaskInfo(taskId("q_2", 2), TaskState.RUNNING, new Duration(100, SECONDS), new Duration(100, SECONDS), false),
                        3, buildTaskInfo(taskId("q_2", 3), TaskState.RUNNING, new Duration(30, SECONDS), new Duration(30, SECONDS), false)));

        // q1_1; n1; walltime 60s;  memory 3; ratio 0.05 (pick for n1)
        // q1_2; n2; walltime 600s; memory 8; ratio 0.0133
        // q2_1; n1; walltime 60s;  memory 1; ratio 0.0166
        // q2_2; n1; walltime 200s; memory 6; ratio 0.03
        // q2_3; n2; walltime 60s;  memory 2; ratio 0.033 (pick for n2)

        assertEquals(
                lowMemoryKiller.chooseTargetToKill(
                        toRunningQueryInfoList(queries, ImmutableSet.of("q_1", "q_2"), taskInfos),
                        toNodeMemoryInfoList(memoryPool, queries, tasks)),
                Optional.of(KillTarget.selectedTasks(
                        ImmutableSet.of(
                                taskId("q_1", 1),
                                taskId("q_2", 3)))));
    }

    @Test
    public void testPrefersKillingSpeculativeTasks()
    {
        int memoryPool = 8;
        Map<String, Map<String, Long>> queries = ImmutableMap.<String, Map<String, Long>>builder()
                .put("q_1", ImmutableMap.of("n1", 3L, "n2", 8L))
                .put("q_2", ImmutableMap.of("n1", 7L, "n2", 2L))
                .buildOrThrow();

        Map<String, Map<String, Map<Integer, Long>>> tasks = ImmutableMap.<String, Map<String, Map<Integer, Long>>>builder()
                .put("q_1", ImmutableMap.of(
                        "n1", ImmutableMap.of(1, 3L),
                        "n2", ImmutableMap.of(2, 8L)))
                .put("q_2", ImmutableMap.of(
                        "n1", ImmutableMap.of(
                                1, 1L,
                                2, 6L),
                        "n2", ImmutableMap.of(3, 2L)))
                .buildOrThrow();

        Map<String, Map<Integer, TaskInfo>> taskInfos = ImmutableMap.of(
                "q_1", ImmutableMap.of(
                        1, buildTaskInfo(taskId("q_1", 1), TaskState.RUNNING, new Duration(30, SECONDS), new Duration(30, SECONDS), false),
                        2, buildTaskInfo(taskId("q_1", 2), TaskState.RUNNING, new Duration(400, SECONDS), new Duration(200, SECONDS), false)),
                "q_2", ImmutableMap.of(
                        1, buildTaskInfo(taskId("q_2", 1), TaskState.RUNNING, new Duration(30, SECONDS), new Duration(30, SECONDS), true),
                        2, buildTaskInfo(taskId("q_2", 2), TaskState.RUNNING, new Duration(100, SECONDS), new Duration(100, SECONDS), false),
                        3, buildTaskInfo(taskId("q_2", 3), TaskState.RUNNING, new Duration(30, SECONDS), new Duration(30, SECONDS), false)));

        assertEquals(
                lowMemoryKiller.chooseTargetToKill(
                        toRunningQueryInfoList(queries, ImmutableSet.of("q_1", "q_2"), taskInfos),
                        toNodeMemoryInfoList(memoryPool, queries, tasks)),
                Optional.of(KillTarget.selectedTasks(
                        ImmutableSet.of(
                                taskId("q_2", 1), // if q_2_1 was not speculative then "q_1_1 would be picked
                                taskId("q_2", 3)))));
    }

    private static TaskInfo buildTaskInfo(TaskId taskId, TaskState state, Duration scheduledTime, Duration blockedTime, boolean speculative)
    {
        return new TaskInfo(
                new TaskStatus(
                        taskId,
                        "task-instance-id",
                        0,
                        state,
                        URI.create("fake://task/" + taskId + "/node/some_node"),
                        "some_node",
                        speculative,
                        ImmutableList.of(),
                        0,
                        0,
                        OutputBufferStatus.initial(),
                        DataSize.of(0, DataSize.Unit.MEGABYTE),
                        DataSize.of(1, DataSize.Unit.MEGABYTE),
                        DataSize.of(1, DataSize.Unit.MEGABYTE),
                        Optional.of(1),
                        DataSize.of(1, DataSize.Unit.MEGABYTE),
                        DataSize.of(1, DataSize.Unit.MEGABYTE),
                        DataSize.of(0, DataSize.Unit.MEGABYTE),
                        0,
                        Duration.valueOf("0s"),
                        0,
                        1,
                        1),
                DateTime.now(),
                new OutputBufferInfo(
                        "TESTING",
                        BufferState.FINISHED,
                        false,
                        false,
                        0,
                        0,
                        0,
                        0,
                        Optional.empty(),
                        Optional.of(new TDigestHistogram(new TDigest())),
                        Optional.empty()),
                ImmutableSet.of(),
                new TaskStats(DateTime.now(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        new Duration(0, MILLISECONDS),
                        new Duration(0, MILLISECONDS),
                        0,
                        0,
                        0,
                        0L,
                        0,
                        0,
                        0L,
                        0,
                        0,
                        0.0,
                        DataSize.ofBytes(0),
                        DataSize.ofBytes(0),
                        DataSize.ofBytes(0),
                        scheduledTime,
                        new Duration(0, MILLISECONDS),
                        blockedTime,
                        false,
                        ImmutableSet.of(),
                        DataSize.ofBytes(0),
                        0,
                        new Duration(0, MILLISECONDS),
                        DataSize.ofBytes(0),
                        0,
                        DataSize.ofBytes(0),
                        0,
                        DataSize.ofBytes(0),
                        0,
                        new Duration(0, MILLISECONDS),
                        DataSize.ofBytes(0),
                        0,
                        new Duration(0, MILLISECONDS),
                        DataSize.ofBytes(0),
                        DataSize.ofBytes(0),
                        Optional.empty(),
                        0,
                        new Duration(0, MILLISECONDS),
                        ImmutableList.of()),
                Optional.empty(),
                false);
    }
}
