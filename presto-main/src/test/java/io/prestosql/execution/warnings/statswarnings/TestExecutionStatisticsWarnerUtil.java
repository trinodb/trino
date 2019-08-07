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

package io.prestosql.execution.warnings.statswarnings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.stats.Distribution;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryStats;
import io.prestosql.execution.StageId;
import io.prestosql.execution.StageInfo;
import io.prestosql.execution.StageState;
import io.prestosql.execution.StageStats;
import io.prestosql.execution.TableInfo;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.TaskInfo;
import io.prestosql.execution.TaskStatus;
import io.prestosql.execution.buffer.OutputBufferInfo;
import io.prestosql.operator.BlockedReason;
import io.prestosql.operator.TaskStats;
import io.prestosql.operator.TestPipelineStats;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.eventlistener.StageGcStatistics;
import io.prestosql.spi.memory.MemoryPoolId;
import io.prestosql.sql.planner.plan.PlanNodeId;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static io.airlift.units.DataSize.Unit.BYTE;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.execution.QueryState.RUNNING;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class TestExecutionStatisticsWarnerUtil
{
    private TestExecutionStatisticsWarnerUtil()
    {
    }

    public static QueryInfo getQueryInfo1() throws Exception
    {
        URI uri = new URI("http://127.0.0.1/");

        StageStats stageStats = createStageStats();

        List<TaskInfo> stageInfo2Taskinfo = Arrays.asList(
                      createTaskInfo(1.0, 1.0, uri, "1", 2),
                      createTaskInfo(1.0, 1.0, uri, "1", 2),
                      createTaskInfo(1.0, 1.0, uri, "1", 2),
                      createTaskInfo(1.0, 1.0, uri, "1", 2),
                      createTaskInfo(1.0, 1.0, uri, "1", 2),
                      createTaskInfo(12.0, 12.0, uri, "1", 2));

        StageInfo stageInfo2 = createStageInfo(2, stageInfo2Taskinfo, uri, stageStats, ImmutableList.of());

        List<TaskInfo> stageInfo1Taskinfo = Arrays.asList(
                      createTaskInfo(1.0, 1.0, uri, "1", 1),
                      createTaskInfo(1.0, 1.0, uri, "1", 1),
                      createTaskInfo(1.0, 1.0, uri, "1", 1),
                      createTaskInfo(1.0, 1.0, uri, "1", 1),
                      createTaskInfo(1.0, 1.0, uri, "1", 1),
                      createTaskInfo(12.0, 12.0, uri, "1", 1));

        StageInfo stageInfo1 = createStageInfo(1, stageInfo1Taskinfo, uri, stageStats, ImmutableList.of(stageInfo2));
        return createQueryInfo(stageInfo1, "16GB", "16GB", "22h");
    }

    public static QueryInfo getQueryInfo2() throws Exception
    {
        URI uri = new URI("http://127.0.0.1/");

        StageStats stageStats = createStageStats();

        List<TaskInfo> stageInfo3TaskInfos = Arrays.asList(
                      createTaskInfo(1.0, 1.0, uri, "1", 3),
                      createTaskInfo(1.0, 1.0, uri, "1", 3),
                      createTaskInfo(1.0, 1.0, uri, "1", 3),
                      createTaskInfo(1.0, 1.0, uri, "1", 3),
                      createTaskInfo(1.0, 1.0, uri, "1", 3),
                      createTaskInfo(1.0, 1.0, uri, "1", 3));

        StageInfo stageInfo3 = createStageInfo(3, stageInfo3TaskInfos, uri, stageStats, ImmutableList.of());

        List<TaskInfo> stageInfo2TaskInfos = Arrays.asList(
                      createTaskInfo(1.0, 1.0, uri, "1", 2),
                      createTaskInfo(1.0, 1.0, uri, "1", 2),
                      createTaskInfo(1.0, 1.0, uri, "1", 2),
                      createTaskInfo(1.0, 1.0, uri, "1", 2),
                      createTaskInfo(1.0, 1.0, uri, "1", 2),
                      createTaskInfo(1.0, 1.0, uri, "1", 2));

        StageInfo stageInfo2 = createStageInfo(2, stageInfo2TaskInfos, uri, stageStats, ImmutableList.of(stageInfo3));

        List<TaskInfo> stageInfo1TaskInfos = Arrays.asList(
                      createTaskInfo(1.0, 1.0, uri, "1", 1));

        StageInfo stageInfo1 = createStageInfo(1, stageInfo1TaskInfos, uri, stageStats, ImmutableList.of(stageInfo2));

        return createQueryInfo(stageInfo1, "1GB", "1GB", "22h");
    }

    private static QueryInfo createQueryInfo(StageInfo rootStage, String peakUserMemory, String peakTotalMemory, String totalCpuTime)
    {
        return new QueryInfo(
                new QueryId("0"),
                TEST_SESSION.toSessionRepresentation(),
                RUNNING,
                new MemoryPoolId("reserved"),
                false,
                URI.create("1"),
                ImmutableList.of("2", "3"),
                "SELECT 4",
                Optional.of("SELECT 4"),
                new QueryStats(
                        DateTime.parse("1991-09-06T05:00-05:30"),
                        DateTime.parse("1991-09-06T05:01-05:30"),
                        DateTime.parse("1991-09-06T05:02-05:30"),
                        DateTime.parse("1991-09-06T06:00-05:30"),
                        Duration.valueOf("8m"),
                        Duration.valueOf("7m"),
                        Duration.valueOf("34m"),
                        Duration.valueOf("35m"),
                        Duration.valueOf("44m"),
                        Duration.valueOf("9m"),
                        Duration.valueOf("10m"),
                        Duration.valueOf("11m"),
                        Duration.valueOf("12m"),
                        13,
                        14,
                        15,
                        16,
                        17,
                        18,
                        34,
                        19,
                        20.0,
                        DataSize.valueOf("16GB"),
                        DataSize.valueOf("16GB"),
                        DataSize.valueOf("16GB"),
                        DataSize.valueOf(peakUserMemory),
                        DataSize.valueOf("16GB"),
                        DataSize.valueOf(peakTotalMemory),
                        DataSize.valueOf("16GB"),
                        DataSize.valueOf("16GB"),
                        DataSize.valueOf("16GB"),
                        true,
                        Duration.valueOf("800000000d"),
                        Duration.valueOf(totalCpuTime),
                        Duration.valueOf("26m"),
                        true,
                        ImmutableSet.of(BlockedReason.WAITING_FOR_MEMORY),
                        DataSize.valueOf("271GB"),
                        281,
                        DataSize.valueOf("272GB"),
                        282,
                        DataSize.valueOf("27GB"),
                        28,
                        DataSize.valueOf("29GB"),
                        30,
                        DataSize.valueOf("31GB"),
                        32,
                        DataSize.valueOf("32GB"),
                        ImmutableList.of(new StageGcStatistics(
                            101,
                            102,
                            103,
                            104,
                            105,
                            106,
                            107)),
                        ImmutableList.of()),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                "33",
                Optional.of(rootStage),
                null,
                StandardErrorCode.ABANDONED_QUERY.toErrorCode(),
                ImmutableList.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                Optional.empty());
    }

    private static StageStats createStageStats()
    {
        StageStats stageStats = new StageStats(
                new DateTime(0),
                new Distribution().snapshot(),
                4,
                5,
                6,
                7,
                8,
                10,
                26,
                11,
                12.0,
                new DataSize(13, BYTE),
                new DataSize(14, BYTE),
                new DataSize(15, BYTE),
                new DataSize(16, BYTE),
                new DataSize(17, BYTE),
                new Duration(15, NANOSECONDS),
                new Duration(16, NANOSECONDS),
                new Duration(18, NANOSECONDS),
                false,
                ImmutableSet.of(),
                new DataSize(191, BYTE),
                201,
                new DataSize(192, BYTE),
                202,
                new DataSize(19, BYTE),
                20,
                new DataSize(21, BYTE),
                22,
                new DataSize(23, BYTE),
                new DataSize(24, BYTE),
                25,
                new DataSize(26, BYTE),
                new StageGcStatistics(
                    101,
                    102,
                    103,
                    104,
                    105,
                    106,
                    107),
                ImmutableList.of());
        return stageStats;
    }

    private static StageInfo createStageInfo(int stageId, List<TaskInfo> taskInfos, URI uri, StageStats stageStats, List<StageInfo> subStages)
    {
        StageInfo stageInfo = new StageInfo(
                new StageId("abcde123", stageId),
                StageState.FINISHED,
                uri,
                null,
                null,
                stageStats,
                taskInfos,
                subStages,
                new HashMap<PlanNodeId, TableInfo>(),
                null);
        return stageInfo;
    }

    private static TaskInfo createTaskInfo(double totalScheduledTime, double totalCPUTime, URI uri, String queryId, int stageId)
    {
        OutputBufferInfo outputBufferInfo = new OutputBufferInfo(null, null, true, true, 1, 1, 1, 1, new ArrayList<>());
        TaskStatus taskStatus = TaskStatus.initialTaskStatus(new TaskId(queryId, stageId, 1), uri, "1");
        return new TaskInfo(taskStatus, DateTime.now(), outputBufferInfo, new HashSet<>(), createTaskStats(totalScheduledTime, totalCPUTime), false);
    }

    private static TaskStats createTaskStats(double totalScheduledTime, double totalCPUTime)
    {
        TaskStats taskStats = new TaskStats(
                new DateTime(1),
                new DateTime(2),
                new DateTime(100),
                new DateTime(101),
                new DateTime(3),
                new Duration(4, NANOSECONDS),
                new Duration(5, NANOSECONDS),
                6,
                7,
                5,
                8,
                6,
                24,
                10,
                11.0,
                new DataSize(12, BYTE),
                new DataSize(13, BYTE),
                new DataSize(14, BYTE),
                new Duration(totalScheduledTime, NANOSECONDS),
                new Duration(totalCPUTime, NANOSECONDS),
                new Duration(18, NANOSECONDS),
                false,
                ImmutableSet.of(),
                new DataSize(191, BYTE),
                201,
                new DataSize(192, BYTE),
                202,
                new DataSize(19, BYTE),
                20,
                new DataSize(21, BYTE),
                22,
                new DataSize(23, BYTE),
                24,
                new DataSize(25, BYTE),
                26,
                new Duration(27, NANOSECONDS),
                ImmutableList.of(TestPipelineStats.EXPECTED));
        return taskStats;
    }
}
