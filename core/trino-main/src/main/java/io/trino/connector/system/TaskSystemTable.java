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
package io.trino.connector.system;

import com.google.inject.Inject;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.execution.SqlTaskManager;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskStatus;
import io.trino.operator.TaskStats;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.InMemoryRecordSet.Builder;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import org.joda.time.DateTime;

import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.spi.connector.SystemTable.Distribution.ALL_NODES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;

public class TaskSystemTable
        implements SystemTable
{
    public static final SchemaTableName TASK_TABLE_NAME = new SchemaTableName("runtime", "tasks");

    public static final ConnectorTableMetadata TASK_TABLE = tableMetadataBuilder(TASK_TABLE_NAME)
            .column("node_id", createUnboundedVarcharType())

            .column("task_id", createUnboundedVarcharType())
            .column("stage_id", createUnboundedVarcharType())
            .column("query_id", createUnboundedVarcharType())
            .column("state", createUnboundedVarcharType())

            .column("splits", BIGINT)
            .column("queued_splits", BIGINT)
            .column("running_splits", BIGINT)
            .column("completed_splits", BIGINT)

            .column("split_scheduled_time_ms", BIGINT)
            .column("split_cpu_time_ms", BIGINT)
            .column("split_blocked_time_ms", BIGINT)

            .column("raw_input_bytes", BIGINT)
            .column("raw_input_rows", BIGINT)

            .column("processed_input_bytes", BIGINT)
            .column("processed_input_rows", BIGINT)

            .column("output_bytes", BIGINT)
            .column("output_rows", BIGINT)

            .column("physical_input_bytes", BIGINT)
            .column("physical_written_bytes", BIGINT)

            .column("created", TIMESTAMP_TZ_MILLIS)
            .column("start", TIMESTAMP_TZ_MILLIS)
            .column("last_heartbeat", TIMESTAMP_TZ_MILLIS)
            .column("end", TIMESTAMP_TZ_MILLIS)
            .build();

    private final SqlTaskManager taskManager;
    private final String nodeId;

    @Inject
    public TaskSystemTable(SqlTaskManager taskManager, NodeInfo nodeInfo)
    {
        this.taskManager = taskManager;
        this.nodeId = nodeInfo.getNodeId();
    }

    @Override
    public Distribution getDistribution()
    {
        return ALL_NODES;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return TASK_TABLE;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        Builder table = InMemoryRecordSet.builder(TASK_TABLE);
        for (TaskInfo taskInfo : taskManager.getAllTaskInfo()) {
            TaskStats stats = taskInfo.getStats();
            TaskStatus taskStatus = taskInfo.getTaskStatus();
            table.addRow(
                    nodeId,

                    taskStatus.getTaskId().toString(),
                    taskStatus.getTaskId().getStageId().toString(),
                    taskStatus.getTaskId().getQueryId().toString(),
                    taskStatus.getState().toString(),

                    (long) stats.getTotalDrivers(),
                    (long) stats.getQueuedDrivers(),
                    (long) stats.getRunningDrivers(),
                    (long) stats.getCompletedDrivers(),

                    toMillis(stats.getTotalScheduledTime()),
                    toMillis(stats.getTotalCpuTime()),
                    toMillis(stats.getTotalBlockedTime()),

                    toBytes(stats.getRawInputDataSize()),
                    stats.getRawInputPositions(),

                    toBytes(stats.getProcessedInputDataSize()),
                    stats.getProcessedInputPositions(),

                    toBytes(stats.getOutputDataSize()),
                    stats.getOutputPositions(),

                    toBytes(stats.getPhysicalInputDataSize()),
                    toBytes(stats.getPhysicalWrittenDataSize()),

                    toTimestampWithTimeZoneMillis(stats.getCreateTime()),
                    toTimestampWithTimeZoneMillis(stats.getFirstStartTime()),
                    toTimestampWithTimeZoneMillis(taskInfo.getLastHeartbeat()),
                    toTimestampWithTimeZoneMillis(stats.getEndTime()));
        }
        return table.build().cursor();
    }

    private static Long toMillis(Duration duration)
    {
        if (duration == null) {
            return null;
        }
        return duration.toMillis();
    }

    private static Long toBytes(DataSize dataSize)
    {
        if (dataSize == null) {
            return null;
        }
        return dataSize.toBytes();
    }

    private static Long toTimestampWithTimeZoneMillis(DateTime dateTime)
    {
        if (dateTime == null) {
            return null;
        }
        // dateTime.getZone() is the server zone, should be of no interest to the user
        return packDateTimeWithZone(dateTime.getMillis(), UTC_KEY);
    }
}
