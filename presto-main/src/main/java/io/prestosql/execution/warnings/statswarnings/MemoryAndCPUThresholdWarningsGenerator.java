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
import com.google.common.collect.Ordering;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.execution.QueryInfo;
import io.prestosql.spi.PrestoWarning;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.prestosql.SystemSessionProperties.getQueryMaxCpuTime;
import static io.prestosql.SystemSessionProperties.getQueryMaxMemory;
import static io.prestosql.SystemSessionProperties.getQueryMaxTotalMemory;
import static io.prestosql.SystemSessionProperties.getQueryWarningThreshold;
import static io.prestosql.spi.connector.StandardWarningCode.TOTAL_CPU_TIME_OVER_THRESHOLD_VAL;
import static io.prestosql.spi.connector.StandardWarningCode.TOTAL_MEMORY_LIMIT_OVER_THRESHOLD_VAL;
import static io.prestosql.spi.connector.StandardWarningCode.USER_MEMORY_LIMIT_OVER_THRESHOLD_VAL;
import static java.lang.String.format;

public class MemoryAndCPUThresholdWarningsGenerator
        implements ExecutionStatisticsWarningsGenerator
{
    private final Duration maxQueryCpuTime;
    private final DataSize maxQueryMemory;
    private final DataSize maxQueryTotalMemory;

    public MemoryAndCPUThresholdWarningsGenerator(Duration maxQueryCpuTime, DataSize maxQueryMemory, DataSize maxQueryTotalMemory)
    {
        this.maxQueryCpuTime = maxQueryCpuTime;
        this.maxQueryMemory = maxQueryMemory;
        this.maxQueryTotalMemory = maxQueryTotalMemory;
    }

    public List<PrestoWarning> generateStatsWarnings(QueryInfo queryInfo, Session session)
    {
        ImmutableList.Builder<PrestoWarning> warningBuilder = new ImmutableList.Builder<>();
        double threshold = getQueryWarningThreshold(session);

        Duration maxCpuTime = Ordering.natural().min(getQueryMaxCpuTime(session), maxQueryCpuTime).convertTo(TimeUnit.DAYS);

        DataSize userMemoryLimit;
        if (getQueryMaxMemory(session).toBytes() < maxQueryMemory.toBytes()) {
            userMemoryLimit = getQueryMaxMemory(session).convertTo(DataSize.Unit.GIGABYTE);
        }
        else {
            userMemoryLimit = maxQueryMemory.convertTo(DataSize.Unit.GIGABYTE);
        }

        DataSize totalMemoryLimit;
        if (getQueryMaxTotalMemory(session).toBytes() < maxQueryTotalMemory.toBytes()) {
            totalMemoryLimit = getQueryMaxTotalMemory(session).convertTo(DataSize.Unit.GIGABYTE);
        }
        else {
            totalMemoryLimit = maxQueryTotalMemory.convertTo(DataSize.Unit.GIGABYTE);
        }

        Duration totalCpuTime = queryInfo.getQueryStats().getTotalCpuTime().convertTo(TimeUnit.DAYS);
        if (totalCpuTime.toMillis() > threshold * maxCpuTime.toMillis()) {
            String message = format("Query Id %s has exceeded the max cpu warning threshold value. This query used %s of the maxCPU of %s.", queryInfo.getQueryId().getId(),
                    totalCpuTime, maxCpuTime);
            warningBuilder.add(new PrestoWarning(TOTAL_CPU_TIME_OVER_THRESHOLD_VAL, message));
        }

        DataSize peakUserMemoryReservation = queryInfo.getQueryStats().getPeakUserMemoryReservation().convertTo(DataSize.Unit.GIGABYTE);
        DataSize peakTotalMemoryReservation = queryInfo.getQueryStats().getPeakTotalMemoryReservation().convertTo(DataSize.Unit.GIGABYTE);

        if (peakUserMemoryReservation.toBytes() > threshold * userMemoryLimit.toBytes()) {
            String message = format("Query Id %s's peakUserMemoryReservation has exceeded the threshold warning value. This query used %s of the userMemoryLimit of %s.", queryInfo.getQueryId().getId(),
                    peakUserMemoryReservation, userMemoryLimit);
            warningBuilder.add(new PrestoWarning(USER_MEMORY_LIMIT_OVER_THRESHOLD_VAL, message));
        }
        if (peakTotalMemoryReservation.toBytes() > threshold * totalMemoryLimit.toBytes()) {
            String message = format("Query Id %s's peakTotalMemoryReservation has exceeded the threshold warning value. This query used %s of the totalMemoryLimit of %s", queryInfo.getQueryId().getId(),
                    peakTotalMemoryReservation, totalMemoryLimit);
            warningBuilder.add(new PrestoWarning(TOTAL_MEMORY_LIMIT_OVER_THRESHOLD_VAL, message));
        }
        return warningBuilder.build();
    }
}
