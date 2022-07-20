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
package io.trino.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Immutable
public class DriverStats
{
    private final DateTime createTime;
    private final DateTime startTime;
    private final DateTime endTime;

    private final Duration queuedTime;
    private final Duration elapsedTime;

    private final DataSize userMemoryReservation;
    private final DataSize revocableMemoryReservation;

    private final Duration totalScheduledTime;
    private final Duration totalCpuTime;
    private final Duration totalBlockedTime;
    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;

    private final DataSize physicalInputDataSize;
    private final long physicalInputPositions;
    private final Duration physicalInputReadTime;

    private final DataSize internalNetworkInputDataSize;
    private final long internalNetworkInputPositions;

    private final DataSize rawInputDataSize;
    private final long rawInputPositions;
    private final Duration rawInputReadTime;

    private final DataSize processedInputDataSize;
    private final long processedInputPositions;

    private final Duration inputBlockedTime;

    private final DataSize outputDataSize;
    private final long outputPositions;

    private final Duration outputBlockedTime;

    private final DataSize physicalWrittenDataSize;

    private final List<OperatorStats> operatorStats;

    public DriverStats()
    {
        this.createTime = DateTime.now();
        this.startTime = null;
        this.endTime = null;
        this.queuedTime = new Duration(0, MILLISECONDS);
        this.elapsedTime = new Duration(0, MILLISECONDS);

        this.userMemoryReservation = DataSize.ofBytes(0);
        this.revocableMemoryReservation = DataSize.ofBytes(0);

        this.totalScheduledTime = new Duration(0, MILLISECONDS);
        this.totalCpuTime = new Duration(0, MILLISECONDS);
        this.totalBlockedTime = new Duration(0, MILLISECONDS);
        this.fullyBlocked = false;
        this.blockedReasons = ImmutableSet.of();

        this.physicalInputDataSize = DataSize.ofBytes(0);
        this.physicalInputPositions = 0;
        this.physicalInputReadTime = new Duration(0, MILLISECONDS);

        this.internalNetworkInputDataSize = DataSize.ofBytes(0);
        this.internalNetworkInputPositions = 0;

        this.rawInputDataSize = DataSize.ofBytes(0);
        this.rawInputPositions = 0;
        this.rawInputReadTime = new Duration(0, MILLISECONDS);

        this.processedInputDataSize = DataSize.ofBytes(0);
        this.processedInputPositions = 0;

        this.inputBlockedTime = new Duration(0, MILLISECONDS);

        this.outputDataSize = DataSize.ofBytes(0);
        this.outputPositions = 0;

        this.outputBlockedTime = new Duration(0, MILLISECONDS);

        this.physicalWrittenDataSize = DataSize.ofBytes(0);

        this.operatorStats = ImmutableList.of();
    }

    @JsonCreator
    public DriverStats(
            @JsonProperty("createTime") DateTime createTime,
            @JsonProperty("startTime") DateTime startTime,
            @JsonProperty("endTime") DateTime endTime,
            @JsonProperty("queuedTime") Duration queuedTime,
            @JsonProperty("elapsedTime") Duration elapsedTime,

            @JsonProperty("userMemoryReservation") DataSize userMemoryReservation,
            @JsonProperty("revocableMemoryReservation") DataSize revocableMemoryReservation,

            @JsonProperty("totalScheduledTime") Duration totalScheduledTime,
            @JsonProperty("totalCpuTime") Duration totalCpuTime,
            @JsonProperty("totalBlockedTime") Duration totalBlockedTime,
            @JsonProperty("fullyBlocked") boolean fullyBlocked,
            @JsonProperty("blockedReasons") Set<BlockedReason> blockedReasons,

            @JsonProperty("physicalInputDataSize") DataSize physicalInputDataSize,
            @JsonProperty("physicalInputPositions") long physicalInputPositions,
            @JsonProperty("physicalInputReadTime") Duration physicalInputReadTime,

            @JsonProperty("internalNetworkInputDataSize") DataSize internalNetworkInputDataSize,
            @JsonProperty("internalNetworkInputPositions") long internalNetworkInputPositions,

            @JsonProperty("rawInputDataSize") DataSize rawInputDataSize,
            @JsonProperty("rawInputPositions") long rawInputPositions,
            @JsonProperty("rawInputReadTime") Duration rawInputReadTime,

            @JsonProperty("processedInputDataSize") DataSize processedInputDataSize,
            @JsonProperty("processedInputPositions") long processedInputPositions,

            @JsonProperty("inputBlockedTime") Duration inputBlockedTime,

            @JsonProperty("outputDataSize") DataSize outputDataSize,
            @JsonProperty("outputPositions") long outputPositions,

            @JsonProperty("outputBlockedTime") Duration outputBlockedTime,

            @JsonProperty("physicalWrittenDataSize") DataSize physicalWrittenDataSize,

            @JsonProperty("operatorStats") List<OperatorStats> operatorStats)
    {
        this.createTime = requireNonNull(createTime, "createTime is null");
        this.startTime = startTime;
        this.endTime = endTime;
        this.queuedTime = requireNonNull(queuedTime, "queuedTime is null");
        this.elapsedTime = requireNonNull(elapsedTime, "elapsedTime is null");

        this.userMemoryReservation = requireNonNull(userMemoryReservation, "userMemoryReservation is null");
        this.revocableMemoryReservation = requireNonNull(revocableMemoryReservation, "revocableMemoryReservation is null");

        this.totalScheduledTime = requireNonNull(totalScheduledTime, "totalScheduledTime is null");
        this.totalCpuTime = requireNonNull(totalCpuTime, "totalCpuTime is null");
        this.totalBlockedTime = requireNonNull(totalBlockedTime, "totalBlockedTime is null");
        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));

        this.physicalInputDataSize = requireNonNull(physicalInputDataSize, "physicalInputDataSize is null");
        checkArgument(physicalInputPositions >= 0, "physicalInputPositions is negative");
        this.physicalInputPositions = physicalInputPositions;
        this.physicalInputReadTime = requireNonNull(physicalInputReadTime, "physicalInputReadTime is null");

        this.internalNetworkInputDataSize = requireNonNull(internalNetworkInputDataSize, "internalNetworkInputDataSize is null");
        checkArgument(internalNetworkInputPositions >= 0, "internalNetworkInputPositions is negative");
        this.internalNetworkInputPositions = internalNetworkInputPositions;

        this.rawInputDataSize = requireNonNull(rawInputDataSize, "rawInputDataSize is null");
        checkArgument(rawInputPositions >= 0, "rawInputPositions is negative");
        this.rawInputPositions = rawInputPositions;
        this.rawInputReadTime = requireNonNull(rawInputReadTime, "rawInputReadTime is null");

        this.processedInputDataSize = requireNonNull(processedInputDataSize, "processedInputDataSize is null");
        checkArgument(processedInputPositions >= 0, "processedInputPositions is negative");
        this.processedInputPositions = processedInputPositions;

        this.inputBlockedTime = requireNonNull(inputBlockedTime, "inputBlockedTime is null");

        this.outputDataSize = requireNonNull(outputDataSize, "outputDataSize is null");
        checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;

        this.outputBlockedTime = requireNonNull(outputBlockedTime, "outputBlockedTime is null");

        this.physicalWrittenDataSize = requireNonNull(physicalWrittenDataSize, "physicalWrittenDataSize is null");

        this.operatorStats = ImmutableList.copyOf(requireNonNull(operatorStats, "operatorStats is null"));
    }

    @JsonProperty
    public DateTime getCreateTime()
    {
        return createTime;
    }

    @Nullable
    @JsonProperty
    public DateTime getStartTime()
    {
        return startTime;
    }

    @Nullable
    @JsonProperty
    public DateTime getEndTime()
    {
        return endTime;
    }

    @JsonProperty
    public Duration getQueuedTime()
    {
        return queuedTime;
    }

    @JsonProperty
    public Duration getElapsedTime()
    {
        return elapsedTime;
    }

    @JsonProperty
    public DataSize getUserMemoryReservation()
    {
        return userMemoryReservation;
    }

    @JsonProperty
    public DataSize getRevocableMemoryReservation()
    {
        return revocableMemoryReservation;
    }

    @JsonProperty
    public Duration getTotalScheduledTime()
    {
        return totalScheduledTime;
    }

    @JsonProperty
    public Duration getTotalCpuTime()
    {
        return totalCpuTime;
    }

    @JsonProperty
    public Duration getTotalBlockedTime()
    {
        return totalBlockedTime;
    }

    @JsonProperty
    public boolean isFullyBlocked()
    {
        return fullyBlocked;
    }

    @JsonProperty
    public Set<BlockedReason> getBlockedReasons()
    {
        return blockedReasons;
    }

    @JsonProperty
    public DataSize getPhysicalInputDataSize()
    {
        return physicalInputDataSize;
    }

    @JsonProperty
    public long getPhysicalInputPositions()
    {
        return physicalInputPositions;
    }

    @JsonProperty
    public Duration getPhysicalInputReadTime()
    {
        return physicalInputReadTime;
    }

    @JsonProperty
    public DataSize getInternalNetworkInputDataSize()
    {
        return internalNetworkInputDataSize;
    }

    @JsonProperty
    public long getInternalNetworkInputPositions()
    {
        return internalNetworkInputPositions;
    }

    @JsonProperty
    public DataSize getRawInputDataSize()
    {
        return rawInputDataSize;
    }

    @JsonProperty
    public long getRawInputPositions()
    {
        return rawInputPositions;
    }

    @JsonProperty
    public Duration getRawInputReadTime()
    {
        return rawInputReadTime;
    }

    @JsonProperty
    public DataSize getProcessedInputDataSize()
    {
        return processedInputDataSize;
    }

    @JsonProperty
    public long getProcessedInputPositions()
    {
        return processedInputPositions;
    }

    @JsonProperty
    public Duration getInputBlockedTime()
    {
        return inputBlockedTime;
    }

    @JsonProperty
    public DataSize getOutputDataSize()
    {
        return outputDataSize;
    }

    @JsonProperty
    public long getOutputPositions()
    {
        return outputPositions;
    }

    @JsonProperty
    public Duration getOutputBlockedTime()
    {
        return outputBlockedTime;
    }

    @JsonProperty
    public DataSize getPhysicalWrittenDataSize()
    {
        return physicalWrittenDataSize;
    }

    @JsonProperty
    public List<OperatorStats> getOperatorStats()
    {
        return operatorStats;
    }
}
