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
package io.trino.spi.eventlistener;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.Unstable;

import static java.util.Objects.requireNonNull;

/**
 * This class is JSON serializable for convenience and serialization compatibility is not guaranteed across versions.
 */
public class StageTaskStatistics
{
    private final int stageId;
    private final int tasks;

    // resource utilization
    private final LongDistribution cpuTimeDistribution;
    private final LongDistribution scheduledTimeDistribution;
    private final LongDistribution peakMemoryReservationDistribution;

    // processing metrics
    private final LongDistribution rawInputDataSizeDistribution;
    private final LongDistribution rawInputPositionsDistribution;
    private final LongDistribution processedInputDataSizeDistribution;
    private final LongDistribution processedInputPositionsDistribution;
    private final LongDistribution outputDataSizeDistribution;
    private final LongDistribution outputPositionsDistribution;
    private final LongDistribution totalDriversDistribution;

    // task lifetime statistics (millis since query start)
    private final LongSymmetricDistribution createTimeMillisDistribution;
    private final LongSymmetricDistribution firstStartTimeMillisDistribution;
    private final LongSymmetricDistribution lastStartTimeMillisDistribution;
    private final LongSymmetricDistribution terminatingStartTimeMillisDistribution;
    private final LongSymmetricDistribution lastEndTimeMillisDistribution;
    private final LongSymmetricDistribution endTimeMillisDistribution;

    // task lifetime statistics (faction of query progress 0.0 - 1.0)
    private final DoubleSymmetricDistribution createTimeScaledDistribution;
    private final DoubleSymmetricDistribution firstStartTimeScaledDistribution;
    private final DoubleSymmetricDistribution lastStartTimeScaledDistribution;
    private final DoubleSymmetricDistribution terminatingStartTimeScaledDistribution;
    private final DoubleSymmetricDistribution lastEndTimeScaledDistribution;
    private final DoubleSymmetricDistribution endTimeScaledDistribution;

    @JsonCreator
    @Unstable
    public StageTaskStatistics(
            @JsonProperty("stageId") int stageId,
            @JsonProperty("tasks") int tasks,
            @JsonProperty("cpuTimeDistribution") LongDistribution cpuTimeDistribution,
            @JsonProperty("scheduledTimeDistribution") LongDistribution scheduledTimeDistribution,
            @JsonProperty("peakMemoryReservationDistribution") LongDistribution peakMemoryReservationDistribution,
            @JsonProperty("rawInputDataSizeDistribution") LongDistribution rawInputDataSizeDistribution,
            @JsonProperty("rawInputPositionsDistribution") LongDistribution rawInputPositionsDistribution,
            @JsonProperty("processedInputDataSizeDistribution") LongDistribution processedInputDataSizeDistribution,
            @JsonProperty("processedInputPositionsDistribution") LongDistribution processedInputPositionsDistribution,
            @JsonProperty("outputDataSizeDistribution") LongDistribution outputDataSizeDistribution,
            @JsonProperty("outputPositionsDistribution") LongDistribution outputPositionsDistribution,
            @JsonProperty("totalDriversDistribution") LongDistribution totalDriversDistribution,
            @JsonProperty("createTimeMillisDistribution") LongSymmetricDistribution createTimeMillisDistribution,
            @JsonProperty("firstStartTimeMillisDistribution") LongSymmetricDistribution firstStartTimeMillisDistribution,
            @JsonProperty("lastStartTimeMillisDistribution") LongSymmetricDistribution lastStartTimeMillisDistribution,
            @JsonProperty("terminatingStartTimeMillisDistribution") LongSymmetricDistribution terminatingStartTimeMillisDistribution,
            @JsonProperty("lastEndTimeMillisDistribution") LongSymmetricDistribution lastEndTimeMillisDistribution,
            @JsonProperty("endTimeMillisDistribution") LongSymmetricDistribution endTimeMillisDistribution,
            @JsonProperty("createTimeScaledDistribution") DoubleSymmetricDistribution createTimeScaledDistribution,
            @JsonProperty("firstStartTimeScaledDistribution") DoubleSymmetricDistribution firstStartTimeScaledDistribution,
            @JsonProperty("lastStartTimeScaledDistribution") DoubleSymmetricDistribution lastStartTimeScaledDistribution,
            @JsonProperty("terminatingStartTimeScaledDistribution") DoubleSymmetricDistribution terminatingStartTimeScaledDistribution,
            @JsonProperty("lastEndTimeScaledDistribution") DoubleSymmetricDistribution lastEndTimeScaledDistribution,
            @JsonProperty("endTimeScaledDistribution") DoubleSymmetricDistribution endTimeScaledDistribution)
    {
        this.stageId = stageId;
        this.tasks = tasks;
        this.cpuTimeDistribution = requireNonNull(cpuTimeDistribution, "cpuTimeDistribution is null");
        this.scheduledTimeDistribution = requireNonNull(scheduledTimeDistribution, "scheduledTimeDistribution is null");
        this.peakMemoryReservationDistribution = requireNonNull(peakMemoryReservationDistribution, "peakMemoryReservationDistribution is null");
        this.rawInputDataSizeDistribution = requireNonNull(rawInputDataSizeDistribution, "rawInputDataSizeDistribution is null");
        this.rawInputPositionsDistribution = requireNonNull(rawInputPositionsDistribution, "rawInputPositionsDistribution is null");
        this.processedInputDataSizeDistribution = requireNonNull(processedInputDataSizeDistribution, "processedInputDataSizeDistribution is null");
        this.processedInputPositionsDistribution = requireNonNull(processedInputPositionsDistribution, "processedInputPositionsDistribution is null");
        this.outputDataSizeDistribution = requireNonNull(outputDataSizeDistribution, "outputDataSizeDistribution is null");
        this.outputPositionsDistribution = requireNonNull(outputPositionsDistribution, "outputPositionsDistribution is null");
        this.totalDriversDistribution = requireNonNull(totalDriversDistribution, "totalDriversDistribution is null");
        this.createTimeMillisDistribution = requireNonNull(createTimeMillisDistribution, "createTimeMillisDistribution is null");
        this.firstStartTimeMillisDistribution = requireNonNull(firstStartTimeMillisDistribution, "firstStartTimeMillisDistribution is null");
        this.lastStartTimeMillisDistribution = requireNonNull(lastStartTimeMillisDistribution, "lastStartTimeMillisDistribution is null");
        this.terminatingStartTimeMillisDistribution = requireNonNull(terminatingStartTimeMillisDistribution, "terminatingStartTimeMillisDistribution is null");
        this.lastEndTimeMillisDistribution = requireNonNull(lastEndTimeMillisDistribution, "lastEndTimeMillisDistribution is null");
        this.endTimeMillisDistribution = requireNonNull(endTimeMillisDistribution, "endTimeMillisDistribution is null");
        this.createTimeScaledDistribution = requireNonNull(createTimeScaledDistribution, "createTimeScaledDistribution is null");
        this.firstStartTimeScaledDistribution = requireNonNull(firstStartTimeScaledDistribution, "firstStartTimeScaledDistribution is null");
        this.lastStartTimeScaledDistribution = requireNonNull(lastStartTimeScaledDistribution, "lastStartTimeScaledDistribution is null");
        this.terminatingStartTimeScaledDistribution = requireNonNull(terminatingStartTimeScaledDistribution, "terminatingStartTimeScaledDistribution is null");
        this.lastEndTimeScaledDistribution = requireNonNull(lastEndTimeScaledDistribution, "lastEndTimeScaledDistribution is null");
        this.endTimeScaledDistribution = requireNonNull(endTimeScaledDistribution, "endTimeScaledDistribution is null");
    }

    @JsonProperty
    public int getStageId()
    {
        return stageId;
    }

    @JsonProperty
    public int getTasks()
    {
        return tasks;
    }

    @JsonProperty
    public LongDistribution getCpuTimeDistribution()
    {
        return cpuTimeDistribution;
    }

    @JsonProperty
    public LongDistribution getScheduledTimeDistribution()
    {
        return scheduledTimeDistribution;
    }

    @JsonProperty
    public LongDistribution getPeakMemoryReservationDistribution()
    {
        return peakMemoryReservationDistribution;
    }

    @JsonProperty
    public LongDistribution getRawInputDataSizeDistribution()
    {
        return rawInputDataSizeDistribution;
    }

    @JsonProperty
    public LongDistribution getRawInputPositionsDistribution()
    {
        return rawInputPositionsDistribution;
    }

    @JsonProperty
    public LongDistribution getProcessedInputDataSizeDistribution()
    {
        return processedInputDataSizeDistribution;
    }

    @JsonProperty
    public LongDistribution getProcessedInputPositionsDistribution()
    {
        return processedInputPositionsDistribution;
    }

    @JsonProperty
    public LongDistribution getOutputDataSizeDistribution()
    {
        return outputDataSizeDistribution;
    }

    @JsonProperty
    public LongDistribution getOutputPositionsDistribution()
    {
        return outputPositionsDistribution;
    }

    @JsonProperty
    public LongDistribution getTotalDriversDistribution()
    {
        return totalDriversDistribution;
    }

    @JsonProperty
    public LongSymmetricDistribution getCreateTimeMillisDistribution()
    {
        return createTimeMillisDistribution;
    }

    @JsonProperty
    public LongSymmetricDistribution getFirstStartTimeMillisDistribution()
    {
        return firstStartTimeMillisDistribution;
    }

    @JsonProperty
    public LongSymmetricDistribution getLastStartTimeMillisDistribution()
    {
        return lastStartTimeMillisDistribution;
    }

    @JsonProperty
    public LongSymmetricDistribution getTerminatingStartTimeMillisDistribution()
    {
        return terminatingStartTimeMillisDistribution;
    }

    @JsonProperty
    public LongSymmetricDistribution getLastEndTimeMillisDistribution()
    {
        return lastEndTimeMillisDistribution;
    }

    @JsonProperty
    public LongSymmetricDistribution getEndTimeMillisDistribution()
    {
        return endTimeMillisDistribution;
    }

    @JsonProperty
    public DoubleSymmetricDistribution getCreateTimeScaledDistribution()
    {
        return createTimeScaledDistribution;
    }

    @JsonProperty
    public DoubleSymmetricDistribution getFirstStartTimeScaledDistribution()
    {
        return firstStartTimeScaledDistribution;
    }

    @JsonProperty
    public DoubleSymmetricDistribution getLastStartTimeScaledDistribution()
    {
        return lastStartTimeScaledDistribution;
    }

    @JsonProperty
    public DoubleSymmetricDistribution getTerminatingStartTimeScaledDistribution()
    {
        return terminatingStartTimeScaledDistribution;
    }

    @JsonProperty
    public DoubleSymmetricDistribution getLastEndTimeScaledDistribution()
    {
        return lastEndTimeScaledDistribution;
    }

    @JsonProperty
    public DoubleSymmetricDistribution getEndTimeScaledDistribution()
    {
        return endTimeScaledDistribution;
    }
}
