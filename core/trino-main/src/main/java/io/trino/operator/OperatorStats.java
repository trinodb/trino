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
import com.google.errorprone.annotations.Immutable;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.spi.Mergeable;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.plan.PlanNodeId;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@Immutable
public class OperatorStats
{
    private final int stageId;
    private final int pipelineId;
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final String operatorType;

    private final long totalDrivers;

    private final long addInputCalls;
    private final Duration addInputWall;
    private final Duration addInputCpu;
    private final DataSize physicalInputDataSize;
    private final long physicalInputPositions;
    private final Duration physicalInputReadTime;
    private final DataSize internalNetworkInputDataSize;
    private final long internalNetworkInputPositions;
    private final DataSize rawInputDataSize;
    private final DataSize inputDataSize;
    private final long inputPositions;
    private final double sumSquaredInputPositions;

    private final long getOutputCalls;
    private final Duration getOutputWall;
    private final Duration getOutputCpu;
    private final DataSize outputDataSize;
    private final long outputPositions;

    private final long dynamicFilterSplitsProcessed;
    private final Metrics metrics;
    private final Metrics connectorMetrics;

    private final DataSize physicalWrittenDataSize;

    private final Duration blockedWall;

    private final long finishCalls;
    private final Duration finishWall;
    private final Duration finishCpu;

    private final DataSize userMemoryReservation;
    private final DataSize revocableMemoryReservation;
    private final DataSize peakUserMemoryReservation;
    private final DataSize peakRevocableMemoryReservation;
    private final DataSize peakTotalMemoryReservation;

    private final DataSize spilledDataSize;

    private final Optional<BlockedReason> blockedReason;

    @Nullable
    private final OperatorInfo info;

    @JsonCreator
    public OperatorStats(
            @JsonProperty("stageId") int stageId,
            @JsonProperty("pipelineId") int pipelineId,
            @JsonProperty("operatorId") int operatorId,
            @JsonProperty("planNodeId") PlanNodeId planNodeId,
            @JsonProperty("operatorType") String operatorType,

            @JsonProperty("totalDrivers") long totalDrivers,

            @JsonProperty("addInputCalls") long addInputCalls,
            @JsonProperty("addInputWall") Duration addInputWall,
            @JsonProperty("addInputCpu") Duration addInputCpu,
            @JsonProperty("physicalInputDataSize") DataSize physicalInputDataSize,
            @JsonProperty("physicalInputPositions") long physicalInputPositions,
            @JsonProperty("physicalInputReadTime") Duration physicalInputReadTime,
            @JsonProperty("internalNetworkInputDataSize") DataSize internalNetworkInputDataSize,
            @JsonProperty("internalNetworkInputPositions") long internalNetworkInputPositions,
            @JsonProperty("rawInputDataSize") DataSize rawInputDataSize,
            @JsonProperty("inputDataSize") DataSize inputDataSize,
            @JsonProperty("inputPositions") long inputPositions,
            @JsonProperty("sumSquaredInputPositions") double sumSquaredInputPositions,

            @JsonProperty("getOutputCalls") long getOutputCalls,
            @JsonProperty("getOutputWall") Duration getOutputWall,
            @JsonProperty("getOutputCpu") Duration getOutputCpu,
            @JsonProperty("outputDataSize") DataSize outputDataSize,
            @JsonProperty("outputPositions") long outputPositions,

            @JsonProperty("dynamicFilterSplitsProcessed") long dynamicFilterSplitsProcessed,
            @JsonProperty("metrics") Metrics metrics,
            @JsonProperty("connectorMetrics") Metrics connectorMetrics,

            @JsonProperty("physicalWrittenDataSize") DataSize physicalWrittenDataSize,

            @JsonProperty("blockedWall") Duration blockedWall,

            @JsonProperty("finishCalls") long finishCalls,
            @JsonProperty("finishWall") Duration finishWall,
            @JsonProperty("finishCpu") Duration finishCpu,

            @JsonProperty("userMemoryReservation") DataSize userMemoryReservation,
            @JsonProperty("revocableMemoryReservation") DataSize revocableMemoryReservation,
            @JsonProperty("peakUserMemoryReservation") DataSize peakUserMemoryReservation,
            @JsonProperty("peakRevocableMemoryReservation") DataSize peakRevocableMemoryReservation,
            @JsonProperty("peakTotalMemoryReservation") DataSize peakTotalMemoryReservation,

            @JsonProperty("spilledDataSize") DataSize spilledDataSize,

            @JsonProperty("blockedReason") Optional<BlockedReason> blockedReason,

            @Nullable
            @JsonProperty("info") OperatorInfo info)
    {
        this.stageId = stageId;
        this.pipelineId = pipelineId;

        checkArgument(operatorId >= 0, "operatorId is negative");
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.operatorType = requireNonNull(operatorType, "operatorType is null");

        this.totalDrivers = totalDrivers;

        this.addInputCalls = addInputCalls;
        this.addInputWall = requireNonNull(addInputWall, "addInputWall is null");
        this.addInputCpu = requireNonNull(addInputCpu, "addInputCpu is null");
        this.physicalInputDataSize = requireNonNull(physicalInputDataSize, "physicalInputDataSize is null");
        this.physicalInputPositions = physicalInputPositions;
        this.physicalInputReadTime = requireNonNull(physicalInputReadTime, "physicalInputReadTime is null");
        this.internalNetworkInputDataSize = requireNonNull(internalNetworkInputDataSize, "internalNetworkInputDataSize is null");
        this.internalNetworkInputPositions = internalNetworkInputPositions;
        this.rawInputDataSize = requireNonNull(rawInputDataSize, "rawInputDataSize is null");
        this.inputDataSize = requireNonNull(inputDataSize, "inputDataSize is null");
        checkArgument(inputPositions >= 0, "inputPositions is negative");
        this.inputPositions = inputPositions;
        this.sumSquaredInputPositions = sumSquaredInputPositions;

        this.getOutputCalls = getOutputCalls;
        this.getOutputWall = requireNonNull(getOutputWall, "getOutputWall is null");
        this.getOutputCpu = requireNonNull(getOutputCpu, "getOutputCpu is null");
        this.outputDataSize = requireNonNull(outputDataSize, "outputDataSize is null");
        checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;

        this.dynamicFilterSplitsProcessed = dynamicFilterSplitsProcessed;
        this.metrics = requireNonNull(metrics, "metrics is null");
        this.connectorMetrics = requireNonNull(connectorMetrics, "connectorMetrics is null");

        this.physicalWrittenDataSize = requireNonNull(physicalWrittenDataSize, "physicalWrittenDataSize is null");

        this.blockedWall = requireNonNull(blockedWall, "blockedWall is null");

        this.finishCalls = finishCalls;
        this.finishWall = requireNonNull(finishWall, "finishWall is null");
        this.finishCpu = requireNonNull(finishCpu, "finishCpu is null");

        this.userMemoryReservation = requireNonNull(userMemoryReservation, "userMemoryReservation is null");
        this.revocableMemoryReservation = requireNonNull(revocableMemoryReservation, "revocableMemoryReservation is null");

        this.peakUserMemoryReservation = requireNonNull(peakUserMemoryReservation, "peakUserMemoryReservation is null");
        this.peakRevocableMemoryReservation = requireNonNull(peakRevocableMemoryReservation, "peakRevocableMemoryReservation is null");
        this.peakTotalMemoryReservation = requireNonNull(peakTotalMemoryReservation, "peakTotalMemoryReservation is null");

        this.spilledDataSize = requireNonNull(spilledDataSize, "spilledDataSize is null");

        this.blockedReason = blockedReason;

        this.info = info;
    }

    @JsonProperty
    public int getStageId()
    {
        return stageId;
    }

    @JsonProperty
    public int getPipelineId()
    {
        return pipelineId;
    }

    @JsonProperty
    public int getOperatorId()
    {
        return operatorId;
    }

    @JsonProperty
    public PlanNodeId getPlanNodeId()
    {
        return planNodeId;
    }

    @JsonProperty
    public String getOperatorType()
    {
        return operatorType;
    }

    @JsonProperty
    public long getTotalDrivers()
    {
        return totalDrivers;
    }

    @JsonProperty
    public long getAddInputCalls()
    {
        return addInputCalls;
    }

    @JsonProperty
    public Duration getAddInputWall()
    {
        return addInputWall;
    }

    @JsonProperty
    public Duration getAddInputCpu()
    {
        return addInputCpu;
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
    public DataSize getInputDataSize()
    {
        return inputDataSize;
    }

    @JsonProperty
    public long getInputPositions()
    {
        return inputPositions;
    }

    @JsonProperty
    public double getSumSquaredInputPositions()
    {
        return sumSquaredInputPositions;
    }

    @JsonProperty
    public long getGetOutputCalls()
    {
        return getOutputCalls;
    }

    @JsonProperty
    public Duration getGetOutputWall()
    {
        return getOutputWall;
    }

    @JsonProperty
    public Duration getGetOutputCpu()
    {
        return getOutputCpu;
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
    public long getDynamicFilterSplitsProcessed()
    {
        return dynamicFilterSplitsProcessed;
    }

    @JsonProperty
    public Metrics getMetrics()
    {
        return metrics;
    }

    @JsonProperty
    public Metrics getConnectorMetrics()
    {
        return connectorMetrics;
    }

    @JsonProperty
    public DataSize getPhysicalWrittenDataSize()
    {
        return physicalWrittenDataSize;
    }

    @JsonProperty
    public Duration getBlockedWall()
    {
        return blockedWall;
    }

    @JsonProperty
    public long getFinishCalls()
    {
        return finishCalls;
    }

    @JsonProperty
    public Duration getFinishWall()
    {
        return finishWall;
    }

    @JsonProperty
    public Duration getFinishCpu()
    {
        return finishCpu;
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
    public DataSize getPeakUserMemoryReservation()
    {
        return peakUserMemoryReservation;
    }

    @JsonProperty
    public DataSize getPeakRevocableMemoryReservation()
    {
        return peakRevocableMemoryReservation;
    }

    @JsonProperty
    public DataSize getPeakTotalMemoryReservation()
    {
        return peakTotalMemoryReservation;
    }

    @JsonProperty
    public DataSize getSpilledDataSize()
    {
        return spilledDataSize;
    }

    @JsonProperty
    public Optional<BlockedReason> getBlockedReason()
    {
        return blockedReason;
    }

    @Nullable
    @JsonProperty
    public OperatorInfo getInfo()
    {
        return info;
    }

    public OperatorStats add(OperatorStats operatorStats)
    {
        return add(ImmutableList.of(operatorStats));
    }

    public OperatorStats add(Iterable<OperatorStats> operators)
    {
        long totalDrivers = this.totalDrivers;

        long addInputCalls = this.addInputCalls;
        long addInputWall = this.addInputWall.roundTo(NANOSECONDS);
        long addInputCpu = this.addInputCpu.roundTo(NANOSECONDS);
        long physicalInputDataSize = this.physicalInputDataSize.toBytes();
        long physicalInputPositions = this.physicalInputPositions;
        long physicalInputReadTimeNanos = this.physicalInputReadTime.roundTo(NANOSECONDS);
        long internalNetworkInputDataSize = this.internalNetworkInputDataSize.toBytes();
        long internalNetworkInputPositions = this.internalNetworkInputPositions;
        long rawInputDataSize = this.rawInputDataSize.toBytes();
        long inputDataSize = this.inputDataSize.toBytes();
        long inputPositions = this.inputPositions;
        double sumSquaredInputPositions = this.sumSquaredInputPositions;

        long getOutputCalls = this.getOutputCalls;
        long getOutputWall = this.getOutputWall.roundTo(NANOSECONDS);
        long getOutputCpu = this.getOutputCpu.roundTo(NANOSECONDS);
        long outputDataSize = this.outputDataSize.toBytes();
        long outputPositions = this.outputPositions;

        long dynamicFilterSplitsProcessed = this.dynamicFilterSplitsProcessed;
        Metrics.Accumulator metricsAccumulator = Metrics.accumulator().add(this.getMetrics());
        Metrics.Accumulator connectorMetricsAccumulator = Metrics.accumulator().add(this.getConnectorMetrics());

        long physicalWrittenDataSize = this.physicalWrittenDataSize.toBytes();

        long blockedWall = this.blockedWall.roundTo(NANOSECONDS);

        long finishCalls = this.finishCalls;
        long finishWall = this.finishWall.roundTo(NANOSECONDS);
        long finishCpu = this.finishCpu.roundTo(NANOSECONDS);

        long memoryReservation = this.userMemoryReservation.toBytes();
        long revocableMemoryReservation = this.revocableMemoryReservation.toBytes();
        long peakUserMemory = this.peakUserMemoryReservation.toBytes();
        long peakRevocableMemory = this.peakRevocableMemoryReservation.toBytes();
        long peakTotalMemory = this.peakTotalMemoryReservation.toBytes();

        long spilledDataSize = this.spilledDataSize.toBytes();

        Optional<BlockedReason> blockedReason = this.blockedReason;

        Mergeable<OperatorInfo> base = getMergeableInfoOrNull(info);
        ImmutableList.Builder<OperatorInfo> operatorInfos = ImmutableList.builder();
        for (OperatorStats operator : operators) {
            checkArgument(operator.getOperatorId() == operatorId, "Expected operatorId to be %s but was %s", operatorId, operator.getOperatorId());
            checkArgument(operator.getOperatorType().equals(operatorType), "Expected operatorType to be %s but was %s", operatorType, operator.getOperatorType());

            totalDrivers += operator.totalDrivers;

            addInputCalls += operator.getAddInputCalls();
            addInputWall += operator.getAddInputWall().roundTo(NANOSECONDS);
            addInputCpu += operator.getAddInputCpu().roundTo(NANOSECONDS);
            physicalInputDataSize += operator.getPhysicalInputDataSize().toBytes();
            physicalInputPositions += operator.getPhysicalInputPositions();
            physicalInputReadTimeNanos += operator.getPhysicalInputReadTime().roundTo(NANOSECONDS);
            internalNetworkInputDataSize += operator.getInternalNetworkInputDataSize().toBytes();
            internalNetworkInputPositions += operator.getInternalNetworkInputPositions();
            rawInputDataSize += operator.getRawInputDataSize().toBytes();
            inputDataSize += operator.getInputDataSize().toBytes();
            inputPositions += operator.getInputPositions();
            sumSquaredInputPositions += operator.getSumSquaredInputPositions();

            getOutputCalls += operator.getGetOutputCalls();
            getOutputWall += operator.getGetOutputWall().roundTo(NANOSECONDS);
            getOutputCpu += operator.getGetOutputCpu().roundTo(NANOSECONDS);
            outputDataSize += operator.getOutputDataSize().toBytes();
            outputPositions += operator.getOutputPositions();

            dynamicFilterSplitsProcessed += operator.getDynamicFilterSplitsProcessed();
            metricsAccumulator.add(operator.getMetrics());
            connectorMetricsAccumulator.add(operator.getConnectorMetrics());

            physicalWrittenDataSize += operator.getPhysicalWrittenDataSize().toBytes();

            finishCalls += operator.getFinishCalls();
            finishWall += operator.getFinishWall().roundTo(NANOSECONDS);
            finishCpu += operator.getFinishCpu().roundTo(NANOSECONDS);

            blockedWall += operator.getBlockedWall().roundTo(NANOSECONDS);

            memoryReservation += operator.getUserMemoryReservation().toBytes();
            revocableMemoryReservation += operator.getRevocableMemoryReservation().toBytes();

            peakUserMemory = max(peakUserMemory, operator.getPeakUserMemoryReservation().toBytes());
            peakRevocableMemory = max(peakRevocableMemory, operator.getPeakRevocableMemoryReservation().toBytes());
            peakTotalMemory = max(peakTotalMemory, operator.getPeakTotalMemoryReservation().toBytes());

            spilledDataSize += operator.getSpilledDataSize().toBytes();

            if (operator.getBlockedReason().isPresent()) {
                blockedReason = operator.getBlockedReason();
            }

            OperatorInfo info = operator.getInfo();
            if (base != null && info != null) {
                verify(base.getClass() == info.getClass(), "Cannot merge operator infos: %s and %s", base, info);
                operatorInfos.add(info);
            }
        }

        return new OperatorStats(
                stageId,
                pipelineId,
                operatorId,
                planNodeId,
                operatorType,

                totalDrivers,

                addInputCalls,
                new Duration(addInputWall, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(addInputCpu, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                DataSize.ofBytes(physicalInputDataSize),
                physicalInputPositions,
                new Duration(physicalInputReadTimeNanos, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                DataSize.ofBytes(internalNetworkInputDataSize),
                internalNetworkInputPositions,
                DataSize.ofBytes(rawInputDataSize),
                DataSize.ofBytes(inputDataSize),
                inputPositions,
                sumSquaredInputPositions,

                getOutputCalls,
                new Duration(getOutputWall, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(getOutputCpu, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                DataSize.ofBytes(outputDataSize),
                outputPositions,

                dynamicFilterSplitsProcessed,
                metricsAccumulator.get(),
                connectorMetricsAccumulator.get(),

                DataSize.ofBytes(physicalWrittenDataSize),

                new Duration(blockedWall, NANOSECONDS).convertToMostSuccinctTimeUnit(),

                finishCalls,
                new Duration(finishWall, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(finishCpu, NANOSECONDS).convertToMostSuccinctTimeUnit(),

                DataSize.ofBytes(memoryReservation),
                DataSize.ofBytes(revocableMemoryReservation),
                DataSize.ofBytes(peakUserMemory),
                DataSize.ofBytes(peakRevocableMemory),
                DataSize.ofBytes(peakTotalMemory),

                DataSize.ofBytes(spilledDataSize),

                blockedReason,

                (OperatorInfo) mergeInfos(base, operatorInfos.build()));
    }

    @SuppressWarnings("unchecked")
    private static Mergeable<OperatorInfo> getMergeableInfoOrNull(OperatorInfo info)
    {
        Mergeable<OperatorInfo> base = null;
        if (info instanceof Mergeable) {
            base = (Mergeable<OperatorInfo>) info;
        }
        return base;
    }

    @SuppressWarnings("unchecked")
    private static <T> Mergeable<T> mergeInfos(Mergeable<T> base, List<T> others)
    {
        if (base == null) {
            return null;
        }
        return (Mergeable<T>) base.mergeWith(others);
    }

    public OperatorStats summarize()
    {
        if (info == null || info.isFinal()) {
            return this;
        }
        OperatorInfo info = null;
        return new OperatorStats(
                stageId,
                pipelineId,
                operatorId,
                planNodeId,
                operatorType,
                totalDrivers,
                addInputCalls,
                addInputWall,
                addInputCpu,
                physicalInputDataSize,
                physicalInputPositions,
                physicalInputReadTime,
                internalNetworkInputDataSize,
                internalNetworkInputPositions,
                rawInputDataSize,
                inputDataSize,
                inputPositions,
                sumSquaredInputPositions,
                getOutputCalls,
                getOutputWall,
                getOutputCpu,
                outputDataSize,
                outputPositions,
                dynamicFilterSplitsProcessed,
                metrics,
                connectorMetrics,
                physicalWrittenDataSize,
                blockedWall,
                finishCalls,
                finishWall,
                finishCpu,
                userMemoryReservation,
                revocableMemoryReservation,
                peakUserMemoryReservation,
                peakRevocableMemoryReservation,
                peakTotalMemoryReservation,
                spilledDataSize,
                blockedReason,
                info);
    }
}
