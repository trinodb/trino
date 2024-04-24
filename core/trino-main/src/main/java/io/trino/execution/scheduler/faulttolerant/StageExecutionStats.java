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
package io.trino.execution.scheduler.faulttolerant;

import com.google.inject.Inject;
import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.Managed;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public class StageExecutionStats
{
    private static final int EXECUTION_FRACTION_RESCALE_FACTOR = 1_000_000;
    private final Map<String, CounterStat> outputEstimationKindCounters = new ConcurrentHashMap<>();
    private final DistributionStat speculativeExecutionFractionDistribution = new DistributionStat();

    private final MBeanExporter mbeanExporter;

    @Inject
    public StageExecutionStats(MBeanExporter mbeanExporter)
    {
        this.mbeanExporter = requireNonNull(mbeanExporter, "mbeanExporter is null");
    }

    public void recordSourceOutputEstimationOnStageStart(String sourceOutputEstimationKind, int sourcesCount)
    {
        updateSourceOutputEstimationKindCounter(sourceOutputEstimationKind, sourcesCount);
    }

    public void recordSourcesFinishedOnStageStart(int sourcesCount)
    {
        updateSourceOutputEstimationKindCounter("finished", sourcesCount);
    }

    @Managed
    public void recordStageSpeculativeExecutionFraction(double fractionSpentSpeculative)
    {
        speculativeExecutionFractionDistribution.add((long) (fractionSpentSpeculative * EXECUTION_FRACTION_RESCALE_FACTOR));
    }

    private void updateSourceOutputEstimationKindCounter(String outputEstimationKind, int sourcesCount)
    {
        getCounterStat(outputEstimationKind).update(sourcesCount);
    }

    private CounterStat getCounterStat(String outputEstimationKind)
    {
        return outputEstimationKindCounters.computeIfAbsent(outputEstimationKind, ignored -> {
            CounterStat counter = new CounterStat();
            mbeanExporter.exportWithGeneratedName(counter, StageExecutionStats.class, "output_size_estimation" + "_" + outputEstimationKind);
            return counter;
        });
    }
}
