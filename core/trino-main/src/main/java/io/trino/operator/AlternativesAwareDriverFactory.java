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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.execution.ScheduledSplit;
import io.trino.metadata.TableHandle;
import io.trino.split.AlternativeChooser;
import io.trino.split.AlternativeChooser.Choice;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AlternativesAwareDriverFactory
        implements SplitDriverFactory
{
    private final AlternativeChooser alternativeChooser;
    private final Session session;
    private final Map<TableHandle, AlternativeDriverFactory> alternatives;
    private final PlanNodeId chooseAlternativeNodeId;
    private final int pipelineId;
    private final boolean inputDriver;
    private final boolean outputDriver;
    private final OptionalInt driverInstances;

    public AlternativesAwareDriverFactory(
            AlternativeChooser alternativeChooser,
            Session session,
            Map<TableHandle, DriverFactory> alternatives,
            PlanNodeId chooseAlternativeNodeId,
            int pipelineId,
            boolean inputDriver,
            boolean outputDriver,
            OptionalInt driverInstances)
    {
        this.alternativeChooser = requireNonNull(alternativeChooser, "alternativeChooser is null");
        this.session = requireNonNull(session, "session is null");
        int alternativeId = 0;
        ImmutableMap.Builder<TableHandle, AlternativeDriverFactory> alternativesBuilder = ImmutableMap.builder();
        for (Map.Entry<TableHandle, DriverFactory> entry : alternatives.entrySet()) {
            alternativesBuilder.put(entry.getKey(), new AlternativeDriverFactory(alternativeId++, entry.getValue()));
        }
        this.alternatives = alternativesBuilder.buildOrThrow();
        this.chooseAlternativeNodeId = requireNonNull(chooseAlternativeNodeId, "chooseAlternativeNodeId is null");
        this.pipelineId = pipelineId;
        this.inputDriver = inputDriver;
        this.outputDriver = outputDriver;
        this.driverInstances = requireNonNull(driverInstances, "driverInstances is null");
    }

    @Override
    public int getPipelineId()
    {
        return pipelineId;
    }

    @Override
    public boolean isInputDriver()
    {
        return inputDriver;
    }

    @Override
    public boolean isOutputDriver()
    {
        return outputDriver;
    }

    @Override
    public OptionalInt getDriverInstances()
    {
        return driverInstances;
    }

    @Override
    public Driver createDriver(DriverContext driverContext, Optional<ScheduledSplit> optionalSplit)
    {
        checkArgument(optionalSplit.isPresent());
        ScheduledSplit split = optionalSplit.get();
        Choice chosen = alternativeChooser.chooseAlternative(session, split.getSplit(), alternatives.keySet());

        AlternativeDriverFactory alternative = alternatives.get(chosen.tableHandle());
        return alternative.driverFactory().createDriver(driverContext.setAlternativePlanContext(chosen.pageSourceProvider(), alternative.id()));
    }

    @Override
    public void noMoreDrivers()
    {
        alternatives.values().forEach(alternative -> alternative.driverFactory().noMoreDrivers());
    }

    @Override
    public boolean isNoMoreDrivers()
    {
        // noMoreDrivers is called on each alternative, so we can use any alternative here
        return alternatives.values().iterator().next().driverFactory().isNoMoreDrivers();
    }

    @Override
    public void localPlannerComplete()
    {
        alternatives.values().forEach(alternative -> alternative.driverFactory().localPlannerComplete());
    }

    @Override
    public Optional<PlanNodeId> getSourceId()
    {
        return Optional.of(chooseAlternativeNodeId);
    }

    private record AlternativeDriverFactory(int id, DriverFactory driverFactory)
    {
        private AlternativeDriverFactory(int id, DriverFactory driverFactory)
        {
            this.id = id;
            this.driverFactory = requireNonNull(driverFactory, "driverFactory is null");
        }
    }
}
