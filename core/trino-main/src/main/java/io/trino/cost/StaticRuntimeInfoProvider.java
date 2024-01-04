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

package io.trino.cost;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanFragmentId;

import java.util.List;
import java.util.Map;

import static io.trino.execution.scheduler.faulttolerant.OutputStatsEstimator.OutputStatsEstimateResult;
import static java.util.Objects.requireNonNull;

public class StaticRuntimeInfoProvider
        implements RuntimeInfoProvider
{
    private final Map<PlanFragmentId, OutputStatsEstimateResult> runtimeOutputStats;
    private final Map<PlanFragmentId, PlanFragment> planFragments;

    public StaticRuntimeInfoProvider(
            Map<PlanFragmentId, OutputStatsEstimateResult> runtimeOutputStats,
            Map<PlanFragmentId, PlanFragment> planFragments)
    {
        this.runtimeOutputStats = ImmutableMap.copyOf(requireNonNull(runtimeOutputStats, "runtimeOutputStats is null"));
        this.planFragments = ImmutableMap.copyOf(requireNonNull(planFragments, "planFragments is null"));
    }

    @Override
    public OutputStatsEstimateResult getRuntimeOutputStats(PlanFragmentId planFragmentId)
    {
        return runtimeOutputStats.getOrDefault(planFragmentId, OutputStatsEstimateResult.unknown());
    }

    @Override
    public PlanFragment getPlanFragment(PlanFragmentId planFragmentId)
    {
        PlanFragment planFragment = planFragments.get(planFragmentId);
        requireNonNull(planFragment, "planFragment must not be null: %s".formatted(planFragmentId));
        return planFragment;
    }

    @Override
    public List<PlanFragment> getAllPlanFragments()
    {
        return ImmutableList.copyOf(planFragments.values());
    }
}
