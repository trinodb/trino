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

import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanFragmentId;

import java.util.List;

import static io.trino.execution.scheduler.faulttolerant.OutputStatsEstimator.OutputStatsEstimateResult;

/**
 * Provides runtime information from FTE execution. This is used to re-optimize the plan based
 * on the actual runtime statistics.
 */
public interface RuntimeInfoProvider
{
    OutputStatsEstimateResult getRuntimeOutputStats(PlanFragmentId planFragmentId);

    PlanFragment getPlanFragment(PlanFragmentId planFragmentId);

    List<PlanFragment> getAllPlanFragments();

    static RuntimeInfoProvider noImplementation()
    {
        return new RuntimeInfoProvider()
        {
            @Override
            public OutputStatsEstimateResult getRuntimeOutputStats(PlanFragmentId planFragmentId)
            {
                throw new UnsupportedOperationException("RuntimeInfoProvider is not implemented");
            }

            @Override
            public PlanFragment getPlanFragment(PlanFragmentId planFragmentId)
            {
                throw new UnsupportedOperationException("RuntimeInfoProvider is not implemented");
            }

            @Override
            public List<PlanFragment> getAllPlanFragments()
            {
                throw new UnsupportedOperationException("RuntimeInfoProvider is not implemented");
            }
        };
    }
}
