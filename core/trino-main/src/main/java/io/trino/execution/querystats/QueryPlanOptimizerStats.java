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
package io.trino.execution.querystats;

import io.trino.spi.eventlistener.QueryPlanOptimizerStatistics;

public class QueryPlanOptimizerStats
{
    private final String rule;
    private long invocations;
    private long applied;
    private long totalTime;
    private long failures;

    public QueryPlanOptimizerStats(String rule)
    {
        this.rule = rule;
    }

    public void record(long nanos, boolean applied)
    {
        if (applied) {
            this.applied += 1;
        }

        invocations += 1;
        totalTime += nanos;
    }

    public void recordFailure()
    {
        failures += 1;
    }

    public String getRule()
    {
        return rule;
    }

    public long getInvocations()
    {
        return invocations;
    }

    public long getApplied()
    {
        return applied;
    }

    public long getFailures()
    {
        return failures;
    }

    public long getTotalTime()
    {
        return totalTime;
    }

    public QueryPlanOptimizerStatistics snapshot(String rule)
    {
        return new QueryPlanOptimizerStatistics(rule, invocations, applied, totalTime, failures);
    }

    public QueryPlanOptimizerStats merge(QueryPlanOptimizerStats other)
    {
        invocations += other.getInvocations();
        applied += other.getApplied();
        failures += other.getFailures();
        totalTime += other.getTotalTime();

        return this;
    }
}
