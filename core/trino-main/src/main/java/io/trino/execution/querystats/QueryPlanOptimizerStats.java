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

import com.google.errorprone.annotations.ThreadSafe;
import io.trino.spi.eventlistener.QueryPlanOptimizerStatistics;

import java.util.concurrent.atomic.LongAdder;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class QueryPlanOptimizerStats
{
    private final String rule;
    private final LongAdder invocations = new LongAdder();
    private final LongAdder applied = new LongAdder();
    private final LongAdder totalTime = new LongAdder();
    private final LongAdder failures = new LongAdder();

    public QueryPlanOptimizerStats(String rule)
    {
        this.rule = requireNonNull(rule, "rule is null");
    }

    public void record(long nanos, boolean applied)
    {
        if (applied) {
            this.applied.increment();
        }

        invocations.increment();
        totalTime.add(nanos);
    }

    public void recordFailure()
    {
        failures.increment();
    }

    public String getRule()
    {
        return rule;
    }

    public long getInvocations()
    {
        return invocations.longValue();
    }

    public long getApplied()
    {
        return applied.longValue();
    }

    public long getFailures()
    {
        return failures.longValue();
    }

    public long getTotalTime()
    {
        return totalTime.longValue();
    }

    public QueryPlanOptimizerStatistics snapshot()
    {
        return new QueryPlanOptimizerStatistics(rule, invocations.longValue(), applied.longValue(), totalTime.longValue(), failures.longValue());
    }

    public QueryPlanOptimizerStats merge(QueryPlanOptimizerStats other)
    {
        checkArgument(rule.equals(other.getRule()), "Cannot merge stats for different rules: %s and %s", rule, other.getRule());

        invocations.add(other.getInvocations());
        applied.add(other.getApplied());
        failures.add(other.getFailures());
        totalTime.add(other.getTotalTime());

        return this;
    }
}
