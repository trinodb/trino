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

import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class QueryPlanOptimizerStats
{
    private final String rule;
    private final AtomicLong invocations = new AtomicLong();
    private final AtomicLong applied = new AtomicLong();
    private final AtomicLong totalTime = new AtomicLong();
    private final AtomicLong failures = new AtomicLong();

    public QueryPlanOptimizerStats(String rule)
    {
        this.rule = requireNonNull(rule, "rule is null");
    }

    public void record(long nanos, boolean applied)
    {
        if (applied) {
            this.applied.incrementAndGet();
        }

        invocations.incrementAndGet();
        totalTime.addAndGet(nanos);
    }

    public void recordFailure()
    {
        failures.incrementAndGet();
    }

    public String getRule()
    {
        return rule;
    }

    public long getInvocations()
    {
        return invocations.get();
    }

    public long getApplied()
    {
        return applied.get();
    }

    public long getFailures()
    {
        return failures.get();
    }

    public long getTotalTime()
    {
        return totalTime.get();
    }

    public QueryPlanOptimizerStatistics snapshot()
    {
        return new QueryPlanOptimizerStatistics(rule, invocations.get(), applied.get(), totalTime.get(), failures.get());
    }

    public QueryPlanOptimizerStats merge(QueryPlanOptimizerStats other)
    {
        checkArgument(rule.equals(other.getRule()), "Cannot merge stats for different rules: %s and %s", rule, other.getRule());

        invocations.addAndGet(other.getInvocations());
        applied.addAndGet(other.getApplied());
        failures.addAndGet(other.getFailures());
        totalTime.addAndGet(other.getTotalTime());

        return this;
    }
}
