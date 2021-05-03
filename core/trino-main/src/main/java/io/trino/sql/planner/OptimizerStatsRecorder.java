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
package io.trino.sql.planner;

import io.trino.sql.planner.optimizations.OptimizerStats;
import io.trino.sql.planner.optimizations.PlanOptimizer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class OptimizerStatsRecorder
{
    private final Map<Class<?>, OptimizerStats> stats = new HashMap<>();

    public void register(PlanOptimizer optimizer)
    {
        requireNonNull(optimizer, "optimizer is null");
        checkArgument(!optimizer.getClass().isAnonymousClass());
        stats.put(optimizer.getClass(), new OptimizerStats());
    }

    public Map<Class<?>, OptimizerStats> getStats()
    {
        return Collections.unmodifiableMap(stats);
    }

    public void record(PlanOptimizer optimizer, long nanos)
    {
        requireNonNull(optimizer, "optimizer is null");
        OptimizerStats optimizerStats = requireNonNull(stats.get(optimizer.getClass()), "optimizer is not registered");
        optimizerStats.record(nanos);
    }

    public void recordFailure(PlanOptimizer optimizer)
    {
        requireNonNull(optimizer, "optimizer is null");
        OptimizerStats optimizerStats = requireNonNull(stats.get(optimizer.getClass()), "optimizer is not registered");
        optimizerStats.recordFailure();
    }
}
