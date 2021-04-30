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

import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.RuleStats;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class RuleStatsRecorder
{
    private final Map<Class<?>, RuleStats> stats = new HashMap<>();

    public void registerAll(Collection<Rule<?>> rules)
    {
        for (Rule<?> rule : rules) {
            checkArgument(!rule.getClass().isAnonymousClass());
            stats.put(rule.getClass(), new RuleStats());
        }
    }

    public void record(Rule<?> rule, long nanos, boolean match)
    {
        stats.get(rule.getClass()).record(nanos, match);
    }

    public void recordFailure(Rule<?> rule)
    {
        stats.get(rule.getClass()).recordFailure();
    }

    public Map<Class<?>, RuleStats> getStats()
    {
        return ImmutableMap.copyOf(stats);
    }
}
