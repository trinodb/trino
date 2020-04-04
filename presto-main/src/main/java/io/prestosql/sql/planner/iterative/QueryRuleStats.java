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
package io.prestosql.sql.planner.iterative;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class QueryRuleStats
{
    final boolean failure;
    final boolean match;
    final long time;

    private QueryRuleStats()
    {
        this.failure = true;
        this.match = false;
        this.time = 0;
    }

    private QueryRuleStats(boolean match, long time)
    {
        this.failure = false;
        this.match = match;
        this.time = time;
    }

    @JsonCreator
    public QueryRuleStats(
            @JsonProperty("match") boolean match,
            @JsonProperty("failure") boolean failure,
            @JsonProperty("time") long time)
    {
        this.match = match;
        this.failure = failure;
        this.time = time;
    }

    @JsonProperty
    public boolean getMatch()
    {
        return match;
    }

    @JsonProperty
    public boolean getFailure()
    {
        return failure;
    }

    @JsonProperty
    public long getTime()
    {
        return time;
    }

    public static QueryRuleStats createFailureQueryRuleStats()
    {
        return new QueryRuleStats();
    }

    public static QueryRuleStats createQueryRuleStats(boolean match, long time)
    {
        return new QueryRuleStats(match, time);
    }
}
