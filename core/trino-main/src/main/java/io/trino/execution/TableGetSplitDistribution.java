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
package io.trino.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.stats.Distribution.DistributionSnapshot;
import io.trino.sql.planner.plan.PlanNodeId;

import javax.annotation.concurrent.Immutable;

import static java.util.Objects.requireNonNull;

@Immutable
public class TableGetSplitDistribution
{
    private final PlanNodeId planNodeId;
    private final DistributionSnapshot splitDistribution;

    @JsonCreator
    public TableGetSplitDistribution(
            @JsonProperty("planNodeId") PlanNodeId planNodeId,
            @JsonProperty("splitDistribution") DistributionSnapshot splitDistribution)
    {
        requireNonNull(planNodeId, "planNodeId is null");
        requireNonNull(splitDistribution, "splitDistribution is null");
        this.planNodeId = planNodeId;
        this.splitDistribution = splitDistribution;
    }

    @JsonProperty
    public PlanNodeId getPlanNodeId()
    {
        return planNodeId;
    }

    @JsonProperty
    public DistributionSnapshot getSplitDistribution()
    {
        return splitDistribution;
    }
}
