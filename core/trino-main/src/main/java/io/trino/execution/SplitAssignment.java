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
import com.google.common.collect.ImmutableSet;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SplitAssignment
{
    private final PlanNodeId planNodeId;
    private final Set<ScheduledSplit> splits;
    private final boolean noMoreSplits;

    @JsonCreator
    public SplitAssignment(
            @JsonProperty("planNodeId") PlanNodeId planNodeId,
            @JsonProperty("splits") Set<ScheduledSplit> splits,
            @JsonProperty("noMoreSplits") boolean noMoreSplits)
    {
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.splits = ImmutableSet.copyOf(requireNonNull(splits, "splits is null"));
        this.noMoreSplits = noMoreSplits;
    }

    @JsonProperty
    public PlanNodeId getPlanNodeId()
    {
        return planNodeId;
    }

    @JsonProperty
    public Set<ScheduledSplit> getSplits()
    {
        return splits;
    }

    @JsonProperty
    public boolean isNoMoreSplits()
    {
        return noMoreSplits;
    }

    public SplitAssignment update(SplitAssignment assignment)
    {
        checkArgument(planNodeId.equals(assignment.getPlanNodeId()), "Expected assignment for node %s, but got assignment for node %s", planNodeId, assignment.getPlanNodeId());

        if (isNewer(assignment)) {
            // assure the new assignment is properly formed
            // we know that either the new assignment one has new splits and/or it is marking the assignment as closed
            checkArgument(!noMoreSplits || splits.containsAll(assignment.getSplits()), "Assignment %s has new splits, but no more splits already set", planNodeId);

            Set<ScheduledSplit> newSplits = ImmutableSet.<ScheduledSplit>builder()
                    .addAll(splits)
                    .addAll(assignment.getSplits())
                    .build();

            return new SplitAssignment(
                    planNodeId,
                    newSplits,
                    assignment.isNoMoreSplits());
        }
        // the specified assignment is older than this one
        return this;
    }

    private boolean isNewer(SplitAssignment assignment)
    {
        // the specified assignment is newer if it changes the no more
        // splits flag or if it contains new splits
        return (!noMoreSplits && assignment.isNoMoreSplits()) ||
                (!splits.containsAll(assignment.getSplits()));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("planNodeId", planNodeId)
                .add("splits", splits)
                .add("noMoreSplits", noMoreSplits)
                .toString();
    }
}
