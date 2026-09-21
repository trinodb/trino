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
package io.trino.sql.planner.runtimeconstraint;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public record RuntimeConstraintProducerGroup(
        ProducerGroupId groupId,
        PlanFragmentId originFragmentId,
        PlanNodeId originPlanNodeId,
        DistributedCompletionPolicy completionPolicy,
        List<RuntimeConstraintLane> collectedLanes,
        List<RuntimeConstraintDerivation> derivations,
        boolean replicated)
{
    public RuntimeConstraintProducerGroup(
            ProducerGroupId groupId,
            PlanFragmentId originFragmentId,
            PlanNodeId originPlanNodeId,
            DistributedCompletionPolicy completionPolicy,
            List<RuntimeConstraintLane> collectedLanes,
            List<RuntimeConstraintDerivation> derivations)
    {
        this(groupId, originFragmentId, originPlanNodeId, completionPolicy, collectedLanes, derivations, completionPolicy == DistributedCompletionPolicy.EQUIVALENT_REPLICAS);
    }

    public RuntimeConstraintProducerGroup
    {
        requireNonNull(groupId, "groupId is null");
        requireNonNull(originFragmentId, "originFragmentId is null");
        requireNonNull(originPlanNodeId, "originPlanNodeId is null");
        requireNonNull(completionPolicy, "completionPolicy is null");
        collectedLanes = ImmutableList.copyOf(requireNonNull(collectedLanes, "collectedLanes is null"));
        derivations = ImmutableList.copyOf(requireNonNull(derivations, "derivations is null"));
        checkArgument(!collectedLanes.isEmpty(), "collectedLanes is empty");
        checkArgument(!derivations.isEmpty(), "derivations is empty");
        checkArgument(collectedLanes.stream().map(RuntimeConstraintLane::index).distinct().count() == collectedLanes.size(), "collectedLanes contains duplicate indexes");
        checkArgument(derivations.stream().map(RuntimeConstraintDerivation::constraintId).distinct().count() == derivations.size(), "derivations contains duplicate constraint IDs");
        int laneCount = collectedLanes.size();
        checkArgument(derivations.stream()
                        .flatMap(derivation -> derivation.collectedLaneIndexes().stream())
                        .allMatch(index -> index < laneCount),
                "derivation references a missing collected lane");
    }
}
