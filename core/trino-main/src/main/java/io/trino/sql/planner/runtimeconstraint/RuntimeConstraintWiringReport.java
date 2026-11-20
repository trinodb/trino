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
import io.trino.operator.RuntimeConstraintRequest;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record RuntimeConstraintWiringReport(
        List<ScanWiring> scans,
        List<Source> sources,
        List<RemoteRequest> remoteRequests,
        List<RuntimeConstraintRequest> appliedOutputRequests,
        List<RuntimeConstraintRequest> rejectedOutputRequests,
        List<RuntimeConstraintSubscription> subscriptions,
        List<RuntimeConstraintSubscription.Input> subscriptionInputs)
{
    public static final RuntimeConstraintWiringReport EMPTY = new RuntimeConstraintWiringReport(ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), ImmutableList.of());

    public RuntimeConstraintWiringReport(List<ScanWiring> scans)
    {
        this(scans, ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), ImmutableList.of());
    }

    public RuntimeConstraintWiringReport(List<ScanWiring> scans, List<Source> sources)
    {
        this(scans, sources, ImmutableList.of(), ImmutableList.of(), ImmutableList.of());
    }

    public RuntimeConstraintWiringReport(List<ScanWiring> scans, List<Source> sources, List<RemoteRequest> remoteRequests)
    {
        this(scans, sources, remoteRequests, ImmutableList.of(), ImmutableList.of());
    }

    public RuntimeConstraintWiringReport(
            List<ScanWiring> scans,
            List<Source> sources,
            List<RemoteRequest> remoteRequests,
            List<RuntimeConstraintRequest> appliedOutputRequests)
    {
        this(scans, sources, remoteRequests, appliedOutputRequests, ImmutableList.of());
    }

    public RuntimeConstraintWiringReport(
            List<ScanWiring> scans,
            List<Source> sources,
            List<RemoteRequest> remoteRequests,
            List<RuntimeConstraintRequest> appliedOutputRequests,
            List<RuntimeConstraintRequest> rejectedOutputRequests)
    {
        this(scans, sources, remoteRequests, appliedOutputRequests, rejectedOutputRequests, ImmutableList.of(), ImmutableList.of());
    }

    public RuntimeConstraintWiringReport
    {
        subscriptions = subscriptions == null ? ImmutableList.of() : ImmutableList.copyOf(subscriptions);
        subscriptionInputs = subscriptionInputs == null ? ImmutableList.of() : ImmutableList.copyOf(subscriptionInputs);
        scans = ImmutableList.copyOf(requireNonNull(scans, "scans is null"));
        sources = ImmutableList.copyOf(requireNonNull(sources, "sources is null"));
        remoteRequests = remoteRequests == null ? ImmutableList.of() : ImmutableList.copyOf(remoteRequests);
        appliedOutputRequests = appliedOutputRequests == null ? ImmutableList.of() : ImmutableList.copyOf(appliedOutputRequests);
        rejectedOutputRequests = rejectedOutputRequests == null ? ImmutableList.of() : ImmutableList.copyOf(rejectedOutputRequests);
    }

    public record CollectedConstraint(RuntimeConstraintId constraintId, ComparisonOperator operator, boolean nullAllowed, int collectedLaneIndex)
    {
        public CollectedConstraint(RuntimeConstraintId constraintId, int collectedLaneIndex)
        {
            this(constraintId, ComparisonOperator.EQUAL, false, collectedLaneIndex);
        }

        public CollectedConstraint
        {
            requireNonNull(constraintId, "constraintId is null");
            requireNonNull(operator, "operator is null");
            if (collectedLaneIndex < 0) {
                throw new IllegalArgumentException("collectedLaneIndex is negative");
            }
        }
    }

    public record Source(
            PlanNodeId sourceId,
            List<CollectedConstraint> constraints,
            List<Type> types,
            DistributedCompletionPolicy completionPolicy,
            boolean replicated)
    {
        public Source(PlanNodeId sourceId, List<CollectedConstraint> constraints, List<Type> types)
        {
            this(sourceId, constraints, types, DistributedCompletionPolicy.UNION_ALL_PARTITIONS);
        }

        public Source(PlanNodeId sourceId, List<CollectedConstraint> constraints, List<Type> types, DistributedCompletionPolicy completionPolicy)
        {
            this(sourceId, constraints, types, completionPolicy, completionPolicy == DistributedCompletionPolicy.EQUIVALENT_REPLICAS);
        }

        public Source
        {
            requireNonNull(sourceId, "sourceId is null");
            constraints = ImmutableList.copyOf(requireNonNull(constraints, "constraints is null"));
            types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            completionPolicy = completionPolicy == null ? DistributedCompletionPolicy.UNION_ALL_PARTITIONS : completionPolicy;
            int laneCount = types.size();
            if (constraints.stream().anyMatch(constraint -> constraint.collectedLaneIndex() >= laneCount)) {
                throw new IllegalArgumentException("runtime constraint source lane index is out of bounds");
            }
        }

        public List<RuntimeConstraintId> constraintIds()
        {
            return constraints.stream().map(CollectedConstraint::constraintId).toList();
        }
    }

    public record ScanWiring(PlanNodeId scanId, List<Binding> bindings)
    {
        public ScanWiring
        {
            requireNonNull(scanId, "scanId is null");
            bindings = ImmutableList.copyOf(requireNonNull(bindings, "bindings is null"));
        }
    }

    public record Binding(RuntimeConstraintRequest request, ColumnHandle column)
    {
        public Binding(RuntimeConstraintId constraintId, ColumnHandle column)
        {
            this(new RuntimeConstraintRequest(constraintId, 0), column);
        }

        public Binding
        {
            requireNonNull(request, "request is null");
            if (!request.isConstraint()) {
                throw new IllegalArgumentException("scan binding must contain a constraint request");
            }
            requireNonNull(column, "column is null");
        }

        public RuntimeConstraintId constraintId()
        {
            return request.constraintId();
        }
    }

    public record RemoteRequest(List<PlanFragmentId> sourceFragmentIds, RuntimeConstraintRequest request)
    {
        public RemoteRequest
        {
            sourceFragmentIds = ImmutableList.copyOf(requireNonNull(sourceFragmentIds, "sourceFragmentIds is null"));
            if (sourceFragmentIds.isEmpty()) {
                throw new IllegalArgumentException("sourceFragmentIds is empty");
            }
            requireNonNull(request, "request is null");
        }
    }
}
