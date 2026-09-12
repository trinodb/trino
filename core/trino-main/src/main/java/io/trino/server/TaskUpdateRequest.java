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
package io.trino.server;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.opentelemetry.api.trace.Span;
import io.trino.SessionRepresentation;
import io.trino.execution.SplitAssignment;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.operator.RuntimeConstraintRequest;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.predicate.Domain;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintUpdateBatch;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/// @param extraCredentials extraCredentials is stored separately from SessionRepresentation to avoid being leaked
public record TaskUpdateRequest(
        SessionRepresentation session,
        Map<String, String> extraCredentials,
        Span stageSpan,
        Optional<PlanFragment> fragment,
        Map<PlanNodeId, ConnectorTableCredentials> tableCredentials,
        List<SplitAssignment> splitAssignments,
        OutputBuffers outputIds,
        Map<DynamicFilterId, Domain> dynamicFilterDomains,
        Optional<Slice> exchangeEncryptionKey,
        boolean speculative,
        List<RuntimeConstraintRequest> runtimeConstraintWiringRequests,
        Optional<RuntimeConstraintUpdateBatch> runtimeConstraintUpdates,
        long runtimeConstraintContributionAcknowledgement)
{
    public TaskUpdateRequest(
            SessionRepresentation session,
            Map<String, String> extraCredentials,
            Span stageSpan,
            Optional<PlanFragment> fragment,
            Map<PlanNodeId, ConnectorTableCredentials> tableCredentials,
            List<SplitAssignment> splitAssignments,
            OutputBuffers outputIds,
            Map<DynamicFilterId, Domain> dynamicFilterDomains,
            Optional<Slice> exchangeEncryptionKey,
            boolean speculative)
    {
        this(session, extraCredentials, stageSpan, fragment, tableCredentials, splitAssignments, outputIds, dynamicFilterDomains, exchangeEncryptionKey, speculative, ImmutableList.of(), Optional.empty(), 0);
    }

    public TaskUpdateRequest(
            SessionRepresentation session,
            Map<String, String> extraCredentials,
            Span stageSpan,
            Optional<PlanFragment> fragment,
            Map<PlanNodeId, ConnectorTableCredentials> tableCredentials,
            List<SplitAssignment> splitAssignments,
            OutputBuffers outputIds,
            List<RuntimeConstraintRequest> runtimeConstraintWiringRequests,
            Optional<RuntimeConstraintUpdateBatch> runtimeConstraintUpdates,
            long runtimeConstraintContributionAcknowledgement,
            Optional<Slice> exchangeEncryptionKey,
            boolean speculative)
    {
        this(session, extraCredentials, stageSpan, fragment, tableCredentials, splitAssignments, outputIds, ImmutableMap.of(), exchangeEncryptionKey, speculative, runtimeConstraintWiringRequests, runtimeConstraintUpdates, runtimeConstraintContributionAcknowledgement);
    }

    public TaskUpdateRequest
    {
        requireNonNull(session, "session is null");
        requireNonNull(extraCredentials, "extraCredentials is null");
        requireNonNull(stageSpan, "stageSpan is null");
        requireNonNull(fragment, "fragment is null");
        dynamicFilterDomains = dynamicFilterDomains == null ? ImmutableMap.of() : ImmutableMap.copyOf(dynamicFilterDomains);
        tableCredentials = ImmutableMap.copyOf(tableCredentials);
        splitAssignments = ImmutableList.copyOf(splitAssignments);
        requireNonNull(outputIds, "outputIds is null");
        runtimeConstraintWiringRequests = runtimeConstraintWiringRequests == null ? ImmutableList.of() : ImmutableList.copyOf(runtimeConstraintWiringRequests);
        runtimeConstraintUpdates = runtimeConstraintUpdates == null ? Optional.empty() : runtimeConstraintUpdates;
        checkArgument(runtimeConstraintContributionAcknowledgement >= 0, "runtimeConstraintContributionAcknowledgement is negative");
        requireNonNull(exchangeEncryptionKey, "exchangeEncryptionKey is null");
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("session", session)
                .add("extraCredentials", extraCredentials.keySet())
                .add("fragment", fragment)
                .add("splitAssignments", splitAssignments)
                .add("outputIds", outputIds)
                .add("runtimeConstraintWiringRequests", runtimeConstraintWiringRequests)
                .add("runtimeConstraintUpdates", runtimeConstraintUpdates)
                .add("runtimeConstraintContributionAcknowledgement", runtimeConstraintContributionAcknowledgement)
                .add("exchangeEncryptionKey", exchangeEncryptionKey.map(_ -> "[REDACTED]"))
                .add("speculative", speculative)
                .toString();
    }
}
