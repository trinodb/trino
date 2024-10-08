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
import io.trino.spi.predicate.Domain;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.DynamicFilterId;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * @param extraCredentials extraCredentials is stored separately from SessionRepresentation to avoid being leaked
 */
public record TaskUpdateRequest(
        SessionRepresentation session,
        Map<String, String> extraCredentials,
        Span stageSpan,
        Optional<PlanFragment> fragment,
        List<SplitAssignment> splitAssignments,
        OutputBuffers outputIds,
        Map<DynamicFilterId, Domain> dynamicFilterDomains,
        Optional<Slice> exchangeEncryptionKey,
        boolean speculative)
{
    public TaskUpdateRequest
    {
        requireNonNull(session, "session is null");
        requireNonNull(extraCredentials, "extraCredentials is null");
        requireNonNull(stageSpan, "stageSpan is null");
        requireNonNull(fragment, "fragment is null");
        splitAssignments = ImmutableList.copyOf(splitAssignments);
        requireNonNull(outputIds, "outputIds is null");
        dynamicFilterDomains = ImmutableMap.copyOf(dynamicFilterDomains);
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
                .add("dynamicFilterDomains", dynamicFilterDomains)
                .add("exchangeEncryptionKey", exchangeEncryptionKey.map(_ -> "[REDACTED]"))
                .add("speculative", speculative)
                .toString();
    }
}
