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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
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

public class TaskUpdateRequest
{
    private final SessionRepresentation session;
    // extraCredentials is stored separately from SessionRepresentation to avoid being leaked
    private final Map<String, String> extraCredentials;
    private final Optional<PlanFragment> fragment;
    private final List<SplitAssignment> splitAssignments;
    private final OutputBuffers outputIds;
    private final Map<DynamicFilterId, Domain> dynamicFilterDomains;

    @JsonCreator
    public TaskUpdateRequest(
            @JsonProperty("session") SessionRepresentation session,
            @JsonProperty("extraCredentials") Map<String, String> extraCredentials,
            @JsonProperty("fragment") Optional<PlanFragment> fragment,
            @JsonProperty("splitAssignments") List<SplitAssignment> splitAssignments,
            @JsonProperty("outputIds") OutputBuffers outputIds,
            @JsonProperty("dynamicFilterDomains") Map<DynamicFilterId, Domain> dynamicFilterDomains)
    {
        requireNonNull(session, "session is null");
        requireNonNull(extraCredentials, "extraCredentials is null");
        requireNonNull(fragment, "fragment is null");
        requireNonNull(splitAssignments, "splitAssignments is null");
        requireNonNull(outputIds, "outputIds is null");
        requireNonNull(dynamicFilterDomains, "dynamicFilterDomains is null");

        this.session = session;
        this.extraCredentials = extraCredentials;
        this.fragment = fragment;
        this.splitAssignments = ImmutableList.copyOf(splitAssignments);
        this.outputIds = outputIds;
        this.dynamicFilterDomains = dynamicFilterDomains;
    }

    @JsonProperty
    public SessionRepresentation getSession()
    {
        return session;
    }

    @JsonProperty
    public Map<String, String> getExtraCredentials()
    {
        return extraCredentials;
    }

    @JsonProperty
    public Optional<PlanFragment> getFragment()
    {
        return fragment;
    }

    @JsonProperty
    public List<SplitAssignment> getSplitAssignments()
    {
        return splitAssignments;
    }

    @JsonProperty
    public OutputBuffers getOutputIds()
    {
        return outputIds;
    }

    @JsonProperty
    public Map<DynamicFilterId, Domain> getDynamicFilterDomains()
    {
        return dynamicFilterDomains;
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
                .toString();
    }
}
