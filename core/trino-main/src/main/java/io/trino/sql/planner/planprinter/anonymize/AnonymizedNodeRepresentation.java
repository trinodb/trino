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

package io.trino.sql.planner.planprinter.anonymize;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.planprinter.TypedSymbol;

import java.util.List;

import static java.util.Objects.requireNonNull;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DefaultNodeRepresentation.class, name = "default"),
        @JsonSubTypes.Type(value = ProjectNodeRepresentation.class, name = "project"),
        @JsonSubTypes.Type(value = TableScanNodeRepresentation.class, name = "tableScan"),
        @JsonSubTypes.Type(value = ValuesNodeRepresentation.class, name = "values"),
        @JsonSubTypes.Type(value = AggregationNodeRepresentation.class, name = "aggregation"),
        @JsonSubTypes.Type(value = FilterNodeRepresentation.class, name = "filter"),
        @JsonSubTypes.Type(value = WindowNodeRepresentation.class, name = "window"),
        @JsonSubTypes.Type(value = TopNNodeRepresentation.class, name = "topN"),
        @JsonSubTypes.Type(value = SortNodeRepresentation.class, name = "sort"),
        @JsonSubTypes.Type(value = RemoteSourceNodeRepresentation.class, name = "remoteSource"),
        @JsonSubTypes.Type(value = ExchangeNodeRepresentation.class, name = "exchange"),
        @JsonSubTypes.Type(value = JoinNodeRepresentation.class, name = "join"),
        @JsonSubTypes.Type(value = SemiJoinNodeRepresentation.class, name = "semiJoin"),
})
public abstract class AnonymizedNodeRepresentation
{
    private final PlanNodeId id;
    private final List<TypedSymbol> outputLayout;
    private final List<AnonymizedNodeRepresentation> sources;

    public AnonymizedNodeRepresentation(
            PlanNodeId id,
            List<TypedSymbol> outputLayout,
            List<AnonymizedNodeRepresentation> sources)
    {
        this.id = requireNonNull(id, "id is null");
        this.outputLayout = requireNonNull(outputLayout, "outputLayout is null");
        this.sources = requireNonNull(sources, "sources is null");
    }

    @JsonProperty
    public PlanNodeId getId()
    {
        return id;
    }

    @JsonProperty
    public List<TypedSymbol> getOutputLayout()
    {
        return outputLayout;
    }

    @JsonProperty
    public List<AnonymizedNodeRepresentation> getSources()
    {
        return sources;
    }
}
