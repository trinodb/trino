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
package io.trino.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@Immutable
public class IndexJoinNode
        extends PlanNode
{
    private final Type type;
    private final PlanNode probeSource;
    private final PlanNode indexSource;
    private final List<EquiJoinClause> criteria;

    @JsonCreator
    public IndexJoinNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") Type type,
            @JsonProperty("probeSource") PlanNode probeSource,
            @JsonProperty("indexSource") PlanNode indexSource,
            @JsonProperty("criteria") List<EquiJoinClause> criteria)
    {
        super(id);
        this.type = requireNonNull(type, "type is null");
        this.probeSource = requireNonNull(probeSource, "probeSource is null");
        this.indexSource = requireNonNull(indexSource, "indexSource is null");
        this.criteria = ImmutableList.copyOf(requireNonNull(criteria, "criteria is null"));
    }

    public enum Type
    {
        INNER("Inner"),
        SOURCE_OUTER("SourceOuter");

        private final String joinLabel;

        Type(String joinLabel)
        {
            this.joinLabel = joinLabel;
        }

        public String getJoinLabel()
        {
            return joinLabel;
        }
    }

    @JsonProperty("type")
    public Type getType()
    {
        return type;
    }

    @JsonProperty("probeSource")
    public PlanNode getProbeSource()
    {
        return probeSource;
    }

    @JsonProperty("indexSource")
    public PlanNode getIndexSource()
    {
        return indexSource;
    }

    @JsonProperty("criteria")
    public List<EquiJoinClause> getCriteria()
    {
        return criteria;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(probeSource, indexSource);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.<Symbol>builder()
                .addAll(probeSource.getOutputSymbols())
                .addAll(indexSource.getOutputSymbols())
                .build();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitIndexJoin(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
        return new IndexJoinNode(getId(), type, newChildren.get(0), newChildren.get(1), criteria);
    }

    public static class EquiJoinClause
    {
        private final Symbol probe;
        private final Symbol index;

        @JsonCreator
        public EquiJoinClause(@JsonProperty("probe") Symbol probe, @JsonProperty("index") Symbol index)
        {
            this.probe = requireNonNull(probe, "probe is null");
            this.index = requireNonNull(index, "index is null");
        }

        @JsonProperty("probe")
        public Symbol getProbe()
        {
            return probe;
        }

        @JsonProperty("index")
        public Symbol getIndex()
        {
            return index;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if (obj == null || !this.getClass().equals(obj.getClass())) {
                return false;
            }

            IndexJoinNode.EquiJoinClause other = (IndexJoinNode.EquiJoinClause) obj;

            return Objects.equals(this.probe, other.probe) &&
                    Objects.equals(this.index, other.index);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(probe, index);
        }

        @Override
        public String toString()
        {
            return format("%s = %s", probe, index);
        }
    }
}
