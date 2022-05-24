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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.planprinter.TypedSymbol;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.sql.planner.plan.AggregationNode.Step;
import static io.trino.sql.planner.planprinter.anonymize.AnonymizationUtils.anonymize;
import static java.util.Objects.requireNonNull;

public class AggregationNodeRepresentation
        extends AnonymizedNodeRepresentation
{
    private final Map<Symbol, AggregationRepresentation> aggregations;
    private final List<Symbol> groupingKeys;
    private final Step step;
    private final Optional<Symbol> hashSymbol;

    @JsonCreator
    public AggregationNodeRepresentation(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("outputLayout") List<TypedSymbol> outputLayout,
            @JsonProperty("sources") List<AnonymizedNodeRepresentation> sources,
            @JsonProperty("aggregations") Map<Symbol, AggregationRepresentation> aggregations,
            @JsonProperty("groupingKeys") List<Symbol> groupingKeys,
            @JsonProperty("step") Step step,
            @JsonProperty("hashSymbol") Optional<Symbol> hashSymbol)
    {
        super(id, outputLayout, sources);
        this.aggregations = requireNonNull(aggregations, "aggregations is null");
        this.groupingKeys = requireNonNull(groupingKeys, "groupingKeys is null");
        this.step = requireNonNull(step, "step is null");
        this.hashSymbol = requireNonNull(hashSymbol, "hashSymbol is null");
    }

    @JsonProperty
    public Map<Symbol, AggregationRepresentation> getAggregations()
    {
        return aggregations;
    }

    @JsonProperty
    public List<Symbol> getGroupingKeys()
    {
        return groupingKeys;
    }

    @JsonProperty
    public Step getStep()
    {
        return step;
    }

    @JsonProperty
    public Optional<Symbol> getHashSymbol()
    {
        return hashSymbol;
    }

    public static AggregationNodeRepresentation fromPlanNode(
            AggregationNode node,
            TypeProvider typeProvider,
            List<AnonymizedNodeRepresentation> sources)
    {
        return new AggregationNodeRepresentation(
                node.getId(),
                anonymize(node.getOutputSymbols(), typeProvider),
                sources,
                node.getAggregations().entrySet().stream()
                        .collect(toImmutableMap(
                                entry -> anonymize(entry.getKey()),
                                entry -> anonymize(entry.getValue()))),
                anonymize(node.getGroupingKeys()),
                node.getStep(),
                node.getHashSymbol().map(AnonymizationUtils::anonymize));
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getId(), getOutputLayout(), getSources(), aggregations, groupingKeys, step, hashSymbol);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if ((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        AggregationNodeRepresentation that = (AggregationNodeRepresentation) o;
        return getId().equals(that.getId())
                && getOutputLayout().equals(that.getOutputLayout())
                && getSources().equals(that.getSources())
                && aggregations.equals(that.aggregations)
                && groupingKeys.equals(that.groupingKeys)
                && step.equals(that.step)
                && hashSymbol.equals(that.hashSymbol);
    }

    public static class AggregationRepresentation
    {
        private final String resolvedFunction;
        private final List<String> arguments;
        private final boolean distinct;
        private final Optional<Symbol> filter;
        private final Optional<Map<Symbol, SortOrder>> orderings;
        private final Optional<Symbol> mask;

        @JsonCreator
        public AggregationRepresentation(
                @JsonProperty("resolvedFunction") String resolvedFunction,
                @JsonProperty("arguments") List<String> arguments,
                @JsonProperty("distinct") boolean distinct,
                @JsonProperty("filter") Optional<Symbol> filter,
                @JsonProperty("orderings") Optional<Map<Symbol, SortOrder>> orderings,
                @JsonProperty("mask") Optional<Symbol> mask)
        {
            this.resolvedFunction = requireNonNull(resolvedFunction, "resolvedFunction is null");
            this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
            this.distinct = distinct;
            this.filter = requireNonNull(filter, "filter is null");
            this.orderings = requireNonNull(orderings, "orderings is null");
            this.mask = requireNonNull(mask, "mask is null");
        }

        @JsonProperty
        public String getResolvedFunction()
        {
            return resolvedFunction;
        }

        @JsonProperty
        public List<String> getArguments()
        {
            return arguments;
        }

        @JsonProperty
        public boolean isDistinct()
        {
            return distinct;
        }

        @JsonProperty
        public Optional<Symbol> getFilter()
        {
            return filter;
        }

        @JsonProperty
        public Optional<Map<Symbol, SortOrder>> getOrderings()
        {
            return orderings;
        }

        @JsonProperty
        public Optional<Symbol> getMask()
        {
            return mask;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(resolvedFunction, arguments, distinct, filter, orderings, mask);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if ((o == null) || (getClass() != o.getClass())) {
                return false;
            }
            AggregationRepresentation that = (AggregationRepresentation) o;
            return resolvedFunction.equals(that.resolvedFunction)
                    && arguments.equals(that.arguments)
                    && distinct == that.distinct
                    && filter.equals(that.filter)
                    && orderings.equals(that.orderings)
                    && mask.equals(that.mask);
        }
    }
}
