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
package io.trino.sql.planner.assertions;

import com.google.common.base.MoreObjects.ToStringHelper;
import io.trino.Session;
import io.trino.cost.StatsProvider;
import io.trino.metadata.Metadata;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern.Ordering;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.trino.sql.planner.assertions.Util.orderingSchemeMatches;
import static java.util.Objects.requireNonNull;

final class ExchangeMatcher
        implements Matcher
{
    private final ExchangeNode.Scope scope;
    private final Optional<ExchangeNode.Type> type;
    private final Optional<PartitioningHandle> partitioningHandle;
    private final List<Ordering> orderBy;
    private final Set<String> partitionedBy;
    private final Optional<List<List<String>>> inputs;
    private final Optional<Optional<Integer>> partitionCount;

    public ExchangeMatcher(
            ExchangeNode.Scope scope,
            Optional<ExchangeNode.Type> type,
            Optional<PartitioningHandle> partitioningHandle,
            List<Ordering> orderBy,
            Set<String> partitionedBy,
            Optional<List<List<String>>> inputs,
            Optional<Optional<Integer>> partitionCount)
    {
        this.scope = requireNonNull(scope, "scope is null");
        this.type = requireNonNull(type, "type is null");
        this.partitioningHandle = requireNonNull(partitioningHandle, "partitioningHandle is null");
        this.orderBy = requireNonNull(orderBy, "orderBy is null");
        this.partitionedBy = requireNonNull(partitionedBy, "partitionedBy is null");
        this.inputs = requireNonNull(inputs, "inputs is null");
        this.partitionCount = requireNonNull(partitionCount, "partitionCount is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        if (!(node instanceof ExchangeNode exchangeNode)) {
            return false;
        }

        return exchangeNode.getScope() == scope
                && type.map(requiredType -> requiredType == exchangeNode.getType()).orElse(true)
                && partitioningHandle.map(handle -> handle.equals(exchangeNode.getPartitioningScheme().getPartitioning().getHandle())).orElse(true);
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
        ExchangeNode exchangeNode = (ExchangeNode) node;
        SymbolAliases outputSymbolAliases = symbolAliases;

        // Update symbol aliases if the exchange has multiple sources
        if (exchangeNode.getSources().size() > 1 && exchangeNode.getInputs().size() > 1) {
            SymbolAliases.Builder builder = SymbolAliases.builder();
            for (int i = 0; i < exchangeNode.getOutputSymbols().size(); i++) {
                Optional<Reference> reference = symbolAliases.getOptional(exchangeNode.getInputs().getFirst().get(i).name());
                Symbol outputSymbol = exchangeNode.getOutputSymbols().get(i);
                // Add the reference for output symbol if it does not match with the input symbol which
                // happens when there are multiple sources.
                if (reference.isPresent() && !outputSymbol.name().equals(reference.get().name())) {
                    builder.put(outputSymbol.name(), outputSymbol.toSymbolReference());
                }
            }
            outputSymbolAliases = symbolAliases.withNewAliases(builder.build());
        }

        if (!orderBy.isEmpty()) {
            if (exchangeNode.getOrderingScheme().isEmpty()) {
                return NO_MATCH;
            }

            if (!orderingSchemeMatches(orderBy, exchangeNode.getOrderingScheme().get(), symbolAliases)) {
                return NO_MATCH;
            }
        }

        if (!partitionedBy.isEmpty()) {
            Set<Symbol> partitionedColumns = exchangeNode.getPartitioningScheme().getPartitioning().getColumns();

            if (!partitionedBy.stream()
                    .map(outputSymbolAliases::get)
                    .map(Symbol::from)
                    .allMatch(partitionedColumns::contains)) {
                return NO_MATCH;
            }
        }

        if (inputs.isPresent()) {
            if (inputs.get().size() != exchangeNode.getInputs().size()) {
                return NO_MATCH;
            }
            for (int i = 0; i < exchangeNode.getInputs().size(); i++) {
                if (!inputs.get().get(i).stream()
                        .map(outputSymbolAliases::get)
                        .map(Symbol::from)
                        .collect(toImmutableList())
                        .equals(exchangeNode.getInputs().get(i))) {
                    return NO_MATCH;
                }
            }
        }

        if (partitionCount.isPresent()
                && !partitionCount.get().equals(exchangeNode.getPartitioningScheme().getPartitionCount())) {
            return NO_MATCH;
        }

        return MatchResult.match();
    }

    @Override
    public String toString()
    {
        ToStringHelper string = toStringHelper(this)
                .add("scope", scope)
                .add("type", type)
                .add("partitionHandle", partitioningHandle)
                .add("orderBy", orderBy)
                .add("partitionedBy", partitionedBy);
        inputs.ifPresent(inputs -> string.add("inputs", inputs));
        partitionCount.ifPresent(partitionCount -> string.add("partitionCount", partitionCount));
        return string.toString();
    }
}
