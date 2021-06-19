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

import io.trino.Session;
import io.trino.cost.StatsProvider;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.RowNumberNode;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.trino.sql.planner.assertions.MatchResult.match;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static java.util.Objects.requireNonNull;

public class RowNumberMatcher
        implements Matcher
{
    private final Optional<List<SymbolAlias>> partitionBy;
    private final Optional<Optional<Integer>> maxRowCountPerPartition;
    private final Optional<SymbolAlias> rowNumberSymbol;
    private final Optional<Optional<SymbolAlias>> hashSymbol;
    private final Optional<Boolean> orderSensitive;

    private RowNumberMatcher(
            Optional<List<SymbolAlias>> partitionBy,
            Optional<Optional<Integer>> maxRowCountPerPartition,
            Optional<SymbolAlias> rowNumberSymbol,
            Optional<Optional<SymbolAlias>> hashSymbol,
            Optional<Boolean> orderSensitive)
    {
        this.partitionBy = requireNonNull(partitionBy, "partitionBy is null");
        this.maxRowCountPerPartition = requireNonNull(maxRowCountPerPartition, "maxRowCountPerPartition is null");
        this.rowNumberSymbol = requireNonNull(rowNumberSymbol, "rowNumberSymbol is null");
        this.hashSymbol = requireNonNull(hashSymbol, "hashSymbol is null");
        this.orderSensitive = requireNonNull(orderSensitive, "orderSensitive is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof RowNumberNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        RowNumberNode rowNumberNode = (RowNumberNode) node;

        if (partitionBy.isPresent()) {
            List<Symbol> expected = partitionBy.get().stream()
                    .map(alias -> alias.toSymbol(symbolAliases))
                    .collect(toImmutableList());

            if (!expected.equals(rowNumberNode.getPartitionBy())) {
                return NO_MATCH;
            }
        }

        if (rowNumberSymbol.isPresent()) {
            Symbol expected = rowNumberSymbol.get().toSymbol(symbolAliases);
            if (!expected.equals(rowNumberNode.getRowNumberSymbol())) {
                return NO_MATCH;
            }
        }

        if (maxRowCountPerPartition.isPresent()) {
            if (!maxRowCountPerPartition.get().equals(rowNumberNode.getMaxRowCountPerPartition())) {
                return NO_MATCH;
            }
        }

        if (hashSymbol.isPresent()) {
            Optional<Symbol> expected = hashSymbol.get().map(alias -> alias.toSymbol(symbolAliases));
            if (!expected.equals(rowNumberNode.getHashSymbol())) {
                return NO_MATCH;
            }
        }

        if (orderSensitive.isPresent()) {
            if (!orderSensitive.get().equals(rowNumberNode.isOrderSensitive())) {
                return NO_MATCH;
            }
        }

        return match();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitionBy", partitionBy)
                .add("maxRowCountPerPartition", maxRowCountPerPartition)
                .add("rowNumberSymbol", rowNumberSymbol)
                .add("hashSymbol", hashSymbol)
                .add("orderSensitive", orderSensitive)
                .toString();
    }

    /**
     * By default, matches any RowNumberNode.  Users add additional constraints by
     * calling the various member functions of the Builder, typically named according
     * to the field names of RowNumberNode.
     */
    public static class Builder
    {
        private final PlanMatchPattern source;
        private Optional<List<SymbolAlias>> partitionBy = Optional.empty();
        private Optional<Optional<Integer>> maxRowCountPerPartition = Optional.empty();
        private Optional<SymbolAlias> rowNumberSymbol = Optional.empty();
        private Optional<Optional<SymbolAlias>> hashSymbol = Optional.empty();
        private Optional<Boolean> orderSensitive = Optional.empty();

        Builder(PlanMatchPattern source)
        {
            this.source = requireNonNull(source, "source is null");
        }

        public Builder partitionBy(List<String> partitionBy)
        {
            requireNonNull(partitionBy, "partitionBy is null");
            this.partitionBy = Optional.of(partitionBy.stream()
                    .map(SymbolAlias::new)
                    .collect(toImmutableList()));
            return this;
        }

        public Builder maxRowCountPerPartition(Optional<Integer> maxRowCountPerPartition)
        {
            this.maxRowCountPerPartition = Optional.of(requireNonNull(maxRowCountPerPartition, "maxRowCountPerPartition is null"));
            return this;
        }

        public Builder rowNumberSymbol(SymbolAlias rowNumberSymbol)
        {
            this.rowNumberSymbol = Optional.of(requireNonNull(rowNumberSymbol, "rowNumberSymbol is null"));
            return this;
        }

        public Builder hashSymbol(Optional<String> hashSymbol)
        {
            requireNonNull(hashSymbol, "hashSymbol is null");
            this.hashSymbol = Optional.of(hashSymbol.map(SymbolAlias::new));
            return this;
        }

        public Builder orderSensitive(boolean isOrderSensitive)
        {
            this.orderSensitive = Optional.of(isOrderSensitive);
            return this;
        }

        PlanMatchPattern build()
        {
            return node(RowNumberNode.class, source).with(
                    new RowNumberMatcher(
                            partitionBy,
                            maxRowCountPerPartition,
                            rowNumberSymbol,
                            hashSymbol,
                            orderSensitive));
        }
    }
}
