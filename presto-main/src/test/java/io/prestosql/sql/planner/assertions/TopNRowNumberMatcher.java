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
package io.prestosql.sql.planner.assertions;

import io.prestosql.Session;
import io.prestosql.cost.StatsProvider;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.TopNRowNumberNode;
import io.prestosql.sql.planner.plan.WindowNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.prestosql.sql.planner.assertions.MatchResult.match;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.node;
import static java.util.Objects.requireNonNull;

public class TopNRowNumberMatcher
        implements Matcher
{
    private final Optional<ExpectedValueProvider<WindowNode.Specification>> specification;
    private final Optional<SymbolAlias> rowNumberSymbol;
    private final Optional<Integer> maxRowCountPerPartition;
    private final Optional<Boolean> partial;
    private final Optional<Optional<SymbolAlias>> hashSymbol;

    private TopNRowNumberMatcher(
            Optional<ExpectedValueProvider<WindowNode.Specification>> specification,
            Optional<SymbolAlias> rowNumberSymbol,
            Optional<Integer> maxRowCountPerPartition,
            Optional<Boolean> partial,
            Optional<Optional<SymbolAlias>> hashSymbol)
    {
        this.specification = requireNonNull(specification, "specification is null");
        this.rowNumberSymbol = requireNonNull(rowNumberSymbol, "rowNumberSymbol is null");
        this.maxRowCountPerPartition = requireNonNull(maxRowCountPerPartition, "maxRowCountPerPartition is null");
        this.partial = requireNonNull(partial, "partial is null");
        this.hashSymbol = requireNonNull(hashSymbol, "hashSymbol is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof TopNRowNumberNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        TopNRowNumberNode topNRowNumberNode = (TopNRowNumberNode) node;

        if (specification.isPresent()) {
            WindowNode.Specification expected = specification.get().getExpectedValue(symbolAliases);
            if (!expected.equals(topNRowNumberNode.getSpecification())) {
                return NO_MATCH;
            }
        }

        if (rowNumberSymbol.isPresent()) {
            Symbol expected = rowNumberSymbol.get().toSymbol(symbolAliases);
            if (!expected.equals(topNRowNumberNode.getRowNumberSymbol())) {
                return NO_MATCH;
            }
        }

        if (maxRowCountPerPartition.isPresent()) {
            if (!maxRowCountPerPartition.get().equals(topNRowNumberNode.getMaxRowCountPerPartition())) {
                return NO_MATCH;
            }
        }

        if (partial.isPresent()) {
            if (!partial.get().equals(topNRowNumberNode.isPartial())) {
                return NO_MATCH;
            }
        }

        if (hashSymbol.isPresent()) {
            Optional<Symbol> expected = hashSymbol.get().map(alias -> alias.toSymbol(symbolAliases));
            if (!expected.equals(topNRowNumberNode.getHashSymbol())) {
                return NO_MATCH;
            }
        }

        return match();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("specification", specification)
                .add("rowNumberSymbol", rowNumberSymbol)
                .add("maxRowCountPerPartition", maxRowCountPerPartition)
                .add("partial", partial)
                .add("hashSymbol", hashSymbol)
                .toString();
    }

    public static class Builder
    {
        private final PlanMatchPattern source;
        private Optional<ExpectedValueProvider<WindowNode.Specification>> specification = Optional.empty();
        private Optional<SymbolAlias> rowNumberSymbol = Optional.empty();
        private Optional<Integer> maxRowCountPerPartition = Optional.empty();
        private Optional<Boolean> partial = Optional.empty();
        private Optional<Optional<SymbolAlias>> hashSymbol = Optional.empty();

        Builder(PlanMatchPattern source)
        {
            this.source = requireNonNull(source, "source is null");
        }

        public Builder specification(List<String> partitionBy, List<String> orderBy, Map<String, SortOrder> orderings)
        {
            this.specification = Optional.of(PlanMatchPattern.specification(partitionBy, orderBy, orderings));
            return this;
        }

        public Builder rowNumberSymbol(SymbolAlias rowNumberSymbol)
        {
            this.rowNumberSymbol = Optional.of(requireNonNull(rowNumberSymbol, "rowNumberSymbol is null"));
            return this;
        }

        public Builder maxRowCountPerPartition(int maxRowCountPerPartition)
        {
            this.maxRowCountPerPartition = Optional.of(maxRowCountPerPartition);
            return this;
        }

        public Builder partial(boolean partial)
        {
            this.partial = Optional.of(partial);
            return this;
        }

        public Builder hashSymbol(Optional<SymbolAlias> hashSymbol)
        {
            this.hashSymbol = Optional.of(requireNonNull(hashSymbol, "hashSymbol is null"));
            return this;
        }

        PlanMatchPattern build()
        {
            return node(TopNRowNumberNode.class, source).with(
                    new TopNRowNumberMatcher(
                            specification,
                            rowNumberSymbol,
                            maxRowCountPerPartition,
                            partial,
                            hashSymbol));
        }
    }
}
