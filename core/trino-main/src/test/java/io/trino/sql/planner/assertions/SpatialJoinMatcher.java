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
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.SpatialJoinNode.Type;
import io.trino.sql.tree.Expression;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.trino.sql.planner.assertions.MatchResult.match;
import static java.util.Objects.requireNonNull;

public class SpatialJoinMatcher
        implements Matcher
{
    private final Type type;
    private final Expression filter;
    private final Optional<String> kdbTree;
    private final Optional<List<String>> outputSymbols;

    public SpatialJoinMatcher(Type type, Expression filter, Optional<String> kdbTree, Optional<List<String>> outputSymbols)
    {
        this.type = type;
        this.filter = requireNonNull(filter, "filter cannot be null");
        this.kdbTree = requireNonNull(kdbTree, "kdbTree cannot be null");
        this.outputSymbols = requireNonNull(outputSymbols, "outputSymbols is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        if (!(node instanceof SpatialJoinNode joinNode)) {
            return false;
        }

        return joinNode.getType() == type;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        SpatialJoinNode joinNode = (SpatialJoinNode) node;
        if (!new ExpressionVerifier(symbolAliases).process(joinNode.getFilter(), filter)) {
            return NO_MATCH;
        }
        if (!joinNode.getKdbTree().equals(kdbTree)) {
            return NO_MATCH;
        }
        if (outputSymbols.isPresent()) {
            if (outputSymbols.get().size() != joinNode.getOutputSymbols().size()) {
                return NO_MATCH;
            }
            if (!outputSymbols.get().stream()
                    .map(symbolAliases::get)
                    .map(Symbol::from)
                    .collect(toImmutableList())
                    .equals(joinNode.getOutputSymbols())) {
                return NO_MATCH;
            }
        }
        return match();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("filter", filter)
                .toString();
    }
}
