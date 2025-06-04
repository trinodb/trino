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
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.trino.sql.planner.assertions.MatchResult.match;
import static java.util.Objects.requireNonNull;

public class CorrelationMatcher
        implements Matcher
{
    private final List<String> correlation;

    CorrelationMatcher(List<String> correlation)
    {
        this.correlation = requireNonNull(correlation, "correlation is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof ApplyNode || node instanceof CorrelatedJoinNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(
                shapeMatches(node),
                "Plan testing framework error: shapeMatches returned false in detailMatches in %s",
                this.getClass().getName());

        List<Symbol> actualCorrelation = getCorrelation(node);
        if (this.correlation.size() != actualCorrelation.size()) {
            return NO_MATCH;
        }

        int i = 0;
        for (String alias : this.correlation) {
            if (!symbolAliases.get(alias).equals(actualCorrelation.get(i++).toSymbolReference())) {
                return NO_MATCH;
            }
        }
        return match();
    }

    private List<Symbol> getCorrelation(PlanNode node)
    {
        if (node instanceof ApplyNode applyNode) {
            return applyNode.getCorrelation();
        }
        if (node instanceof CorrelatedJoinNode correlatedJoinNode) {
            return correlatedJoinNode.getCorrelation();
        }
        throw new IllegalStateException("Unexpected plan node: " + node);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("correlation", correlation)
                .toString();
    }
}
