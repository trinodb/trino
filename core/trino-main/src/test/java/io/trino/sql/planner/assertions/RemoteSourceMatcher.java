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
import io.trino.operator.RetryPolicy;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.RemoteSourceNode;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.trino.sql.planner.assertions.MatchResult.match;
import static java.util.Objects.requireNonNull;

public class RemoteSourceMatcher
        implements Matcher
{
    private final List<PlanFragmentId> sourceFragmentIds;
    private final Optional<List<String>> outputSymbols;
    private final Optional<OrderingScheme> orderingScheme;
    private final Optional<ExchangeNode.Type> exchangeType;
    private final Optional<RetryPolicy> retryPolicy;

    public RemoteSourceMatcher(
            List<PlanFragmentId> sourceFragmentIds,
            Optional<List<String>> outputSymbols,
            Optional<OrderingScheme> orderingScheme,
            Optional<ExchangeNode.Type> exchangeType,
            Optional<RetryPolicy> retryPolicy)
    {
        this.sourceFragmentIds = requireNonNull(sourceFragmentIds, "sourceFragmentIds is null");
        this.outputSymbols = requireNonNull(outputSymbols, "outputSymbols is null");
        this.orderingScheme = requireNonNull(orderingScheme, "orderingScheme is null");
        this.exchangeType = requireNonNull(exchangeType, "exchangeType is null");
        this.retryPolicy = requireNonNull(retryPolicy, "retryPolicy is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof RemoteSourceNode && sourceFragmentIds.equals(((RemoteSourceNode) node).getSourceFragmentIds());
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
        RemoteSourceNode remoteSourceNode = (RemoteSourceNode) node;

        if (orderingScheme.isPresent() && !remoteSourceNode.getOrderingScheme().equals(orderingScheme)) {
            return NO_MATCH;
        }

        if (exchangeType.isPresent() && !remoteSourceNode.getExchangeType().equals(exchangeType.get())) {
            return NO_MATCH;
        }

        if (retryPolicy.isPresent() && !remoteSourceNode.getRetryPolicy().equals(retryPolicy.get())) {
            return NO_MATCH;
        }

        SymbolAliases.Builder newAliases = SymbolAliases.builder();
        if (outputSymbols.isPresent()) {
            List<String> actualSymbols = remoteSourceNode.getOutputSymbols().stream()
                    .map(Symbol::name)
                    .collect(toImmutableList());
            if (!outputSymbols.get().equals(actualSymbols)) {
                return NO_MATCH;
            }
            remoteSourceNode.getOutputSymbols().stream()
                    .map(Symbol::toSymbolReference)
                    .forEach(reference -> newAliases.put(reference.name(), reference));
        }
        return match(newAliases.build());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("sourceFragmentIds", sourceFragmentIds)
                .toString();
    }
}
