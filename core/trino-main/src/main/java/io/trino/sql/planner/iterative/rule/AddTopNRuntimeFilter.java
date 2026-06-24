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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.type.Type;
import io.trino.sql.DynamicFilters;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TopNNode;

import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.isEnableDynamicFiltering;
import static io.trino.matching.Capture.newCapture;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.Patterns.exchange;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.topN;
import static io.trino.sql.planner.plan.TopNNode.Step.PARTIAL;
import static java.util.Objects.requireNonNull;

public class AddTopNRuntimeFilter
        implements Rule<TopNNode>
{
    private static final long MAX_SUPPORTED_LIMIT = 10_000;

    private static final Capture<ExchangeNode> EXCHANGE = newCapture();

    private static final Pattern<TopNNode> PATTERN = topN()
            .matching(node -> node.getStep() != PARTIAL)
            .matching(node -> node.getRuntimeFilter().isEmpty())
            .matching(node -> node.getOrderingScheme().orderBy().size() == 1)
            .with(source().matching(exchange()
                    .matching(exchange -> exchange.getSources().size() == 1)
                    .capturedAs(EXCHANGE)));

    private final PlannerContext plannerContext;

    public AddTopNRuntimeFilter(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public Pattern<TopNNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isEnableDynamicFiltering(session);
    }

    @Override
    public Result apply(TopNNode node, Captures captures, Context context)
    {
        Symbol sortSymbol = getOnlyElement(node.getOrderingScheme().orderBy());
        if (node.getCount() == 0) {
            return Result.empty();
        }
        if (node.getCount() > MAX_SUPPORTED_LIMIT) {
            return Result.empty();
        }

        Type type = sortSymbol.type();
        if (!type.isOrderable() || type.equals(REAL) || type.equals(DOUBLE)) {
            return Result.empty();
        }

        ExchangeNode exchange = captures.get(EXCHANGE);
        DynamicFilterId dynamicFilterId = new DynamicFilterId("topn_rf_" + context.getIdAllocator().getNextId());
        PlanNode filteredSource = addRuntimeFilter(exchange, sortSymbol, dynamicFilterId, context);
        if (filteredSource == exchange) {
            return Result.empty();
        }

        return Result.ofPlanNode(new TopNNode(
                node.getId(),
                filteredSource,
                node.getCount(),
                node.getOrderingScheme(),
                node.getStep(),
                Optional.of(new TopNNode.RuntimeFilter(dynamicFilterId, sortSymbol))));
    }

    private PlanNode addRuntimeFilter(PlanNode node, Symbol symbol, DynamicFilterId dynamicFilterId, Context context)
    {
        PlanNode resolved = context.getLookup().resolve(node);
        if (resolved instanceof ExchangeNode exchangeNode && exchangeNode.getSources().size() == 1) {
            int symbolIndex = exchangeNode.getOutputSymbols().indexOf(symbol);
            if (symbolIndex < 0) {
                return node;
            }

            Symbol sourceSymbol = getOnlyElement(exchangeNode.getInputs()).get(symbolIndex);
            PlanNode source = getOnlyElement(exchangeNode.getSources());
            PlanNode filteredSource = exchangeNode.getScope() == REMOTE
                    ? addRuntimeFilterConsumer(source, sourceSymbol, dynamicFilterId, context)
                    : addRuntimeFilter(source, sourceSymbol, dynamicFilterId, context);
            if (filteredSource == source) {
                return node;
            }
            return new ExchangeNode(
                    exchangeNode.getId(),
                    exchangeNode.getType(),
                    exchangeNode.getScope(),
                    exchangeNode.getPartitioningScheme(),
                    ImmutableList.of(filteredSource),
                    exchangeNode.getInputs(),
                    exchangeNode.getOrderingScheme());
        }

        return addRuntimeFilterConsumer(node, symbol, dynamicFilterId, context);
    }

    private PlanNode addRuntimeFilterConsumer(PlanNode node, Symbol symbol, DynamicFilterId dynamicFilterId, Context context)
    {
        PlanNode resolved = context.getLookup().resolve(node);
        if (resolved instanceof TopNNode topNNode &&
                topNNode.getStep() == PARTIAL &&
                topNNode.getRuntimeFilter().isEmpty() &&
                topNNode.getOrderingScheme().orderBy().equals(ImmutableList.of(symbol))) {
            return new TopNNode(
                    topNNode.getId(),
                    new FilterNode(
                            context.getIdAllocator().getNextId(),
                            topNNode.getSource(),
                            createDynamicFilterExpression(symbol, dynamicFilterId)),
                    topNNode.getCount(),
                    topNNode.getOrderingScheme(),
                    topNNode.getStep(),
                    topNNode.getRuntimeFilter());
        }

        if (!node.getOutputSymbols().contains(symbol)) {
            return node;
        }
        return new FilterNode(
                context.getIdAllocator().getNextId(),
                node,
                createDynamicFilterExpression(symbol, dynamicFilterId));
    }

    private Expression createDynamicFilterExpression(Symbol symbol, DynamicFilterId dynamicFilterId)
    {
        return DynamicFilters.createDynamicFilterExpression(
                plannerContext.getMetadata(),
                dynamicFilterId,
                symbol.type(),
                symbol.toSymbolReference(),
                EQUAL);
    }
}
