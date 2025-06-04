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
import io.trino.metadata.Metadata;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.PlanNode;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class AggregationFunctionMatcher
        implements RvalueMatcher
{
    private final ExpectedValueProvider<AggregationFunction> callMaker;

    public AggregationFunctionMatcher(ExpectedValueProvider<AggregationFunction> callMaker)
    {
        this.callMaker = requireNonNull(callMaker, "callMaker is null");
    }

    @Override
    public Optional<Symbol> getAssignedSymbol(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        Optional<Symbol> result = Optional.empty();
        if (!(node instanceof AggregationNode aggregationNode)) {
            return result;
        }

        AggregationFunction expectedCall = callMaker.getExpectedValue(symbolAliases);
        for (Map.Entry<Symbol, Aggregation> assignment : aggregationNode.getAggregations().entrySet()) {
            Aggregation aggregation = assignment.getValue();
            if (aggregationMatches(aggregation, expectedCall)) {
                checkState(result.isEmpty(), "Ambiguous function calls in %s", aggregationNode);
                result = Optional.of(assignment.getKey());
            }
        }

        return result;
    }

    private static boolean aggregationMatches(Aggregation aggregation, AggregationFunction expectedCall)
    {
        return Objects.equals(expectedCall.name(), aggregation.getResolvedFunction().signature().getName().getFunctionName()) &&
                Objects.equals(expectedCall.filter(), aggregation.getFilter()) &&
                Objects.equals(expectedCall.orderingScheme(), aggregation.getOrderingScheme()) &&
                expectedCall.distinct() == aggregation.isDistinct() &&
                Objects.equals(expectedCall.arguments(), aggregation.getArguments());
    }

    @Override
    public String toString()
    {
        return callMaker.toString();
    }
}
