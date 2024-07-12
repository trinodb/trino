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
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.plan.WindowNode.Function;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class WindowFunctionMatcher
        implements RvalueMatcher
{
    private final ExpectedValueProvider<WindowFunction> callMaker;

    public WindowFunctionMatcher(ExpectedValueProvider<WindowFunction> callMaker)
    {
        this.callMaker = requireNonNull(callMaker, "callMaker is null");
    }

    @Override
    public Optional<Symbol> getAssignedSymbol(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        Optional<Symbol> result = Optional.empty();

        Map<Symbol, Function> assignments;
        if (node instanceof WindowNode) {
            assignments = ((WindowNode) node).getWindowFunctions();
        }
        else if (node instanceof PatternRecognitionNode) {
            assignments = ((PatternRecognitionNode) node).getWindowFunctions();
        }
        else {
            return result;
        }

        WindowFunction expectedCall = callMaker.getExpectedValue(symbolAliases);
        for (Map.Entry<Symbol, Function> assignment : assignments.entrySet()) {
            Function function = assignment.getValue();
            if (windowFunctionMatches(function, expectedCall, symbolAliases)) {
                checkState(result.isEmpty(), "Ambiguous function calls in %s", node);
                result = Optional.of(assignment.getKey());
            }
        }

        return result;
    }

    private boolean windowFunctionMatches(Function windowFunction, WindowFunction expectedCall, SymbolAliases aliases)
    {
        return expectedCall.name().equals(windowFunction.getResolvedFunction().signature().getName().getFunctionName()) &&
                WindowFrameMatcher.matches(expectedCall.frame(), windowFunction.getFrame(), aliases) &&
                expectedCall.arguments().equals(windowFunction.getArguments());
    }

    @Override
    public String toString()
    {
        // Only include fields in the description if they are actual constraints.
        return toStringHelper(this)
                .omitNullValues()
                .add("callMaker", callMaker)
                .toString();
    }
}
