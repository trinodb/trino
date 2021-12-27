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
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.plan.WindowNode.Function;
import io.trino.sql.tree.FunctionCall;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.metadata.ResolvedFunction.extractFunctionName;
import static java.util.Objects.requireNonNull;

public class WindowFunctionMatcher
        implements RvalueMatcher
{
    private final ExpectedValueProvider<FunctionCall> callMaker;
    private final Optional<ResolvedFunction> resolvedFunction;
    private final Optional<ExpectedValueProvider<WindowNode.Frame>> frameMaker;

    /**
     * @param callMaker Always validates the function call
     * @param resolvedFunction Optionally validates the signature
     * @param frameMaker Optionally validates the frame
     */
    public WindowFunctionMatcher(
            ExpectedValueProvider<FunctionCall> callMaker,
            Optional<ResolvedFunction> resolvedFunction,
            Optional<ExpectedValueProvider<WindowNode.Frame>> frameMaker)
    {
        this.callMaker = requireNonNull(callMaker, "callMaker is null");
        this.resolvedFunction = requireNonNull(resolvedFunction, "resolvedFunction is null");
        this.frameMaker = requireNonNull(frameMaker, "frameMaker is null");
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

        FunctionCall expectedCall = callMaker.getExpectedValue(symbolAliases);
        Optional<WindowNode.Frame> expectedFrame = frameMaker.map(maker -> maker.getExpectedValue(symbolAliases));

        for (Map.Entry<Symbol, Function> assignment : assignments.entrySet()) {
            Function function = assignment.getValue();
            boolean signatureMatches = resolvedFunction.map(assignment.getValue().getResolvedFunction()::equals).orElse(true);
            if (signatureMatches && windowFunctionMatches(function, expectedCall, expectedFrame)) {
                checkState(result.isEmpty(), "Ambiguous function calls in %s", node);
                result = Optional.of(assignment.getKey());
            }
        }

        return result;
    }

    private boolean windowFunctionMatches(Function windowFunction, FunctionCall expectedCall, Optional<WindowNode.Frame> expectedFrame)
    {
        if (expectedCall.getWindow().isPresent()) {
            return false;
        }

        return resolvedFunction.map(windowFunction.getResolvedFunction()::equals).orElse(true) &&
                expectedFrame.map(windowFunction.getFrame()::equals).orElse(true) &&
                Objects.equals(extractFunctionName(expectedCall.getName()), windowFunction.getResolvedFunction().getSignature().getName()) &&
                Objects.equals(expectedCall.getArguments(), windowFunction.getArguments());
    }

    @Override
    public String toString()
    {
        // Only include fields in the description if they are actual constraints.
        return toStringHelper(this)
                .omitNullValues()
                .add("callMaker", callMaker)
                .add("signature", resolvedFunction.orElse(null))
                .add("frameMaker", frameMaker.orElse(null))
                .toString();
    }
}
