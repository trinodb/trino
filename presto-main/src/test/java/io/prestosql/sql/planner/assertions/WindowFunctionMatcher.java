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
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.planner.plan.WindowNode.Function;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.QualifiedName;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.sql.ExpressionTestUtils.getFunctionName;
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
        this.callMaker = requireNonNull(callMaker, "functionCall is null");
        this.resolvedFunction = requireNonNull(resolvedFunction, "resolvedFunction is null");
        this.frameMaker = requireNonNull(frameMaker, "frameMaker is null");
    }

    @Override
    public Optional<Symbol> getAssignedSymbol(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        Optional<Symbol> result = Optional.empty();
        if (!(node instanceof WindowNode)) {
            return result;
        }

        WindowNode windowNode = (WindowNode) node;

        FunctionCall expectedCall = callMaker.getExpectedValue(symbolAliases);
        Optional<WindowNode.Frame> expectedFrame = frameMaker.map(maker -> maker.getExpectedValue(symbolAliases));

        for (Map.Entry<Symbol, Function> assignment : windowNode.getWindowFunctions().entrySet()) {
            Function function = assignment.getValue();
            boolean signatureMatches = resolvedFunction.map(assignment.getValue().getResolvedFunction()::equals).orElse(true);
            if (signatureMatches && windowFunctionMatches(function, expectedCall, expectedFrame)) {
                checkState(!result.isPresent(), "Ambiguous function calls in %s", windowNode);
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
                Objects.equals(getFunctionName(expectedCall), QualifiedName.of(windowFunction.getResolvedFunction().getSignature().getName())) &&
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
