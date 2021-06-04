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
import io.trino.spi.type.Type;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PatternRecognitionNode.Measure;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.rowpattern.ExpressionAndValuePointersEquivalence;
import io.trino.sql.planner.rowpattern.ir.IrLabel;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.sql.planner.assertions.PatternRecognitionMatcher.rewrite;
import static java.util.Objects.requireNonNull;

public class MeasureMatcher
        implements RvalueMatcher
{
    private final String expression;
    private final Map<IrLabel, Set<IrLabel>> subsets;
    private final Type type;

    public MeasureMatcher(String expression, Map<IrLabel, Set<IrLabel>> subsets, Type type)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.subsets = requireNonNull(subsets, "subsets is null");
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    public Optional<Symbol> getAssignedSymbol(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        Optional<Symbol> result = Optional.empty();
        if (!(node instanceof PatternRecognitionNode)) {
            return result;
        }

        PatternRecognitionNode patternRecognitionNode = (PatternRecognitionNode) node;

        Measure expectedMeasure = new Measure(rewrite(expression, subsets), type);

        for (Map.Entry<Symbol, Measure> assignment : patternRecognitionNode.getMeasures().entrySet()) {
            Measure actualMeasure = assignment.getValue();
            if (measuresEquivalent(actualMeasure, expectedMeasure, symbolAliases)) {
                checkState(result.isEmpty(), "Ambiguous measures in %s", patternRecognitionNode);
                result = Optional.of(assignment.getKey());
            }
        }

        return result;
    }

    private static boolean measuresEquivalent(Measure actual, Measure expected, SymbolAliases symbolAliases)
    {
        if (!actual.getType().equals(expected.getType())) {
            return false;
        }

        ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);
        return ExpressionAndValuePointersEquivalence.equivalent(
                actual.getExpressionAndValuePointers(),
                expected.getExpressionAndValuePointers(),
                (actualSymbol, expectedSymbol) -> verifier.process(actualSymbol.toSymbolReference(), expectedSymbol.toSymbolReference()));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("expression", expression)
                .add("subsets", subsets)
                .add("type", type)
                .toString();
    }
}
