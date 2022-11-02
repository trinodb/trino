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
package io.trino.sql.planner.rowpattern;

import io.trino.sql.planner.Symbol;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.LabelDereference;
import io.trino.sql.tree.QualifiedName;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.analyzer.ExpressionAnalyzer.isPatternRecognitionFunction;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class AggregateArgumentsRewriter
{
    private AggregateArgumentsRewriter() {}

    /**
     * This rewriter is dedicated for aggregation arguments in pattern recognition context.
     * <p>
     * Rewrite expressions so that they do not contain any elements specific to row pattern recognition:
     * - remove labels from `LabelDereference`s and `CLASSIFIER()` calls,
     * - replace `CLASSIFIER()` and `MATCH_NUMBER()` calls with new symbols,
     * - the expressions do not contain navigations by analysis.
     * <p>
     * NOTE: Unlike `LogicalIndexExtractor`, this rewriter does not re-allocate all symbols. The rewritten expressions
     * contain all the original symbols, and additionally they contain new symbols replacing `CLASSIFIER()` and `MATCH_NUMBER()` calls.
     * It is correct, because each of the expressions (by analysis) is effectively evaluated within a single row, so any expression
     * optimizations based on symbols are applicable. Additionally, that makes the expressions eligible for pre-projection on the condition
     * that there were no `CLASSIFIER()` or `MATCH_NUMBER()` calls. The `PushDownProjectionsFromPatternRecognition` rule pushes down
     * argument computations and replaces them with single symbols.
     * <p>
     * Because the expressions are effectively evaluated within a single row, it is correct to replace all `CLASSIFIER()` calls
     * with the same symbol and all `MATCH_NUMBER()` calls with the same symbol.
     */
    public static List<Expression> rewrite(List<Expression> arguments, Symbol classifierSymbol, Symbol matchNumberSymbol)
    {
        Rewriter rewriter = new Rewriter(classifierSymbol, matchNumberSymbol);

        return arguments.stream()
                .filter(argument -> !isAllRowsReference(argument))
                .map(argument -> ExpressionTreeRewriter.rewriteWith(rewriter, argument))
                .collect(toImmutableList());
    }

    private static boolean isAllRowsReference(Expression argument)
    {
        return argument instanceof LabelDereference && ((LabelDereference) argument).getReference().isEmpty();
    }

    private static class Rewriter
            extends ExpressionRewriter<Void>
    {
        private final Symbol classifierSymbol;
        private final Symbol matchNumberSymbol;

        public Rewriter(Symbol classifierSymbol, Symbol matchNumberSymbol)
        {
            this.classifierSymbol = requireNonNull(classifierSymbol, "classifierSymbol is null");
            this.matchNumberSymbol = requireNonNull(matchNumberSymbol, "matchNumberSymbol is null");
        }

        @Override
        protected Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            return treeRewriter.defaultRewrite(node, context);
        }

        @Override
        public Expression rewriteLabelDereference(LabelDereference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            return node.getReference().orElseThrow();
        }

        @Override
        public Expression rewriteFunctionCall(FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (isPatternRecognitionFunction(node)) {
                QualifiedName name = node.getName();
                String functionName = name.getSuffix().toUpperCase(ENGLISH);
                return switch (functionName) {
                    case "CLASSIFIER" -> classifierSymbol.toSymbolReference();
                    case "MATCH_NUMBER" -> matchNumberSymbol.toSymbolReference();
                    default -> throw new UnsupportedOperationException("unexpected pattern recognition function: " + node.getName());
                };
            }

            return super.rewriteFunctionCall(node, context, treeRewriter);
        }
    }
}
