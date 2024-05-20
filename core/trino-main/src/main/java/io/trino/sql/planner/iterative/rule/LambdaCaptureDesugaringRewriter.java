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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.sql.ir.Bind;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static java.util.Objects.requireNonNull;

public final class LambdaCaptureDesugaringRewriter
{
    public static Expression rewrite(Expression expression, SymbolAllocator symbolAllocator)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(symbolAllocator), expression, new Context());
    }

    private LambdaCaptureDesugaringRewriter() {}

    private static class Visitor
            extends ExpressionRewriter<Context>
    {
        private final SymbolAllocator symbolAllocator;

        public Visitor(SymbolAllocator symbolAllocator)
        {
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        }

        @Override
        public Expression rewriteLambda(Lambda node, Context context, ExpressionTreeRewriter<Context> treeRewriter)
        {
            // Use linked hash set to guarantee deterministic iteration order
            LinkedHashSet<Symbol> referencedSymbols = new LinkedHashSet<>();
            Expression rewrittenBody = treeRewriter.rewrite(node.body(), context.withReferencedSymbols(referencedSymbols));

            List<Symbol> lambdaArguments = node.arguments();

            Set<Symbol> captureSymbols = Sets.difference(referencedSymbols, ImmutableSet.copyOf(lambdaArguments));

            // x -> f(x, captureSymbol)    will be rewritten into
            // "Bind"(captureSymbol, (extraSymbol, x) -> f(x, extraSymbol))

            ImmutableMap.Builder<Symbol, Symbol> captureSymbolToExtraSymbol = ImmutableMap.builder();
            ImmutableList.Builder<Symbol> newLambdaArguments = ImmutableList.builder();
            for (Symbol captureSymbol : captureSymbols) {
                Symbol extraSymbol = symbolAllocator.newSymbol(captureSymbol.name(), captureSymbol.type());
                captureSymbolToExtraSymbol.put(captureSymbol, extraSymbol);
                newLambdaArguments.add(extraSymbol);
            }
            newLambdaArguments.addAll(node.arguments());

            ImmutableMap<Symbol, Symbol> symbolsMap = captureSymbolToExtraSymbol.buildOrThrow();
            Function<Symbol, Expression> symbolMapping = symbol -> symbolsMap.getOrDefault(symbol, symbol).toSymbolReference();
            Lambda lambda = new Lambda(newLambdaArguments.build(), inlineSymbols(symbolMapping, rewrittenBody));

            Expression rewrittenExpression = lambda;
            if (captureSymbols.size() != 0) {
                List<Expression> capturedValues = captureSymbols.stream()
                        .map(symbol -> new Reference(symbol.type(), symbol.name()))
                        .collect(toImmutableList());
                rewrittenExpression = new Bind(capturedValues, lambda);
            }

            context.getReferencedSymbols().addAll(captureSymbols);
            return rewrittenExpression;
        }

        @Override
        public Expression rewriteReference(Reference node, Context context, ExpressionTreeRewriter<Context> treeRewriter)
        {
            context.getReferencedSymbols().add(new Symbol(node.type(), node.name()));
            return null;
        }
    }

    private static class Context
    {
        // Use linked hash set to guarantee deterministic iteration order
        final LinkedHashSet<Symbol> referencedSymbols;

        public Context()
        {
            this(new LinkedHashSet<>());
        }

        private Context(LinkedHashSet<Symbol> referencedSymbols)
        {
            this.referencedSymbols = referencedSymbols;
        }

        public LinkedHashSet<Symbol> getReferencedSymbols()
        {
            return referencedSymbols;
        }

        public Context withReferencedSymbols(LinkedHashSet<Symbol> symbols)
        {
            return new Context(symbols);
        }
    }
}
