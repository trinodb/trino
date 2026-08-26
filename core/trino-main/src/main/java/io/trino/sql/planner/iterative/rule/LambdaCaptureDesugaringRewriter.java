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
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
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
            Set<String> lambdaArgumentNames = lambdaArguments.stream()
                    .map(Symbol::name)
                    .collect(toImmutableSet());

            // x -> f(x, captureSymbol)    will be rewritten into
            // "Bind"(captureSymbol, (extraSymbol, x) -> f(x, extraSymbol))

            ImmutableMap.Builder<Symbol, Symbol> captureSymbolToExtraSymbol = ImmutableMap.builder();
            ImmutableList.Builder<Symbol> newLambdaArguments = ImmutableList.builder();
            for (Symbol captureSymbol : captureSymbols) {
                Symbol extraSymbol = symbolAllocator.newSymbol(captureSymbol.name(), captureSymbol.type());
                while (lambdaArgumentNames.contains(extraSymbol.name())) {
                    extraSymbol = symbolAllocator.newSymbol(captureSymbol.name(), captureSymbol.type());
                }
                captureSymbolToExtraSymbol.put(captureSymbol, extraSymbol);
                newLambdaArguments.add(extraSymbol);
            }
            newLambdaArguments.addAll(node.arguments());

            Map<Symbol, Symbol> symbolsMap = captureSymbolToExtraSymbol.buildOrThrow();
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
        public Expression rewriteLet(Let node, Context context, ExpressionTreeRewriter<Context> treeRewriter)
        {
            // The value is evaluated in the enclosing scope, so its references are genuine captures.
            Expression value = treeRewriter.rewrite(node.value(), context);

            // The bound symbol is local to the body and must not be treated as a captured symbol.
            Context bodyContext = context.forLetBody(node.name());
            Expression body = treeRewriter.rewrite(node.body(), bodyContext);

            // Nested lambda will return the bound symbol as a capture, so we must explicitly exclude
            // it at this boundary. Otherwise, it would be propagated upward beyond the point of its
            // definition, and reported as a subexpression's free variable.
            bodyContext.getReferencedSymbols().remove(node.name());
            context.getReferencedSymbols().addAll(bodyContext.getReferencedSymbols());

            if (value != node.value() || body != node.body()) {
                return new Let(node.name(), value, body);
            }
            return node;
        }

        @Override
        public Expression rewriteReference(Reference node, Context context, ExpressionTreeRewriter<Context> treeRewriter)
        {
            Symbol symbol = new Symbol(node.type(), node.name());
            if (!context.isLetBoundSymbol(symbol)) {
                context.getReferencedSymbols().add(symbol);
            }
            return null;
        }
    }

    private static class Context
    {
        // Use linked hash set to guarantee deterministic iteration order
        final LinkedHashSet<Symbol> referencedSymbols;
        final Set<Symbol> letBoundSymbols;

        public Context()
        {
            this(new LinkedHashSet<>(), ImmutableSet.of());
        }

        private Context(LinkedHashSet<Symbol> referencedSymbols, Set<Symbol> letBoundSymbols)
        {
            this.referencedSymbols = referencedSymbols;
            this.letBoundSymbols = letBoundSymbols;
        }

        public LinkedHashSet<Symbol> getReferencedSymbols()
        {
            return referencedSymbols;
        }

        public boolean isLetBoundSymbol(Symbol symbol)
        {
            return letBoundSymbols.contains(symbol);
        }

        public Context withReferencedSymbols(LinkedHashSet<Symbol> symbols)
        {
            // A Let binding from an enclosing scope is a free variable inside a nested lambda, so
            // it must be captured there. Drop the bindings when crossing a lambda boundary.
            return new Context(symbols, ImmutableSet.of());
        }

        public Context forLetBody(Symbol symbol)
        {
            return new Context(new LinkedHashSet<>(), ImmutableSet.<Symbol>builder()
                    .addAll(letBoundSymbols)
                    .add(symbol)
                    .build());
        }
    }
}
