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
package io.trino.sql.planner;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Reference;

import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;

public final class ExpressionSymbolInliner
{
    public static Expression inlineSymbols(Map<Symbol, ? extends Expression> mapping, Expression expression)
    {
        return inlineSymbols(mapping::get, expression);
    }

    public static Expression inlineSymbols(Function<Symbol, Expression> mapping, Expression expression)
    {
        return new ExpressionSymbolInliner(mapping).rewrite(expression);
    }

    private final Function<Symbol, Expression> mapping;

    private ExpressionSymbolInliner(Function<Symbol, Expression> mapping)
    {
        this.mapping = mapping;
    }

    private Expression rewrite(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(), expression);
    }

    private class Visitor
            extends ExpressionRewriter<Void>
    {
        private final Multiset<String> excludedNames = HashMultiset.create();

        @Override
        public Expression rewriteReference(Reference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (excludedNames.contains(node.name())) {
                return node;
            }

            Expression expression = mapping.apply(Symbol.from(node));
            checkState(expression != null, "Cannot resolve symbol %s", node.name());
            return expression;
        }

        @Override
        public Expression rewriteLambda(Lambda node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            excludedNames.addAll(node.arguments().stream()
                    .map(Symbol::name)
                    .toList());

            Expression result = treeRewriter.defaultRewrite(node, context);
            for (Symbol argument : node.arguments()) {
                verify(excludedNames.remove(argument.name()));
            }
            return result;
        }
    }
}
