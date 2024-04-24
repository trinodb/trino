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

import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.Row;
import io.trino.type.UnknownType;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Transforms expressions of the form
 *
 * <code>ROW(x, y)[1]</code> to <code>x</code>
 *
 * and
 *
 * <code>CAST(ROW(x, y) AS row(f1 type1, f2 type2))[1]</code> to <code>CAST(x AS type1)</code>
 *
 */
public class UnwrapRowSubscript
        extends ExpressionRewriteRuleSet
{
    public UnwrapRowSubscript()
    {
        super((expression, context) -> ExpressionTreeRewriter.rewriteWith(new Rewriter(), expression));
    }

    private static class Rewriter
            extends io.trino.sql.ir.ExpressionRewriter<Void>
    {
        @Override
        public Expression rewriteSubscript(FieldReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Expression base = treeRewriter.rewrite(node.base(), context);

            Deque<Coercion> coercions = new ArrayDeque<>();
            while (base instanceof Cast cast) {
                if (!(cast.type() instanceof RowType rowType)) {
                    break;
                }

                Type type = rowType.getFields().get(node.field()).getType();
                if (!(type instanceof UnknownType)) {
                    coercions.push(new Coercion(type, cast.safe()));
                }

                base = cast.expression();
            }

            if (base instanceof Row row) {
                Expression result = row.items().get(node.field());

                while (!coercions.isEmpty()) {
                    Coercion coercion = coercions.pop();
                    result = new Cast(result, coercion.getType(), coercion.isSafe());
                }

                return result;
            }

            // TODO: if we can prove base is of type row(...) or any other subscript-bearing type, we can push the subscript all the way down:
            //    1. CAST(row_producing_function() AS row(bigint, integer))[1] => CAST(row_producing_function()[1] AS bigint)
            //    2. CAST(array_producing_function() AS array(bigint))[1] => CAST(array_producing_function()[1]) AS bigint)
            return treeRewriter.defaultRewrite(node, context);
        }
    }

    private static class Coercion
    {
        private final Type type;
        private final boolean safe;

        public Coercion(Type type, boolean safe)
        {
            this.type = type;
            this.safe = safe;
        }

        public Type getType()
        {
            return type;
        }

        public boolean isSafe()
        {
            return safe;
        }
    }
}
