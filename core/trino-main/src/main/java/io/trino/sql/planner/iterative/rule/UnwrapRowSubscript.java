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

import io.trino.sql.tree.Cast;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.RowDataType;
import io.trino.sql.tree.SubscriptExpression;
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
            extends io.trino.sql.tree.ExpressionRewriter<Void>
    {
        @Override
        public Expression rewriteSubscriptExpression(SubscriptExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Expression base = treeRewriter.rewrite(node.getBase(), context);

            Deque<Coercion> coercions = new ArrayDeque<>();
            while (base instanceof Cast) {
                Cast cast = (Cast) base;

                if (!(cast.getType() instanceof RowDataType)) {
                    break;
                }

                int index = (int) ((LongLiteral) node.getIndex()).getValue();
                RowDataType rowType = (RowDataType) cast.getType();
                DataType type = rowType.getFields().get(index - 1).getType();
                if (!(type instanceof GenericDataType) || !((GenericDataType) type).getName().getValue().equalsIgnoreCase(UnknownType.NAME)) {
                    coercions.push(new Coercion(type, cast.isTypeOnly(), cast.isSafe()));
                }

                base = cast.getExpression();
            }

            if (base instanceof Row) {
                Row row = (Row) base;
                int index = (int) ((LongLiteral) node.getIndex()).getValue();
                Expression result = row.getItems().get(index - 1);

                while (!coercions.isEmpty()) {
                    Coercion coercion = coercions.pop();
                    result = new Cast(result, coercion.getType(), coercion.isSafe(), coercion.isTypeOnly());
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
        private final DataType type;
        private final boolean typeOnly;
        private final boolean safe;

        public Coercion(DataType type, boolean typeOnly, boolean safe)
        {
            this.type = type;
            this.typeOnly = typeOnly;
            this.safe = safe;
        }

        public DataType getType()
        {
            return type;
        }

        public boolean isTypeOnly()
        {
            return typeOnly;
        }

        public boolean isSafe()
        {
            return safe;
        }
    }
}
