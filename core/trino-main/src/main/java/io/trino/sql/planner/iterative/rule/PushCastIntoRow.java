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
import io.trino.spi.type.RowType;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Row;

/**
 * Transforms expressions of the form
 *
 * <pre>
 *  CAST(
 *      ROW(x, y)
 *      AS row(f1 type1, f2 type2))
 * </pre>
 *
 * to
 *
 * <pre>
 *  ROW(
 *      f1 CAST(x AS type1),
 *      f2 CAST(y AS type2))
 * </pre>
 */
public class PushCastIntoRow
        extends ExpressionRewriteRuleSet
{
    public PushCastIntoRow()
    {
        super((expression, context) -> ExpressionTreeRewriter.rewriteWith(new Rewriter(), expression, null));
    }

    private static class Rewriter
            extends io.trino.sql.ir.ExpressionRewriter<Void>
    {
        @Override
        public Expression rewriteCast(Cast node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (!(node.type() instanceof RowType castToType)) {
                return treeRewriter.defaultRewrite(node, null);
            }

            Expression value = treeRewriter.rewrite(node.expression(), null);
            if (value instanceof Row(java.util.List<Row.Field> fields)) {
                ImmutableList.Builder<Row.Field> items = ImmutableList.builder();
                for (int i = 0; i < fields.size(); i++) {
                    Expression fieldValue = fields.get(i).value();
                    RowType.Field castToField = castToType.getFields().get(i);
                    if (!fieldValue.type().equals(castToField.getType())) {
                        fieldValue = new Cast(fieldValue, castToField.getType());
                    }
                    items.add(new Row.Field(castToField.getName(), fieldValue));
                }
                return new Row(items.build());
            }

            return treeRewriter.defaultRewrite(node, null);
        }
    }
}
