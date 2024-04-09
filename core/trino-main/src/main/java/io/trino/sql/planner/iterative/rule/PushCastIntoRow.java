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
import io.trino.spi.type.Type;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Row;
import io.trino.type.UnknownType;

/**
 * Transforms expressions of the form
 *
 * <pre>
 *  CAST(
 *      CAST(
 *          ROW(x, y)
 *          AS row(f1 type1, f2 type2))
 *      AS row(g1 type3, g2 type4))
 * </pre>
 *
 * to
 *
 * <pre>
 *  CAST(
 *      ROW(
 *          CAST(x AS type1),
 *          CAST(y AS type2))
 *      AS row(g1 type3, g2 type4))
 * </pre>
 *
 * Note: it preserves the top-level CAST if the row type has field names because the names are needed by the ROW to JSON cast
 *       TODO: ideally, the types involved in ROW to JSON cast should be captured at analysis time and
 *         remain fixed for the duration of the optimization process so as to have flexibility in terms
 *         of removing field names, which are irrelevant in the IR
 */
public class PushCastIntoRow
        extends ExpressionRewriteRuleSet
{
    public PushCastIntoRow()
    {
        super((expression, context) -> ExpressionTreeRewriter.rewriteWith(new Rewriter(), expression, false));
    }

    private static class Rewriter
            extends io.trino.sql.ir.ExpressionRewriter<Boolean>
    {
        @Override
        public Expression rewriteCast(Cast node, Boolean inRowCast, ExpressionTreeRewriter<Boolean> treeRewriter)
        {
            if (!(node.type() instanceof RowType type)) {
                return treeRewriter.defaultRewrite(node, false);
            }

            // if inRowCast == true or row is anonymous, we're free to push Cast into Row. An enclosing CAST(... AS ROW) will take care of preserving field names
            // otherwise, apply recursively with inRowCast == true and don't push this one

            if (inRowCast || type.getFields().stream().allMatch(field -> field.getName().isEmpty())) {
                Expression value = treeRewriter.rewrite(node.expression(), true);

                if (value instanceof Row row) {
                    ImmutableList.Builder<Expression> items = ImmutableList.builder();
                    for (int i = 0; i < row.items().size(); i++) {
                        Expression item = row.items().get(i);
                        Type itemType = type.getFields().get(i).getType();
                        if (!(itemType instanceof UnknownType)) {
                            item = new Cast(item, itemType, node.safe());
                        }
                        items.add(item);
                    }
                    return new Row(items.build());
                }
            }

            return treeRewriter.defaultRewrite(node, true);
        }
    }
}
