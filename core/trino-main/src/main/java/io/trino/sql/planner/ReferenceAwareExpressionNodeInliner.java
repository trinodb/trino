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

import com.google.common.collect.ImmutableMap;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.NodeRef;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ReferenceAwareExpressionNodeInliner
        extends ExpressionRewriter<Void>
{
    public static Expression replaceExpression(Expression expression, Map<NodeRef<Expression>, Expression> mappings)
    {
        return ExpressionTreeRewriter.rewriteWith(new ReferenceAwareExpressionNodeInliner(mappings), expression);
    }

    private final Map<NodeRef<Expression>, Expression> mappings;

    private ReferenceAwareExpressionNodeInliner(Map<NodeRef<Expression>, Expression> mappings)
    {
        this.mappings = ImmutableMap.copyOf(requireNonNull(mappings, "mappings is null"));
    }

    @Override
    protected Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        return mappings.get(NodeRef.of(node));
    }
}
