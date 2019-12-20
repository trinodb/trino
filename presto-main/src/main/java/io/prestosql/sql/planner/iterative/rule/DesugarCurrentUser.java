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
package io.prestosql.sql.planner.iterative.rule;

import io.prestosql.metadata.Metadata;
import io.prestosql.sql.planner.FunctionCallBuilder;
import io.prestosql.sql.tree.CurrentUser;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.QualifiedName;

public class DesugarCurrentUser
        extends ExpressionRewriteRuleSet
{
    public DesugarCurrentUser(Metadata metadata)
    {
        super(createRewrite(metadata));
    }

    private static ExpressionRewriter createRewrite(Metadata metadata)
    {
        return (expression, context) -> ExpressionTreeRewriter.rewriteWith(new io.prestosql.sql.tree.ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteCurrentUser(CurrentUser node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return getCall(node, metadata);
            }
        }, expression);
    }

    public static FunctionCall getCall(CurrentUser node, Metadata metadata)
    {
        return new FunctionCallBuilder(metadata)
                .setName(QualifiedName.of("$current_user"))
                .build();
    }
}
