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

import io.trino.metadata.Metadata;
import io.trino.sql.planner.FunctionCallBuilder;
import io.trino.sql.tree.CurrentCatalog;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.QualifiedName;

public class DesugarCurrentCatalog
        extends ExpressionRewriteRuleSet
{
    public DesugarCurrentCatalog(Metadata metadata)
    {
        super(createRewrite(metadata));
    }

    private static ExpressionRewriter createRewrite(Metadata metadata)
    {
        return (expression, context) -> ExpressionTreeRewriter.rewriteWith(new io.trino.sql.tree.ExpressionRewriter<>()
        {
            @Override
            public Expression rewriteCurrentCatalog(CurrentCatalog node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return getCall(node, metadata);
            }
        }, expression);
    }

    public static FunctionCall getCall(CurrentCatalog node, Metadata metadata)
    {
        return new FunctionCallBuilder(metadata)
                .setName(QualifiedName.of("$current_catalog"))
                .build();
    }
}
