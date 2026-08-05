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
package io.trino.sql.gen;

import com.google.common.collect.ImmutableList;
import io.trino.sql.ir.DefaultTraversalVisitor;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;

import java.util.List;

public final class LambdaExpressionExtractor
{
    private LambdaExpressionExtractor() {}

    public static List<Lambda> extractLambdaExpressions(Expression expression)
    {
        ImmutableList.Builder<Lambda> lambdas = ImmutableList.builder();
        new DefaultTraversalVisitor<Void>()
        {
            @Override
            protected Void visitLambda(Lambda node, Void context)
            {
                super.visitLambda(node, context);
                lambdas.add(node);
                return null;
            }
        }.process(expression, null);
        return lambdas.build();
    }
}
