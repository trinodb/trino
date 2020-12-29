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

import io.prestosql.sql.planner.DesugarRowSubscriptRewriter;
import io.prestosql.sql.planner.TypeAnalyzer;

import static java.util.Objects.requireNonNull;

public class DesugarRowSubscript
        extends ExpressionRewriteRuleSet
{
    public DesugarRowSubscript(TypeAnalyzer typeAnalyzer)
    {
        super(createRewrite(typeAnalyzer));
    }

    private static ExpressionRewriter createRewrite(TypeAnalyzer typeAnalyzer)
    {
        requireNonNull(typeAnalyzer, "typeAnalyzer is null");

        return (expression, context) -> DesugarRowSubscriptRewriter.rewrite(expression, context.getSession(), typeAnalyzer, context.getSymbolAllocator());
    }
}
