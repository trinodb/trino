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
package io.trino.plugin.varada.expression.rewrite.worker.varadatolucene;

import io.trino.matching.Pattern;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.expression.VaradaVariable;
import io.trino.plugin.varada.expression.rewrite.ExpressionPatterns;

import static io.trino.plugin.varada.expression.rewrite.ExpressionPatterns.argument;
import static io.trino.plugin.varada.expression.rewrite.ExpressionPatterns.argumentCount;

public class LuceneVariableRewriter
        implements ExpressionRewriter<VaradaCall>
{
    private static final Pattern<VaradaCall> PATTERN = ExpressionPatterns.call()
            .with(argumentCount().equalTo(1))
            .with(argument(0).matching(x -> x instanceof VaradaVariable));

    @Override
    public Pattern<VaradaCall> getPattern()
    {
        return PATTERN;
    }

    public boolean handleIsNull(VaradaExpression varadaExpression, LuceneRewriteContext luceneRewriteContext)
    {
        return true;
    }
}
