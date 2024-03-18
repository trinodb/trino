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
import io.trino.plugin.varada.expression.rewrite.ExpressionPatterns;
import io.trino.spi.expression.StandardFunctions;
import org.apache.lucene.search.BooleanClause;

import static io.trino.plugin.varada.expression.rewrite.ExpressionPatterns.functionName;

class LogicalOperatorRewriter
        implements ExpressionRewriter<VaradaCall>
{
    private static final Pattern<VaradaCall> PATTERN = ExpressionPatterns.call()
            .with(functionName().matching(x -> x.equals(StandardFunctions.AND_FUNCTION_NAME.getName()) ||
                    x.equals(StandardFunctions.OR_FUNCTION_NAME.getName())));
    private final LuceneRulesHandler luceneRulesHandler;

    LogicalOperatorRewriter(LuceneRulesHandler luceneRulesHandler)
    {
        this.luceneRulesHandler = luceneRulesHandler;
    }

    @Override
    public Pattern<VaradaCall> getPattern()
    {
        return PATTERN;
    }

    boolean handleOr(VaradaExpression expression, LuceneRewriteContext context)
    {
        return rewrite(expression, context, BooleanClause.Occur.SHOULD);
    }

    boolean handleAnd(VaradaExpression expression, LuceneRewriteContext context)
    {
        return rewrite(expression, context, BooleanClause.Occur.MUST);
    }

    private boolean rewrite(VaradaExpression expression, LuceneRewriteContext context, BooleanClause.Occur occur)
    {
        LuceneRewriteContext subContext = createContext(context, occur);
        boolean validExpression = false;
        for (VaradaExpression argumentExpression : expression.getChildren()) {
            boolean isValid = luceneRulesHandler.rewrite(argumentExpression, subContext);
            if (!isValid && BooleanClause.Occur.SHOULD.equals(occur)) {
                validExpression = false;
                break;
            }
            validExpression |= isValid;
        }
        return validExpression;
    }
}
