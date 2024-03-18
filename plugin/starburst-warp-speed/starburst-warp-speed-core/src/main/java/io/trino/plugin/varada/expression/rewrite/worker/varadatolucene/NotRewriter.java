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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;

import static io.trino.plugin.varada.expression.rewrite.ExpressionPatterns.argument;
import static io.trino.plugin.varada.expression.rewrite.ExpressionPatterns.argumentCount;
import static io.trino.plugin.varada.expression.rewrite.ExpressionPatterns.functionName;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;
import static java.util.Objects.requireNonNull;

class NotRewriter
        implements ExpressionRewriter<VaradaCall>
{
    private static final Pattern<VaradaCall> PATTERN = ExpressionPatterns.call()
            .with(functionName().matching(x -> x.equals(NOT_FUNCTION_NAME.getName())))
            .with(argumentCount().equalTo(1))
            .with(argument(0).matching(x -> x instanceof VaradaCall))
            .with(argument(0).matching(x -> ((VaradaCall) x).getFunctionName().equals(LIKE_FUNCTION_NAME.getName())));
    private final LuceneRulesHandler luceneRulesHandler;

    NotRewriter(LuceneRulesHandler luceneRulesHandler)
    {
        this.luceneRulesHandler = requireNonNull(luceneRulesHandler);
    }

    @Override
    public Pattern<VaradaCall> getPattern()
    {
        return PATTERN;
    }

    public boolean handleNotLike(VaradaExpression varadaExpression, LuceneRewriteContext context)
    {
        BooleanQuery.Builder innerQueryBuilder = new BooleanQuery.Builder();
        LuceneRewriteContext subContext = new LuceneRewriteContext(innerQueryBuilder, BooleanClause.Occur.MUST_NOT, context.collectNulls());
        subContext.queryBuilder().add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
        boolean valid = luceneRulesHandler.rewrite(varadaExpression.getChildren().get(0), subContext);
        if (valid) {
            context.queryBuilder().add(innerQueryBuilder.build(), BooleanClause.Occur.MUST);
        }
        return valid;
    }
}
