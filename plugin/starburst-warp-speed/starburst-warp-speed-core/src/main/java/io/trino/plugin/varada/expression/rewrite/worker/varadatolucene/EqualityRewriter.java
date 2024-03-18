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
import io.trino.plugin.varada.expression.VaradaConstant;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.expression.rewrite.ExpressionPatterns;
import io.trino.spi.type.BooleanType;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;

import static io.trino.plugin.varada.expression.rewrite.ExpressionPatterns.argument;
import static io.trino.plugin.varada.expression.rewrite.ExpressionPatterns.argumentCount;
import static io.trino.plugin.varada.expression.rewrite.ExpressionPatterns.functionName;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;

class EqualityRewriter
        implements ExpressionRewriter<VaradaCall>

{
    private static final Pattern<VaradaCall> PATTERN = ExpressionPatterns.call()
            .with(argumentCount().equalTo(2))
            .with(argument(0).matching(x -> x instanceof VaradaCall))
            .with(argument(1).matching(x -> x instanceof VaradaConstant && x.getType().equals(BooleanType.BOOLEAN)))
            .with(functionName().matching(x -> x.equals(NOT_EQUAL_OPERATOR_FUNCTION_NAME.getName()) ||
                    x.equals(EQUAL_OPERATOR_FUNCTION_NAME.getName())));
    private final LuceneRulesHandler luceneRulesHandler;

    EqualityRewriter(LuceneRulesHandler luceneRulesHandler)
    {
        this.luceneRulesHandler = luceneRulesHandler;
    }

    @Override
    public Pattern<VaradaCall> getPattern()
    {
        return PATTERN;
    }

    boolean handleNotEqual(VaradaExpression expression, LuceneRewriteContext context)
    {
        return rewrite(expression, context, false);
    }

    boolean handleEqual(VaradaExpression expression, LuceneRewriteContext context)
    {
        return rewrite(expression, context, true);
    }

    private boolean rewrite(VaradaExpression expression, LuceneRewriteContext context, boolean isEqual)
    {
        boolean value = (boolean) ((VaradaConstant) expression.getChildren().get(1)).getValue();
        BooleanQuery.Builder queryBuilder = context.queryBuilder();
        BooleanClause.Occur res;
        if (isEqual != value) { //=false->false, =true->true, !=true->false, !=false->true
            // See https://stackoverflow.com/a/16091066
            queryBuilder.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
            res = BooleanClause.Occur.MUST_NOT;
        }
        else {
            res = context.occur();
        }
        VaradaCall innerCall = (VaradaCall) expression.getChildren().get(0);
        LuceneRewriteContext insideContext = createContext(context, res);
        return luceneRulesHandler.rewrite(innerCall, insideContext);
    }
}
