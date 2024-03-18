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

import io.airlift.slice.Slice;
import io.trino.matching.Pattern;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaConstant;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.expression.VaradaSliceConstant;
import io.trino.plugin.varada.expression.VaradaVariable;
import io.trino.plugin.varada.expression.rewrite.ExpressionPatterns;
import io.trino.spi.predicate.Range;
import io.trino.spi.type.Type;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.varada.expression.rewrite.ExpressionPatterns.argument;
import static io.trino.plugin.varada.expression.rewrite.ExpressionPatterns.argumentCount;
import static io.trino.plugin.varada.storage.lucene.LuceneQueryUtils.createContainsQuery;
import static io.trino.plugin.varada.storage.lucene.LuceneQueryUtils.createLikeQuery;
import static io.trino.plugin.varada.storage.lucene.LuceneQueryUtils.createPrefixQuery;
import static io.trino.plugin.varada.storage.lucene.LuceneQueryUtils.createRangeQuery;

class GeneralRewriter
        implements ExpressionRewriter<VaradaCall>
{
    private static final Pattern<VaradaCall> PATTERN = ExpressionPatterns.call()
            .with(argumentCount().equalTo(2))
            .with(argument(0).matching(x -> x instanceof VaradaVariable))
            .with(argument(1).matching(x -> x instanceof VaradaSliceConstant));

    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;

    GeneralRewriter(DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer)
    {
        this.dispatcherProxiedConnectorTransformer = dispatcherProxiedConnectorTransformer;
    }

    @Override
    public Pattern<VaradaCall> getPattern()
    {
        return PATTERN;
    }

    boolean handleLike(VaradaExpression expression, LuceneRewriteContext context)
    {
        return rewrite(expression, context, (value, type) -> createLikeQuery(value));
    }

    public boolean handleContains(VaradaExpression expression, LuceneRewriteContext context)
    {
        return rewrite(expression, context, (value, type) -> createContainsQuery(value));
    }

    boolean handleStartsWith(VaradaExpression expression, LuceneRewriteContext context)
    {
        return rewrite(expression, context, (value, type) -> createPrefixQuery(value));
    }

    boolean handleNotEqual(VaradaExpression expression, LuceneRewriteContext context)
    {
        return rewrite(expression, context, (value, type) -> {
            Query lessThan = createRangeQuery(Range.lessThan(type, value));
            Query greaterThan = createRangeQuery(Range.greaterThan(type, value));
            BooleanQuery.Builder innerQueryBuilder = new BooleanQuery.Builder();
            innerQueryBuilder.add(lessThan, BooleanClause.Occur.SHOULD);
            innerQueryBuilder.add(greaterThan, BooleanClause.Occur.SHOULD);
            return innerQueryBuilder.build();
        });
    }

    boolean handleEqual(VaradaExpression expression, LuceneRewriteContext context)
    {
        return rewrite(expression, context, (value, type) -> createRangeQuery(Range.equal(type, value)));
    }

    boolean handleLessThan(VaradaExpression expression, LuceneRewriteContext context)
    {
        return rewrite(expression, context, (value, type) -> createRangeQuery(Range.lessThan(type, value)));
    }

    boolean handleLessThanOrEqual(VaradaExpression expression, LuceneRewriteContext context)
    {
        return rewrite(expression, context, (value, type) -> createRangeQuery(Range.lessThanOrEqual(type, value)));
    }

    boolean greateThan(VaradaExpression expression, LuceneRewriteContext context)
    {
        return rewrite(expression, context, (value, type) -> createRangeQuery(Range.greaterThan(type, value)));
    }

    boolean greatThanOrEqual(VaradaExpression expression, LuceneRewriteContext context)
    {
        return rewrite(expression, context, (value, type) -> createRangeQuery(Range.greaterThanOrEqual(type, value)));
    }

    private boolean rewrite(VaradaExpression expression, LuceneRewriteContext context, BiFunction<Slice, Type, Query> queryFunction)
    {
        Type type = getType((VaradaVariable) expression.getChildren().get(0));
        VaradaConstant varadaConstant = (VaradaConstant) expression.getChildren().get(1);
        checkArgument(varadaConstant instanceof VaradaSliceConstant, "%s is not VaradaSliceConstant", varadaConstant);
        Slice value = ((Slice) varadaConstant.getValue());
        Query query = queryFunction.apply(value, type);
        context.queryBuilder().add(query, context.occur());
        return true;
    }

    private Type getType(VaradaVariable varadaVariable)
    {
        return dispatcherProxiedConnectorTransformer.getColumnType(varadaVariable.getColumnHandle());
    }
}
