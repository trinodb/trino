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
import io.airlift.slice.Slices;
import io.trino.matching.Pattern;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.expression.VaradaSliceConstant;
import io.trino.plugin.varada.expression.rewrite.ExpressionPatterns;
import io.trino.plugin.varada.util.SliceUtils;
import org.apache.lucene.search.Query;

import java.util.ArrayList;
import java.util.List;

import static io.trino.plugin.varada.expression.rewrite.ExpressionPatterns.argument;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.LTRIM;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.RTRIM;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.SPLIT_PART;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.SUBSTR;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.SUBSTRING;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.TRIM;
import static io.trino.plugin.varada.storage.lucene.LuceneQueryUtils.createOrOfLikesQuery;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;

public class InRewriter
        implements ExpressionRewriter<VaradaCall>

{
    private static final Pattern<VaradaCall> PATTERN = ExpressionPatterns.call()
            .with(ExpressionPatterns.functionName().equalTo(IN_PREDICATE_FUNCTION_NAME.getName()))
            .with(ExpressionPatterns.type().equalTo(BOOLEAN))
            .with(argument(0).matching(x -> x instanceof VaradaCall varadaCall &&
                    (varadaCall.getFunctionName().equals(SUBSTRING.getName()) ||
                            varadaCall.getFunctionName().equals(SPLIT_PART.getName()) ||
                            varadaCall.getFunctionName().equals(TRIM.getName()) ||
                            varadaCall.getFunctionName().equals(LTRIM.getName()) ||
                            varadaCall.getFunctionName().equals(RTRIM.getName()) ||
                            varadaCall.getFunctionName().equals(SUBSTR.getName()))))
            .with(ExpressionPatterns.argumentCount().equalTo(2))
            .with(ExpressionPatterns.argument(1).matching(ExpressionPatterns.call().with(ExpressionPatterns.functionName().equalTo(ARRAY_CONSTRUCTOR_FUNCTION_NAME.getName()))));

    @Override
    public Pattern<VaradaCall> getPattern()
    {
        return PATTERN;
    }

    boolean handleIn(VaradaExpression expression, LuceneRewriteContext context)
    {
        boolean res = false;
        List<Slice> likeValues = new ArrayList<>();
        for (VaradaExpression valueExpression : expression.getChildren().get(1).getChildren()) {
            if (!(valueExpression instanceof VaradaSliceConstant)) {
                likeValues.clear();
                break;
            }
            Slice sliceValue = ((VaradaSliceConstant) valueExpression).getValue();
            String val = SliceUtils.serializeSlice(sliceValue);
            likeValues.add(Slices.utf8Slice("%" + val + "%"));
        }
        if (likeValues.size() > 0) {
            Query listOfLikeQuery = createOrOfLikesQuery(likeValues);
            context.queryBuilder().add(listOfLikeQuery, context.occur());
            res = true;
        }
        return res;
    }
}
