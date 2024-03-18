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
import io.trino.plugin.varada.expression.VaradaConstant;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.expression.VaradaPrimitiveConstant;
import io.trino.plugin.varada.expression.VaradaSliceConstant;
import io.trino.plugin.varada.expression.rewrite.ExpressionPatterns;
import io.trino.plugin.varada.util.SliceUtils;
import io.trino.spi.type.BooleanType;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.varada.expression.rewrite.ExpressionPatterns.argument;
import static io.trino.plugin.varada.expression.rewrite.ExpressionPatterns.argumentCount;
import static io.trino.plugin.varada.expression.rewrite.ExpressionPatterns.functionName;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.LTRIM;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.RTRIM;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.SPLIT_PART;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.STRPOS;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.SUBSTR;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.SUBSTRING;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.TRIM;
import static io.trino.plugin.varada.storage.lucene.LuceneQueryUtils.createLikeQuery;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;

public class EqualityValueRewriter
        implements ExpressionRewriter<VaradaCall>
{
    private static final Pattern<VaradaCall> PATTERN = ExpressionPatterns.call()
            .with(argumentCount().equalTo(2))
            .with(functionName().matching(x -> x.equals(EQUAL_OPERATOR_FUNCTION_NAME.getName())))
            .with(argument(0).matching(x -> x instanceof VaradaCall varadaCall &&
                    (varadaCall.getFunctionName().equals(SUBSTRING.getName()) ||
                            varadaCall.getFunctionName().equals(SPLIT_PART.getName()) ||
                            varadaCall.getFunctionName().equals(TRIM.getName()) ||
                            varadaCall.getFunctionName().equals(LTRIM.getName()) ||
                            varadaCall.getFunctionName().equals(RTRIM.getName()) ||
                            varadaCall.getFunctionName().equals(SUBSTR.getName()) ||
                            varadaCall.getFunctionName().equals(STRPOS.getName()))))
            .with(argument(1).matching(x -> x instanceof VaradaConstant && !x.getType().equals(BooleanType.BOOLEAN)));

    public EqualityValueRewriter()
    {
    }

    @Override
    public Pattern<VaradaCall> getPattern()
    {
        return PATTERN;
    }

    public boolean handleEqual(VaradaExpression expression, LuceneRewriteContext context)
    {
        VaradaCall varadaCall = (VaradaCall) expression.getChildren().get(0);
        VaradaConstant varadaConstant;
        if (varadaCall.getFunctionName().equals(STRPOS.getName())) {
            VaradaExpression positionValue = expression.getChildren().get(1);
            if (!(positionValue instanceof VaradaPrimitiveConstant)) {
                return false;
            }
            if (((VaradaPrimitiveConstant) positionValue).getValue() == Integer.valueOf(0)) {
                // See https://stackoverflow.com/a/16091066, =false->false, =true->true, !=true->false, !=false->true
                context.queryBuilder().add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
                context = createContext(context, BooleanClause.Occur.MUST_NOT);
            }
            varadaConstant = (VaradaConstant) varadaCall.getChildren().get(1);
        }
        else {
            varadaConstant = (VaradaConstant) expression.getChildren().get(1);
        }
        checkArgument(varadaConstant instanceof VaradaSliceConstant, "%s is not VaradaSliceConstant", varadaConstant);
        Slice sliceValue = ((Slice) varadaConstant.getValue());
        String val = SliceUtils.serializeSlice(sliceValue);
        Slice likeSlice = Slices.utf8Slice("%" + val + "%");
        Query likeQuery = createLikeQuery(likeSlice);
        context.queryBuilder().add(likeQuery, context.occur());
        return true;
    }
}
