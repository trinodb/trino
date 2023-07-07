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
package io.trino.plugin.mongodb.expression;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.type.VarcharType;
import org.bson.Document;

import java.util.Arrays;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.mongodb.expression.MongoExpressions.documentOf;
import static io.trino.spi.expression.StandardFunctions.IS_NULL_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;

public class RewriteIsNull
        implements ConnectorExpressionRule<Call, FilterExpression>
{
    private final Pattern<Call> pattern;

    public RewriteIsNull()
    {
        this.pattern = call()
                .with(functionName().equalTo(IS_NULL_FUNCTION_NAME))
                .with(type().equalTo(BOOLEAN))
                .with(argumentCount().equalTo(1));
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return pattern;
    }

    @Override
    public Optional<FilterExpression> rewrite(Call call, Captures captures, RewriteContext<FilterExpression> context)
    {
        ConnectorExpression argumentValue = getOnlyElement(call.getArguments());
        Optional<FilterExpression> rewrittenValue = context.defaultRewrite(argumentValue);
        if (rewrittenValue.isEmpty()) {
            return Optional.empty();
        }
        FilterExpression filterExpression = rewrittenValue.get();
        Object value = filterExpression.expression();

        ImmutableList.Builder<Document> expressionBuilder = ImmutableList.<Document>builder()
                .add(documentOf("$eq", Arrays.asList(value, null)))
                .add(documentOf("$eq", Arrays.asList(value, documentOf("$undefined", true))));

        if (filterExpression.isVariable()) {
            ExpressionInfo expressionInfo = filterExpression.expressionInfo().orElseThrow();
            Optional<String> mongoType = expressionInfo.getMongoType();
            // If mapping between trino type and mongo type does not happen, then pushdown won't be supported
            if (mongoType.isEmpty()) {
                return Optional.empty();
            }
            // Handle mismatched data type value of documents by matching the data type of column and document value
            if (expressionInfo.getType() == BIGINT) {
                expressionBuilder.add(documentOf("$not", documentOf("$in", ImmutableList.of(documentOf("$type", expressionInfo.getName()), ImmutableList.of(mongoType.get(), "int")))));
            }
            else if (!(expressionInfo.getType() instanceof VarcharType)) {
                expressionBuilder.add(documentOf("$ne", ImmutableList.of(documentOf("$type", value), mongoType.get())));
            }
        }

        return Optional.of(new FilterExpression(
                documentOf("$or", expressionBuilder.build()),
                FilterExpression.ExpressionType.DOCUMENT));
    }
}
