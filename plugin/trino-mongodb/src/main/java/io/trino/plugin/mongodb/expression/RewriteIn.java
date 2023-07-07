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
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.arguments;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.mongodb.expression.MongoExpressions.buildVariableExpression;
import static io.trino.plugin.mongodb.expression.MongoExpressions.documentOf;
import static io.trino.plugin.mongodb.expression.MongoExpressions.isVarchar;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;

public class RewriteIn
        implements ConnectorExpressionRule<Call, FilterExpression>
{
    private static final Capture<ConnectorExpression> VALUE = newCapture();
    private static final Capture<List<ConnectorExpression>> EXPRESSIONS = newCapture();

    private static final Pattern<Call> PATTERN = call()
            .with(functionName().equalTo(IN_PREDICATE_FUNCTION_NAME))
            .with(type().equalTo(BOOLEAN))
            .with(argumentCount().equalTo(2))
            .with(argument(0).matching(expression().capturedAs(VALUE)))
            .with(argument(1).matching(call().with(functionName().equalTo(ARRAY_CONSTRUCTOR_FUNCTION_NAME)).with(arguments().capturedAs(EXPRESSIONS))));

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<FilterExpression> rewrite(Call call, Captures captures, RewriteContext<FilterExpression> context)
    {
        Optional<FilterExpression> rewrittenValue = context.defaultRewrite(captures.get(VALUE));
        if (rewrittenValue.isEmpty()) {
            return Optional.empty();
        }

        List<ConnectorExpression> expressions = captures.get(EXPRESSIONS);

        // IN elements could be null, so use mutable list
        List<Object> rewrittenInValues = new ArrayList<>(expressions.size());
        for (ConnectorExpression expression : expressions) {
            Optional<FilterExpression> rewritten = context.defaultRewrite(expression);
            if (rewritten.isEmpty()) {
                return Optional.empty();
            }
            rewrittenInValues.add(rewritten.get().expression());
        }
        verify(!rewrittenInValues.isEmpty(), "rewrittenInValues is empty");
        FilterExpression leftFilterExpression = rewrittenValue.get();

        ImmutableList.Builder<Document> expressionBuilder = ImmutableList.builder();

        if (leftFilterExpression.isVariable()) {
            Optional<List<Document>> expression = buildVariableExpression(leftFilterExpression);
            if (expression.isEmpty()) {
                return Optional.empty();
            }
            expressionBuilder.addAll(expression.get());
        }

        Object leftValue = leftFilterExpression.expression();
        if (isVarchar(rewrittenValue.get())) {
            leftValue = MongoExpressions.toString(leftValue);
        }

        expressionBuilder.add(documentOf("$in", Arrays.asList(leftValue, rewrittenInValues)));

        return Optional.of(new FilterExpression(
                documentOf("$and", expressionBuilder.build()),
                FilterExpression.ExpressionType.DOCUMENT));
    }
}
