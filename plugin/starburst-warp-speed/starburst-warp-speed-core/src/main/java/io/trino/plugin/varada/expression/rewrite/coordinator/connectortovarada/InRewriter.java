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
package io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada;

import io.airlift.log.Logger;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.type.ArrayType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.arguments;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;

class InRewriter
        implements ConnectorExpressionRule<Call, VaradaExpression>
{
    private static final Capture<ConnectorExpression> VALUE = newCapture();
    private static final Capture<List<ConnectorExpression>> EXPRESSIONS = newCapture();
    private static final Logger logger = Logger.get(InRewriter.class);

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
    public Optional<VaradaExpression> rewrite(Call expression, Captures captures, RewriteContext<VaradaExpression> context)
    {
        ConnectorExpression expression1 = captures.get(VALUE);
        Optional<VaradaExpression> valueExpression = context.defaultRewrite(expression1);
        if (valueExpression.isEmpty()) {
            return Optional.empty();
        }

        List<VaradaExpression> inValues = new ArrayList<>();
        List<ConnectorExpression> expressions = captures.get(EXPRESSIONS);
        for (ConnectorExpression constantValue : expressions) {
            if (constantValue instanceof Constant) {
                Optional<VaradaExpression> constant = context.defaultRewrite(constantValue);
                if (constant.isPresent()) {
                    inValues.add(constant.get());
                }
                else {
                    logger.debug("can't convert expression=%s to varada expression", constantValue);
                    return Optional.empty();
                }
            }
            else {
                logger.debug("expression=%s is not type of Constant expression", constantValue);
                return Optional.empty();
            }
        }

        VaradaExpression valuesExpression = new VaradaCall(ARRAY_CONSTRUCTOR_FUNCTION_NAME.getName(), inValues, new ArrayType(inValues.get(0).getType()));

        return Optional.of(new VaradaCall(IN_PREDICATE_FUNCTION_NAME.getName(), List.of(valueExpression.get(), valuesExpression), expression.getType()));
    }
}
