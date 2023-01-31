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
package io.trino.plugin.jdbc.expression;

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionIndex;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static com.google.common.collect.MoreCollectors.toOptional;
import static io.trino.spi.expression.StandardFunctions.SIMPLE_CASE_WHEN_FUNCTION_NAME;
import static java.lang.String.format;

public class RewriteSimpleCaseExpression
        implements ConnectorExpressionRule<Call, String>
{
    public static RewriteSimpleCaseExpression createRewrite()
    {
        return createRewrite(Collections.emptyMap(), Optional.empty(), Optional.empty());
    }

    public static RewriteSimpleCaseExpression createRewrite(Map<String, Set<String>> typeClass,
                                                            String operandType,
                                                            String resultType)
    {
        return createRewrite(typeClass, Optional.of(operandType), Optional.of(resultType));
    }

    public static RewriteSimpleCaseExpression createRewrite(Map<String, Set<String>> typeClass,
                                                            Optional<String> operandType,
                                                            Optional<String> resultType)
    {
        ExpressionMappingParser parser = new ExpressionMappingParser(typeClass);
        Optional<TypePattern> operandTypePattern = operandType.map(parser::createTypePattern);
        Optional<TypePattern> resultTypePattern = resultType.map(parser::createTypePattern);

        Predicate<ConnectorExpressionIndex> operandTypePredicate = connectorExpressionIndex -> {
            int index = connectorExpressionIndex.getIndex();
            if (index == 0 || (index % 2 != 0 && !connectorExpressionIndex.isLast())) {
                return operandTypePattern.map(typePattern -> typePattern.getPattern()
                                .match(connectorExpressionIndex.getType())
                                .collect(toOptional()).isPresent()).orElse(true);
            }
            else {
                return resultTypePattern.map(typePattern -> typePattern.getPattern()
                        .match(connectorExpressionIndex.getType())
                        .collect(toOptional()).isPresent()).orElse(true);
            }
        };

        ExpressionListCapture expressionListCapture = new ExpressionListCapture("arguments", operandTypePredicate);

        VariableLengthCallPattern variableLengthCallPattern = new VariableLengthCallPattern(
                SIMPLE_CASE_WHEN_FUNCTION_NAME.getName(), expressionListCapture, resultTypePattern);

        return new RewriteSimpleCaseExpression(variableLengthCallPattern);
    }

    private final VariableLengthCallPattern variableLengthCallPattern;

    private RewriteSimpleCaseExpression(VariableLengthCallPattern variableLengthCallPattern)
    {
        this.variableLengthCallPattern = variableLengthCallPattern;
    }

    @Override
    public Pattern<Call> getPattern()
    {
        //noinspection unchecked
        return (Pattern<Call>) this.variableLengthCallPattern.getPattern();
    }

    @Override
    public Optional<String> rewrite(Call expression, Captures captures, RewriteContext<String> context)
    {
        MatchContext matchContext = new MatchContext();
        variableLengthCallPattern.resolve(captures, matchContext);

        StringBuilder rewritten = new StringBuilder();

        rewritten.append("(CASE ");

        Optional<Object> capture = matchContext.getIfPresent("arguments");

        if (capture.isPresent()) {
            Object value = capture.get();
            if (value instanceof List<?> list) {
                for (int i = 0; i < list.size(); i++) {
                    Object arg = list.get(i);
                    if (arg instanceof ConnectorExpression argExpression) {
                        Optional<String> rewrittenExpression = context.defaultRewrite(argExpression);
                        if (rewrittenExpression.isEmpty()) {
                            return Optional.empty();
                        }
                        if (i == 0) {
                            rewritten.append(rewrittenExpression.get());
                        }
                        else if (i % 2 != 0 && i != list.size() - 1) {
                            rewritten.append(" WHEN ").append(rewrittenExpression.get());
                        }
                        else if (i % 2 == 0) {
                            rewritten.append(" THEN ").append(rewrittenExpression.get());
                        }
                        else {
                            rewritten.append(" ELSE ").append(rewrittenExpression.get());
                        }
                    }
                    else {
                        throw new UnsupportedOperationException(format("Unsupported value: %s (%s)", arg, arg.getClass()));
                    }
                }
                rewritten.append(" END)");
            }
            else {
                throw new UnsupportedOperationException(format("Unsupported value: %s (%s)", value, value.getClass()));
            }

            return Optional.of(rewritten.toString());
        }

        return Optional.empty();
    }
}
