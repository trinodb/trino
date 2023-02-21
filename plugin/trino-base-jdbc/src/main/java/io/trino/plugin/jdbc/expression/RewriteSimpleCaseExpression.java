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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.base.expression.ConnectorExpressionWithIndex;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static io.trino.plugin.jdbc.expression.TypePattern.getArrayTypePredicate;
import static io.trino.plugin.jdbc.expression.TypePattern.getTypePredicate;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.SIMPLE_CASE_WHEN_CONDITION_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.SIMPLE_CASE_WHEN_FUNCTION_NAME;
import static java.lang.String.format;

public class RewriteSimpleCaseExpression
        implements ConnectorExpressionRule<Call, String>
{
    public static RewriteSimpleCaseExpression createRewrite()
    {
        return createRewrite(ImmutableMap.of(), Optional.empty(), Optional.empty());
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

        VariableLengthCallPattern whenClauseArrayCallPattern = createWhenClausesCallPattern(
                operandTypePattern, resultTypePattern);

        VariableLengthCallPattern simpleCaseWhenCallPattern = createSimpleCaseWhenCallPattern(
                whenClauseArrayCallPattern, operandTypePattern, resultTypePattern);

        return new RewriteSimpleCaseExpression(simpleCaseWhenCallPattern);
    }

    private static VariableLengthCallPattern createWhenClausesCallPattern(Optional<TypePattern> operandTypePattern,
                                                                          Optional<TypePattern> resultTypePattern)
    {
        Predicate<ConnectorExpressionWithIndex> whenConditionsPredicate = connectorExpressionWithIndex -> {
            ConnectorExpression connectorExpression = connectorExpressionWithIndex.getConnectorExpression();
            CallPattern singleWhenClausePattern = new CallPattern(SIMPLE_CASE_WHEN_CONDITION_FUNCTION_NAME.getName(),
                    ImmutableList.of(new ExpressionCapture("operand", operandTypePattern),
                            new ExpressionCapture("result", resultTypePattern)),
                    resultTypePattern);
            return getTypePredicate(resultTypePattern).test(connectorExpression.getType())
                    && singleWhenClausePattern.getPattern().match(connectorExpression).findFirst().isPresent();
        };

        ExpressionListCapture whenConditionsCapture = new ExpressionListCapture("whenConditions", whenConditionsPredicate);
        return new VariableLengthCallPattern(ARRAY_CONSTRUCTOR_FUNCTION_NAME.getName(), whenConditionsCapture, resultTypePattern);
    }

    private static VariableLengthCallPattern createSimpleCaseWhenCallPattern(VariableLengthCallPattern whenClauseArrayCallPattern,
                                                                             Optional<TypePattern> operandTypePattern,
                                                                             Optional<TypePattern> resultTypePattern)
    {
        Predicate<Type> operandTypePredicate = getTypePredicate(operandTypePattern);

        Predicate<Type> whenClauseArrayTypePredicate = getArrayTypePredicate(resultTypePattern);

        Predicate<Type> resultTypePredicate = getTypePredicate(resultTypePattern);

        Predicate<ConnectorExpressionWithIndex> argumentsPredicate = connectorExpressionWithIndex -> {
            int index = connectorExpressionWithIndex.getIndex();
            ConnectorExpression connectorExpression = connectorExpressionWithIndex.getConnectorExpression();
            return switch (index) {
                case 0 -> operandTypePredicate.test(connectorExpression.getType());
                case 1 -> whenClauseArrayTypePredicate.test(connectorExpression.getType())
                        && whenClauseArrayCallPattern.getPattern().match(connectorExpression).findFirst().isPresent();
                case 2 -> resultTypePredicate.test(connectorExpression.getType());
                default -> false;
            };
        };

        ExpressionListCapture argumentsCapture = new ExpressionListCapture("arguments", argumentsPredicate);

        return new VariableLengthCallPattern(SIMPLE_CASE_WHEN_FUNCTION_NAME.getName(), argumentsCapture, resultTypePattern);
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

        Optional<Object> capture = matchContext.getIfPresent("arguments");

        if (capture.isEmpty()) {
            return Optional.empty();
        }

        Object value = capture.get();
        if (!(value instanceof List<?> list)) {
            throw new UnsupportedOperationException(format("Unsupported value: %s (%s)", value, value.getClass()));
        }

        rewritten.append("(CASE ");
        for (int i = 0; i < list.size(); i++) {
            Object arg = list.get(i);
            if (!(arg instanceof ConnectorExpression argExpression)) {
                throw new UnsupportedOperationException(format("Unsupported value: %s (%s)", arg, arg.getClass()));
            }

            if (i == 1) {
                if (!rewriteWhenClauseArray(argExpression, rewritten, context)) {
                    return Optional.empty();
                }
            }
            else {
                Optional<String> rewrittenExpression = context.defaultRewrite(argExpression);
                if (rewrittenExpression.isEmpty()) {
                    return Optional.empty();
                }

                if (i == 2) {
                    rewritten.append(" ELSE ");
                }
                rewritten.append(rewrittenExpression.get());
            }
        }
        rewritten.append(" END)");
        return Optional.of(rewritten.toString());
    }

    private boolean rewriteWhenClauseArray(ConnectorExpression connectorExpression,
                                           StringBuilder rewritten,
                                           RewriteContext<String> context)
    {
        if (!(connectorExpression instanceof Call whenClauseArray)) {
            return false;
        }
        for (ConnectorExpression whenClause : whenClauseArray.getArguments()) {
            if (!rewriteWhenClause(whenClause, rewritten, context)) {
                return false;
            }
        }
        return true;
    }

    private boolean rewriteWhenClause(ConnectorExpression connectorExpression,
                                      StringBuilder rewritten,
                                      RewriteContext<String> context)
    {
        if (!(connectorExpression instanceof Call whenClause) || whenClause.getArguments().size() != 2) {
            return false;
        }

        Optional<String> operand = context.defaultRewrite(whenClause.getArguments().get(0));
        if (operand.isEmpty()) {
            return false;
        }

        Optional<String> result = context.defaultRewrite(whenClause.getArguments().get(1));
        if (result.isEmpty()) {
            return false;
        }
        rewritten.append(" WHEN ").append(operand.get()).append(" THEN ").append(result.get());
        return true;
    }
}
