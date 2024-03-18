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
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.expression.ExpressionMappingParser;
import io.trino.plugin.jdbc.expression.ExpressionPattern;
import io.trino.plugin.jdbc.expression.MatchContext;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.expression.VaradaVariable;
import io.trino.plugin.warp.gen.stats.VaradaStatsPushdownPredicates;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.ArrayType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;

class GenericRewriter
        implements ConnectorExpressionRule<Call, VaradaExpression>
{
    private static final Logger logger = Logger.get(GenericRewriter.class);

    private static final java.util.regex.Pattern REWRITE_TOKENS = java.util.regex.Pattern.compile("(?<![a-zA-Z0-9_$])[a-zA-Z_$][a-zA-Z0-9_$]*(?![a-zA-Z0-9_$])");

    private final ExpressionPattern expressionPattern;
    private final String originalExpression;
    private final boolean allowCompositeExpression;
    private final VaradaStatsPushdownPredicates varadaStatsPushdownPredicates;

    GenericRewriter(Map<String, Set<String>> typeClasses,
            String expressionPattern,
            boolean allowCompositeExpression,
            VaradaStatsPushdownPredicates varadaStatsPushdownPredicates)
    {
        this.allowCompositeExpression = allowCompositeExpression;
        this.varadaStatsPushdownPredicates = varadaStatsPushdownPredicates;
        ExpressionMappingParser parser = new ExpressionMappingParser(typeClasses);
        this.expressionPattern = parser.createExpressionPattern(expressionPattern);
        this.originalExpression = expressionPattern;
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return (Pattern<Call>) expressionPattern.getPattern();
    }

    @Override
    public Optional<VaradaExpression> rewrite(Call call, Captures captures, RewriteContext<VaradaExpression> context)
    {
        if (call.getArguments().size() == 2 &&
                isArrayType(call.getArguments().get(0)) &&
                isArrayType(call.getArguments().get(1))) {
            //handle case of arrayColumn = ARRAY['a', 'b'], currently not supported. see SIC-1645
            return Optional.empty();
        }
        if (call.getArguments().size() == 2 &&
                call.getArguments().get(0) instanceof Variable &&
                call.getArguments().get(1) instanceof Call) {
            //Call must contain Variable, and we don't support 2 Variables in a single predicate
            return Optional.empty();
        }
        MatchContext matchContext = new MatchContext();
        expressionPattern.resolve(captures, matchContext);
        List<VaradaExpression> arguments = new ArrayList<>();
        Matcher matcher = REWRITE_TOKENS.matcher(originalExpression);
        VaradaVariable varadaVariable = null;
        while (matcher.find()) {
            String identifier = matcher.group(0);
            Optional<Object> capture = matchContext.getIfPresent(identifier);
            if (capture.isPresent()) {
                Object value = capture.get();
                if (value instanceof ConnectorExpression) {
                    Optional<VaradaExpression> rewrittenExpression = context.defaultRewrite((ConnectorExpression) value);
                    if (rewrittenExpression.isEmpty()) {
                        return Optional.empty();
                    }
                    else if (rewrittenExpression.get() instanceof VaradaCall && !allowCompositeExpression) {
                        varadaStatsPushdownPredicates.incunsupported_functions_composite();
                        return Optional.empty();
                    }
                    if (rewrittenExpression.get() instanceof VaradaVariable variable) {
                        if (varadaVariable == null || varadaVariable.equals(variable)) {
                            varadaVariable = variable;
                            arguments.add(rewrittenExpression.get());
                        }
                        else {
                            varadaStatsPushdownPredicates.incunsupported_functions();
                            return Optional.empty();
                        }
                    }
                    else {
                        arguments.add(rewrittenExpression.get());
                    }
                }
                else {
                    varadaStatsPushdownPredicates.incunsupported_functions();
                    logger.error("Unsupported expression value: %s (%s)", value, value.getClass());
                    return Optional.empty();
                }
            }
        }
        VaradaExpression value = new VaradaCall(call.getFunctionName().getName(), arguments, call.getType());
        return Optional.of(value);
    }

    private boolean isArrayType(ConnectorExpression connectorExpression)
    {
        return (connectorExpression instanceof Variable variable && variable.getType() instanceof ArrayType) ||
                (connectorExpression instanceof Constant constant && constant.getType() instanceof ArrayType);
    }
}
