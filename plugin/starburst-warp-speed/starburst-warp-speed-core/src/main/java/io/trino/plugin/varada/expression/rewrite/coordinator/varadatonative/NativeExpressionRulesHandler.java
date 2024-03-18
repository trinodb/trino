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
package io.trino.plugin.varada.expression.rewrite.coordinator.varadatonative;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.matching.Pattern;
import io.trino.plugin.varada.expression.NativeExpression;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.gen.stats.VaradaStatsPushdownPredicates;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

import static io.trino.plugin.varada.expression.rewrite.ExpressionService.PUSHDOWN_PREDICATES_STAT_GROUP;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.CEIL;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.DAY;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.DAY_OF_MONTH;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.DAY_OF_WEEK;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.DAY_OF_YEAR;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.DOW;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.DOY;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.ELEMENT_AT;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.IS_NAN;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.JSON_EXTRACT_SCALAR;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.LOWER;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.UPPER;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.WEEK;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.WEEK_OF_YEAR;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.YEAR_OF_WEEK;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.YOW;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;

@Singleton
public class NativeExpressionRulesHandler
{
    private static final Logger logger = Logger.get(NativeExpressionRulesHandler.class);
    private final VaradaStatsPushdownPredicates varadaStatsPushdownPredicates;

    private final SetMultimap<String, RewriteRule> functionToRewriteRules;

    @Inject
    public NativeExpressionRulesHandler(StorageEngineConstants storageEngineConstants, MetricsManager metricsManager)
    {
        this.varadaStatsPushdownPredicates = metricsManager.registerMetric(VaradaStatsPushdownPredicates.create(PUSHDOWN_PREDICATES_STAT_GROUP));

        VariableRewriter variableRewriter = new VariableRewriter(storageEngineConstants, varadaStatsPushdownPredicates);
        InNativeRewriter inNativeRewriter = new InNativeRewriter(this, varadaStatsPushdownPredicates);
        CallAndConstantRewriter callAndConstantRewriter = new CallAndConstantRewriter(this, varadaStatsPushdownPredicates);
        VariableAndConstantRewriter variableAndConstantRewriter = new VariableAndConstantRewriter(this, varadaStatsPushdownPredicates);
        AndOrRewriter andOrRewriter = new AndOrRewriter(this, varadaStatsPushdownPredicates);
        FunctionsWithCastRewriter functionsWithCastRewriter = new FunctionsWithCastRewriter(this);

        functionToRewriteRules = HashMultimap.create();
        functionToRewriteRules.put(DAY_OF_MONTH.getName(), new RewriteRule(functionsWithCastRewriter.getPattern(), functionsWithCastRewriter::day));
        functionToRewriteRules.put(DAY.getName(), new RewriteRule(functionsWithCastRewriter.getPattern(), functionsWithCastRewriter::day));
        functionToRewriteRules.put(DOW.getName(), new RewriteRule(functionsWithCastRewriter.getPattern(), functionsWithCastRewriter::dayOfWeek));
        functionToRewriteRules.put(DAY_OF_WEEK.getName(), new RewriteRule(functionsWithCastRewriter.getPattern(), functionsWithCastRewriter::dayOfWeek));
        functionToRewriteRules.put(DAY_OF_YEAR.getName(), new RewriteRule(functionsWithCastRewriter.getPattern(), functionsWithCastRewriter::dayOfYear));
        functionToRewriteRules.put(DOY.getName(), new RewriteRule(functionsWithCastRewriter.getPattern(), functionsWithCastRewriter::dayOfYear));
        functionToRewriteRules.put(WEEK_OF_YEAR.getName(), new RewriteRule(functionsWithCastRewriter.getPattern(), functionsWithCastRewriter::week));
        functionToRewriteRules.put(WEEK.getName(), new RewriteRule(functionsWithCastRewriter.getPattern(), functionsWithCastRewriter::week));
        functionToRewriteRules.put(YOW.getName(), new RewriteRule(functionsWithCastRewriter.getPattern(), functionsWithCastRewriter::yearOfWeek));
        functionToRewriteRules.put(YEAR_OF_WEEK.getName(), new RewriteRule(functionsWithCastRewriter.getPattern(), functionsWithCastRewriter::yearOfWeek));
        functionToRewriteRules.put(GREATER_THAN_OPERATOR_FUNCTION_NAME.getName(), new RewriteRule(callAndConstantRewriter.getPattern(), callAndConstantRewriter::greaterThan));
        functionToRewriteRules.put(GREATER_THAN_OPERATOR_FUNCTION_NAME.getName(), new RewriteRule(variableAndConstantRewriter.getPattern(), variableAndConstantRewriter::greaterThan));
        functionToRewriteRules.put(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.getName(), new RewriteRule(callAndConstantRewriter.getPattern(), callAndConstantRewriter::greaterThanOrEqual));
        functionToRewriteRules.put(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.getName(), new RewriteRule(variableAndConstantRewriter.getPattern(), variableAndConstantRewriter::greaterThanOrEqual));
        functionToRewriteRules.put(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.getName(), new RewriteRule(callAndConstantRewriter.getPattern(), callAndConstantRewriter::lessThanOrEqual));
        functionToRewriteRules.put(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.getName(), new RewriteRule(variableAndConstantRewriter.getPattern(), variableAndConstantRewriter::lessThanOrEqual));
        functionToRewriteRules.put(LESS_THAN_OPERATOR_FUNCTION_NAME.getName(), new RewriteRule(callAndConstantRewriter.getPattern(), callAndConstantRewriter::lessThan));
        functionToRewriteRules.put(LESS_THAN_OPERATOR_FUNCTION_NAME.getName(), new RewriteRule(variableAndConstantRewriter.getPattern(), variableAndConstantRewriter::lessThan));
        functionToRewriteRules.put(EQUAL_OPERATOR_FUNCTION_NAME.getName(), new RewriteRule(callAndConstantRewriter.getPattern(), callAndConstantRewriter::equal));
        functionToRewriteRules.put(EQUAL_OPERATOR_FUNCTION_NAME.getName(), new RewriteRule(variableAndConstantRewriter.getPattern(), variableAndConstantRewriter::equal));
        functionToRewriteRules.put(OR_FUNCTION_NAME.getName(), new RewriteRule(andOrRewriter.getPattern(), andOrRewriter::or));
        functionToRewriteRules.put(AND_FUNCTION_NAME.getName(), new RewriteRule(andOrRewriter.getPattern(), andOrRewriter::and));
        functionToRewriteRules.put(CAST_FUNCTION_NAME.getName(), new RewriteRule(variableRewriter.getPattern(), variableRewriter::cast));
        functionToRewriteRules.put(CEIL.getName(), new RewriteRule(variableRewriter.getPattern(), variableRewriter::ceil));
        functionToRewriteRules.put(IS_NAN.getName(), new RewriteRule(variableRewriter.getPattern(), variableRewriter::isNan));
        functionToRewriteRules.put(DAY.getName(), new RewriteRule(variableRewriter.getPattern(), variableRewriter::day));
        functionToRewriteRules.put(DAY_OF_MONTH.getName(), new RewriteRule(variableRewriter.getPattern(), variableRewriter::day));
        functionToRewriteRules.put(DAY_OF_WEEK.getName(), new RewriteRule(variableRewriter.getPattern(), variableRewriter::dayOfWeek));
        functionToRewriteRules.put(DOW.getName(), new RewriteRule(variableRewriter.getPattern(), variableRewriter::dayOfWeek));
        functionToRewriteRules.put(DAY_OF_YEAR.getName(), new RewriteRule(variableRewriter.getPattern(), variableRewriter::dayOfYear));
        functionToRewriteRules.put(DOY.getName(), new RewriteRule(variableRewriter.getPattern(), variableRewriter::dayOfYear));
        functionToRewriteRules.put(WEEK.getName(), new RewriteRule(variableRewriter.getPattern(), variableRewriter::week));
        functionToRewriteRules.put(WEEK_OF_YEAR.getName(), new RewriteRule(variableRewriter.getPattern(), variableRewriter::week));
        functionToRewriteRules.put(YEAR_OF_WEEK.getName(), new RewriteRule(variableRewriter.getPattern(), variableRewriter::yearOfWeek));
        functionToRewriteRules.put(YOW.getName(), new RewriteRule(variableRewriter.getPattern(), variableRewriter::yearOfWeek));
        functionToRewriteRules.put(LOWER.getName(), new RewriteRule(variableRewriter.getPattern(), variableRewriter::lower));
        functionToRewriteRules.put(UPPER.getName(), new RewriteRule(variableRewriter.getPattern(), variableRewriter::upper));
        functionToRewriteRules.put(ELEMENT_AT.getName(), new RewriteRule(variableAndConstantRewriter.getPattern(), variableAndConstantRewriter::elementAt));
        functionToRewriteRules.put(IN_PREDICATE_FUNCTION_NAME.getName(), new RewriteRule(inNativeRewriter.getPattern(), inNativeRewriter::convertIn));
        functionToRewriteRules.put(JSON_EXTRACT_SCALAR.getName(), new RewriteRule(variableAndConstantRewriter.getPattern(), variableAndConstantRewriter::jsonExtractScalar));
    }

    public Optional<NativeExpression> rewrite(VaradaExpression varadaExpression,
            Type columnType,
            Set<String> unsupportedNativeFunctions,
            Map<String, Long> customStats)
    {
        Optional<NativeExpression> res = Optional.empty();
        try {
            RewriteContext context = new RewriteContext(NativeExpression.builder(),
                    columnType,
                    unsupportedNativeFunctions,
                    customStats);
            boolean isValid = rewrite(varadaExpression, context);
            if (isValid) {
                res = Optional.of(context.nativeExpressionBuilder().build());
            }
        }
        catch (Exception e) {
            logger.error(e, "failed to convert varadaExpression to domain pattern. %s", varadaExpression);
            varadaStatsPushdownPredicates.incfailed_rewrite_to_native_expression();
        }
        return res;
    }

    boolean rewrite(VaradaExpression varadaExpression,
            RewriteContext context)
    {
        if (!(varadaExpression instanceof VaradaCall)) {
            context.customStats().compute("unsupported_functions_native", (key, value) -> value == null ? 1L : value + 1);
            varadaStatsPushdownPredicates.incunsupported_functions_native();
            return false;
        }
        boolean isValid = false;
        String functionName = ((VaradaCall) varadaExpression).getFunctionName();
        if (context.unsupportedNativeFunctions().contains(functionName)) {
            context.customStats().compute("unsupported_functions_native", (key, value) -> value == null ? 1L : value + 1);
            varadaStatsPushdownPredicates.incunsupported_functions_native();
            return false;
        }
        Set<RewriteRule> rewriteRules = functionToRewriteRules.get(functionName);
        if (rewriteRules.isEmpty()) {
            context.customStats().compute("unsupported_functions_native", (key, value) -> value == null ? 1L : value + 1);
            varadaStatsPushdownPredicates.incunsupported_functions_native();
        }
        for (RewriteRule rule : rewriteRules) {
            if (rule.pattern.matches(varadaExpression, null)) {
                isValid = rule.rewriteCallback.apply(varadaExpression, context);
                break;
            }
        }

        return isValid;
    }

    private record RewriteRule(
            @SuppressWarnings("unused") Pattern<VaradaCall> pattern,
            @SuppressWarnings("unused") BiFunction<VaradaExpression, RewriteContext, Boolean> rewriteCallback) {}
}
