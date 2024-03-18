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

import io.airlift.slice.Slice;
import io.trino.matching.Pattern;
import io.trino.plugin.varada.expression.NativeExpression;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaConstant;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.expression.VaradaVariable;
import io.trino.plugin.varada.expression.rewrite.ExpressionPatterns;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.stats.VaradaStatsPushdownPredicates;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static io.trino.plugin.varada.expression.rewrite.coordinator.varadatonative.CallAndConstantRewriter.EQUAL_FUNCTION;
import static io.trino.plugin.varada.expression.rewrite.coordinator.varadatonative.VariableRewriter.calcPredicateType;
import static io.trino.plugin.varada.expression.rewrite.coordinator.varadatonative.VariableRewriter.convertSingleValueToDomain;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

class InNativeRewriter
        implements ExpressionRewriter<VaradaCall>

{
    private static final Pattern<VaradaCall> PATTERN = ExpressionPatterns.call()
            .with(ExpressionPatterns.functionName().equalTo(IN_PREDICATE_FUNCTION_NAME.getName()))
            .with(ExpressionPatterns.type().equalTo(BOOLEAN))
            .with(ExpressionPatterns.argumentCount().equalTo(2))
            .with(ExpressionPatterns.argument(1).matching(ExpressionPatterns.call().with(ExpressionPatterns.functionName().equalTo(ARRAY_CONSTRUCTOR_FUNCTION_NAME.getName()))));
    private final NativeExpressionRulesHandler nativeExpressionRulesHandler;
    private final VaradaStatsPushdownPredicates varadaStatsPushdownPredicates;

    InNativeRewriter(NativeExpressionRulesHandler nativeExpressionRulesHandler, VaradaStatsPushdownPredicates varadaStatsPushdownPredicates)
    {
        this.nativeExpressionRulesHandler = requireNonNull(nativeExpressionRulesHandler);
        this.varadaStatsPushdownPredicates = requireNonNull(varadaStatsPushdownPredicates);
    }

    @Override
    public Pattern<VaradaCall> getPattern()
    {
        return PATTERN;
    }

    boolean convertIn(VaradaExpression varadaExpression, RewriteContext rewriteContext)
    {
        VaradaExpression child0 = varadaExpression.getChildren().get(0);
        boolean validValue;
        if (child0 instanceof VaradaVariable) {
            rewriteContext.nativeExpressionBuilder().functionType(FunctionType.FUNCTION_TYPE_NONE);
            validValue = true;
        }
        else {
            validValue = nativeExpressionRulesHandler.rewrite(child0, rewriteContext);
        }
        if (!validValue) {
            return false;
        }
        List<? extends VaradaExpression> inConstantValues = varadaExpression.getChildren().get(1).getChildren();
        if (inConstantValues.isEmpty()) {
            return false;
        }
        boolean valid = true;
        Type domainType;
        List<Object> functionParams = Collections.emptyList();
        try {
            Function<VaradaConstant, Optional<Range>> convertToRangeFunc;
            NativeExpression.Builder nativeExpressionBuilder = rewriteContext.nativeExpressionBuilder();
            Type columnType = rewriteContext.columnType();
            if (columnType instanceof MapType mapType) {
                convertToRangeFunc = constant -> Optional.of(Range.equal(constant.getType(), constant.getValue()));
                domainType = mapType.getValueType();
            }
            else {
                Type castType = inConstantValues.get(0).getType();
                boolean isCastExpression = columnType != castType;
                if (isCastExpression) {
                    boolean isCastToVarchar = castType instanceof VarcharType;
                    if (isCastToVarchar) {
                        convertToRangeFunc = constant -> {
                            Slice slice = (Slice) constant.getValue();
                            Domain singleValueDomain = convertSingleValueToDomain(slice, columnType, ((VarcharType) castType).getLength());
                            if (singleValueDomain.isNone()) {
                                return Optional.empty();
                            }
                            else {
                                return Optional.of(singleValueDomain.getValues().getRanges().getOrderedRanges().get(0));
                            }
                        };
                        domainType = columnType;
                        functionParams = nativeExpressionBuilder.getFunctionParams();
                    }
                    else {
                        convertToRangeFunc = constant -> {
                            Type type = constant.getType();
                            Object value = constant.getValue();
                            return Optional.of(EQUAL_FUNCTION.apply(type, value));
                        };
                        domainType = castType;
                    }
                }
                else {
                    convertToRangeFunc = constant -> Optional.of(Range.equal(constant.getType(), constant.getValue()));
                    domainType = columnType;
                }
            }
            List<Range> ranges = new ArrayList<>();
            for (VaradaExpression value : inConstantValues) {
                Optional<Range> range = convertToRangeFunc.apply((VaradaConstant) value);
                range.ifPresent(ranges::add);
            }
            Domain domain;
            if (ranges.isEmpty()) {
                domain = Domain.none(domainType);
            }
            else {
                domain = Domain.create(SortedRangeSet.copyOf(domainType, ranges), false);
            }
            PredicateType predicateType = calcPredicateType(domainType, EQUAL_OPERATOR_FUNCTION_NAME.getName());
            nativeExpressionBuilder.domain(domain)
                    .predicateType(predicateType)
                    .collectNulls(domain.isNullAllowed())
                    .functionParams(functionParams);
        }
        catch (Exception e) {
            logger.debug("failed to convertInFunction. error=%s, varadaExpression=%s", e, varadaExpression);
            varadaStatsPushdownPredicates.incunsupported_functions_native();
            valid = false;
        }
        return valid;
    }
}
