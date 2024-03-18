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
package io.trino.plugin.varada.dispatcher.query.classifier;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.TreeMultimap;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.query.PredicateContext;
import io.trino.plugin.varada.dispatcher.query.data.match.BasicBloomQueryMatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.varada.expression.NativeExpression;
import io.trino.plugin.varada.expression.VaradaExpressionData;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.varada.log.ShapingLogger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.varada.type.TypeUtils.isBigIntegerType;
import static io.trino.plugin.varada.type.TypeUtils.isDoubleType;
import static io.trino.plugin.varada.type.TypeUtils.isIntType;
import static io.trino.plugin.varada.type.TypeUtils.isRealType;
import static io.trino.plugin.varada.type.TypeUtils.isSmallIntType;
import static io.trino.plugin.varada.type.TypeUtils.isStrType;
import static io.trino.plugin.varada.type.TypeUtils.isTimestampType;
import static io.trino.plugin.varada.type.TypeUtils.isTinyIntType;

public class RangeMatcher
        implements Matcher
{
    private static final Logger logger = Logger.get(RangeMatcher.class);
    private final ShapingLogger shapingLogger;

    public RangeMatcher(GlobalConfiguration globalConfiguration)
    {
        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfiguration.getShapingLoggerThreshold(),
                globalConfiguration.getShapingLoggerDuration(),
                globalConfiguration.getShapingLoggerNumberOfSamples());
    }

    @Override
    public MatchContext match(ClassifyArgs classifyArgs, MatchContext matchContext)
    {
        if (!classifyArgs.isMinMaxFilter()) {
            return matchContext;
        }

        WarmedWarmupTypes allWarmupElements = classifyArgs.getWarmedWarmupTypes();

        Map<VaradaColumn, PredicateContext> varadaColumnPredicateContextMap = matchContext.remainingPredicateContext();
        for (Map.Entry<VaradaColumn, PredicateContext> remaining : varadaColumnPredicateContextMap.entrySet()) {
            PredicateContext context = remaining.getValue();
            Domain domain = context.getDomain();
            Type type = context.getColumnType();
            Optional<WarmUpElement> warmUpElement = findColumnInWarmupElements(remaining.getKey(), allWarmupElements);

            if (warmUpElement.isPresent() &&
                    warmUpElement.get().getRecTypeCode().isSupportedFiltering() &&
                    warmUpElement.get().getWarmupElementStats().isValidRange() &&
                    isNotFunction(context.getVaradaExpressionData()) &&
                    !warmUpElement.get().getVaradaColumn().isTransformedColumn() &&
                    domain.getValues() instanceof SortedRangeSet sortedRangeSet &&
                    !(sortedRangeSet.isAll() || sortedRangeSet.isNone()) &&
                    (!domain.isNullAllowed() || warmUpElement.get().getWarmupElementStats().getNullsCount() == 0)) {
                Object maxValue = warmUpElement.get().getWarmupElementStats().getMaxValue();
                Object minValue = warmUpElement.get().getWarmupElementStats().getMinValue();
                Optional<Range> range = Optional.empty();
                try {
                    range = createRange(type, minValue, maxValue);
                }
                catch (Exception e) {
                    shapingLogger.warn("failed on MatchRange. error=%s, %s, key=%s", e.getMessage(), warmUpElement, classifyArgs.getRowGroupData().getRowGroupKey());
                }
                boolean overlaps = true;
                if (range.isPresent()) {
                    ValueSet valueSet = ValueSet.ofRanges(range.get());
                    overlaps = valueSet.overlaps(sortedRangeSet);
                }
                if (!overlaps) {
                    List<QueryMatchData> matchDataList = new ArrayList<>();
                    Optional<NativeExpression> nativeExpression = context.getNativeExpression();
                    nativeExpression.ifPresent(expression -> matchDataList.add(BasicBloomQueryMatchData.builder()
                            .warmUpElement(warmUpElement.get())
                            .type(context.getColumnType())
                            .domain(Optional.of(domain))
                            .simplifiedDomain(context.isSimplified())
                            .nativeExpression(expression)
                            .tightnessRequired(classifyArgs.getDispatcherTableHandle().isSubsumedPredicates())
                            .build()));

                    matchContext = new MatchContext(matchDataList, Collections.emptyMap(), false);
                    break;
                }
            }
        }
        return matchContext;
    }

    private boolean isNotFunction(VaradaExpressionData expressionData)
    {
        Optional<NativeExpression> nativeExpression = expressionData.getNativeExpressionOptional();
        return nativeExpression.isPresent() &&
                nativeExpression.get().functionType() == FunctionType.FUNCTION_TYPE_NONE;
    }

    private Optional<WarmUpElement> findColumnInWarmupElements(VaradaColumn column, WarmedWarmupTypes allWarmupElements)
    {
        ImmutableListMultimap<VaradaColumn, WarmUpElement> basicWarmupElements = allWarmupElements.basicWarmedElements();
        ImmutableMap<VaradaColumn, WarmUpElement> dataWarmupElements = allWarmupElements.dataWarmedElements();
        TreeMultimap<VaradaColumn, WarmUpElement> bloomWarmupElements = allWarmupElements.bloomWarmedElements();
        ImmutableMap<VaradaColumn, WarmUpElement> luceneWarmupElements = allWarmupElements.luceneWarmedElements();

        Optional<WarmUpElement> element;
        if (luceneWarmupElements.containsKey(column)) {
            element = Optional.empty();
        }
        else {
            element = basicWarmupElements.get(column).stream()
                    .filter(elem -> !elem.getVaradaColumn().isTransformedColumn())
                    .findFirst();
            if (element.isEmpty()) {
                element = Optional.ofNullable(dataWarmupElements.get(column));
            }

            if (element.isEmpty()) {
                element = bloomWarmupElements.get(column).stream().filter(x -> !x.getVaradaColumn().isTransformedColumn()).findAny();
            }
        }

        return element;
    }

    private Optional<Range> createRange(Type type, Object minValue, Object maxValue)
    {
        Optional<Range> res;
        //values of smallInt and TinyInt converted to int during import
        if ((isIntType(type) || isSmallIntType(type) || isTinyIntType(type)) && minValue instanceof Integer intMinValue && maxValue instanceof Integer intMaxValue) {
            res = Optional.of(Range.range(type, intMinValue.longValue(), true, intMaxValue.longValue(), true));
        }
        else if (isDoubleType(type) && minValue instanceof Double doubleMinValue && maxValue instanceof Double doubleMaxValue) {
            res = Optional.of(Range.range(type, doubleMinValue, true, doubleMaxValue, true));
        }
        else if ((isBigIntegerType(type) || isTimestampType(type)) &&
                (minValue instanceof Integer ||
                        minValue instanceof Long ||
                        maxValue instanceof Integer ||
                        maxValue instanceof Long)) {
            //values may be converted to int during import
            Object minVal = minValue instanceof Integer ? ((Integer) minValue).longValue() : minValue;
            Object maxVal = maxValue instanceof Integer ? ((Integer) maxValue).longValue() : maxValue;
            res = Optional.of(Range.range(type, minVal, true, maxVal, true));
        }
        else if (isRealType(type) && minValue instanceof Float intMinValue && maxValue instanceof Float intMaxValue) {
            res = Optional.of(Range.range(type, (long) Float.floatToIntBits(intMinValue), true, (long) Float.floatToIntBits(intMaxValue), true));
        }
        else if (isSmallIntType(type) && minValue instanceof Short shortMinValue && maxValue instanceof Short shortMaxValue) {
            res = Optional.of(Range.range(type, shortMinValue.longValue(), true, shortMaxValue.longValue(), true));
        }
        else if (isTinyIntType(type) && minValue instanceof Byte byteMinValue && maxValue instanceof Byte byteMaxValue) {
            res = Optional.of(Range.range(type, byteMinValue.longValue(), true, byteMaxValue.longValue(), true));
        }
        else if (isStrType(type)) {
            Slice min;
            Slice max;
            if (minValue instanceof String minVal && maxValue instanceof String maxVal) {
                //todo:in fast warming byte array converted to String, need to check
                min = Slices.utf8Slice(minVal);
                max = Slices.utf8Slice(maxVal);
                res = Optional.of(Range.range(type, min, true, max, true));
            }
            else {
                throw new IllegalArgumentException();
            }
        }
        else {
            String errorMsg = String.format("cant find match for type=%s minValue=%s, maxValue=%s", type, minValue.getClass().getName(), maxValue.getClass().getName());
            throw new IllegalArgumentException(errorMsg);
        }
        return res;
    }
}
