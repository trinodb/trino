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
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.TransformedColumn;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.query.PredicateContext;
import io.trino.plugin.varada.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.varada.expression.NativeExpression;
import io.trino.plugin.varada.expression.TransformFunction;
import io.trino.plugin.varada.expression.VaradaExpressionData;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.IntegerType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RangeMatcherTest
        extends ClassifierTest
{
    private RangeMatcher rangeMatcher;

    @BeforeEach
    public void before()
    {
        init();
    }

    /**
     * warmupElementStats [-100, 100]
     * domain - 0
     */
    @Test
    public void testRangeMatcherValidRange()
    {
        ClassifyArgs classifyArgs = mock(ClassifyArgs.class);
        Domain domain = Domain.singleValue(IntegerType.INTEGER, 0L);
        QueryMatchData queryMatchData = mock(QueryMatchData.class);
        when(queryMatchData.getDomain()).thenReturn(Optional.of(domain));
        when(queryMatchData.isFunction()).thenReturn(false);
        when(queryMatchData.getLeavesDFS()).thenReturn(List.of(queryMatchData));
        WarmUpElement warmUpElement = mock(WarmUpElement.class);
        when(warmUpElement.getRecTypeCode()).thenReturn(RecTypeCode.REC_TYPE_INTEGER);
        when(queryMatchData.getWarmUpElement()).thenReturn(warmUpElement);
        WarmupElementStats warmupElementStats = new WarmupElementStats(1, -100, 100);
        when(queryMatchData.getWarmUpElement()).thenReturn(warmUpElement);
        when(warmUpElement.getWarmupElementStats()).thenReturn(warmupElementStats);
        rangeMatcher = new RangeMatcher(new GlobalConfiguration());
        Map<VaradaColumn, PredicateContext> remainingPredicateContext = Map.of(new RegularColumn("remainingColumn"), mock(PredicateContext.class));
        MatchContext matchContext = new MatchContext(List.of(queryMatchData), remainingPredicateContext, true);

        MatchContext res = rangeMatcher.match(classifyArgs, matchContext);

        assertThat(res.validRange()).isTrue();
        assertThat(res.remainingPredicateContext()).isEqualTo(remainingPredicateContext);
    }

    static Stream<Arguments> rangeConfiguration()
    {
        ClassifyArgs classifyArgs = mock(ClassifyArgs.class);
        when(classifyArgs.isMinMaxFilter()).thenReturn(true);
        ClassifyArgs classifyArgsFeatureDisabled = mock(ClassifyArgs.class);
        return Stream.of(
                arguments(classifyArgs, false),
                arguments(classifyArgsFeatureDisabled, true));
    }

    /**
     * warmupElementStats [1, 100]
     * domain - 0
     */
    @ParameterizedTest
    @MethodSource("rangeConfiguration")
    public void testRangeMatcherInvalidRange(ClassifyArgs classifyArgs, boolean expectedValidRange)
    {
        QueryMatchData queryMatchData = mock(QueryMatchData.class);
        WarmUpElement warmUpElement = mock(WarmUpElement.class);
        when(warmUpElement.getRecTypeCode()).thenReturn(RecTypeCode.REC_TYPE_INTEGER);
        WarmupElementStats warmupElementStats = new WarmupElementStats(1, 1, 100);
        when(queryMatchData.getWarmUpElement()).thenReturn(warmUpElement);
        when(warmUpElement.getVaradaColumn()).thenReturn(new RegularColumn("aaa"));
        when(warmUpElement.getWarmupElementStats()).thenReturn(warmupElementStats);

        RegularColumn column = new RegularColumn("remainingColumn");
        PredicateContext context = mock(PredicateContext.class);
        IntegerType type = IntegerType.INTEGER;
        Domain domain = Domain.singleValue(type, 0L);
        when(context.getDomain()).thenReturn(domain);
        when(context.getColumnType()).thenReturn(type);

        Optional<NativeExpression> nativeExpression = Optional.of(mock(NativeExpression.class));
        when(nativeExpression.get().functionType()).thenReturn(FunctionType.FUNCTION_TYPE_NONE);
        VaradaExpressionData expressionData = mock(VaradaExpressionData.class);
        when(expressionData.getNativeExpressionOptional()).thenReturn(nativeExpression);
        when(context.getVaradaExpressionData()).thenReturn(expressionData);

        WarmedWarmupTypes warmupTypes = mock(WarmedWarmupTypes.class);
        ImmutableListMultimap<VaradaColumn, WarmUpElement> basicWarmUpElements = ImmutableListMultimap.of(column, warmUpElement);
        when(warmupTypes.basicWarmedElements()).thenReturn(basicWarmUpElements);
        when(warmupTypes.luceneWarmedElements()).thenReturn(ImmutableMap.of());
        when(classifyArgs.getWarmedWarmupTypes()).thenReturn(warmupTypes);

        rangeMatcher = new RangeMatcher(new GlobalConfiguration());
        Map<VaradaColumn, PredicateContext> remainingPredicateContext = Map.of(column, context);
        MatchContext matchContext = new MatchContext(List.of(queryMatchData), remainingPredicateContext, true);

        MatchContext res = rangeMatcher.match(classifyArgs, matchContext);

        assertThat(res.validRange()).isEqualTo(expectedValidRange);
        if (res.validRange()) {
            assertThat(res.remainingPredicateContext()).isEqualTo(remainingPredicateContext);
        }
        else {
            assertThat(res.remainingPredicateContext()).isEmpty();   // no need to perform further filtering
        }
    }

    @ParameterizedTest
    @MethodSource("rangeConfiguration")
    public void testDataRangeMatcherInvalidRange(ClassifyArgs classifyArgs, boolean expectedValidRange)
    {
        QueryMatchData queryMatchData = mock(QueryMatchData.class);
        WarmUpElement warmUpElement = mock(WarmUpElement.class);
        when(warmUpElement.getRecTypeCode()).thenReturn(RecTypeCode.REC_TYPE_INTEGER);
        WarmupElementStats warmupElementStats = new WarmupElementStats(1, 1, 100);
        when(queryMatchData.getWarmUpElement()).thenReturn(warmUpElement);
        when(warmUpElement.getVaradaColumn()).thenReturn(new RegularColumn("aaa"));
        when(warmUpElement.getWarmupElementStats()).thenReturn(warmupElementStats);

        RegularColumn column = new RegularColumn("remainingColumn");
        PredicateContext context = mock(PredicateContext.class);
        IntegerType type = IntegerType.INTEGER;
        Domain domain = Domain.singleValue(type, 0L);
        when(context.getDomain()).thenReturn(domain);
        when(context.getColumnType()).thenReturn(type);

        Optional<NativeExpression> nativeExpression = Optional.of(mock(NativeExpression.class));
        when(nativeExpression.get().functionType()).thenReturn(FunctionType.FUNCTION_TYPE_NONE);
        VaradaExpressionData expressionData = mock(VaradaExpressionData.class);
        when(expressionData.getNativeExpressionOptional()).thenReturn(nativeExpression);
        when(context.getVaradaExpressionData()).thenReturn(expressionData);

        WarmedWarmupTypes warmupTypes = mock(WarmedWarmupTypes.class);
        ImmutableListMultimap<VaradaColumn, WarmUpElement> basicWarmUpElements = ImmutableListMultimap.of();
        when(warmupTypes.basicWarmedElements()).thenReturn(basicWarmUpElements);
        ImmutableMap<VaradaColumn, WarmUpElement> dataElements = ImmutableMap.of(column, warmUpElement);
        when(warmupTypes.dataWarmedElements()).thenReturn(dataElements);
        when(warmupTypes.luceneWarmedElements()).thenReturn(ImmutableMap.of());
        when(classifyArgs.getWarmedWarmupTypes()).thenReturn(warmupTypes);

        rangeMatcher = new RangeMatcher(new GlobalConfiguration());
        Map<VaradaColumn, PredicateContext> remainingPredicateContext = Map.of(column, context);
        MatchContext matchContext = new MatchContext(List.of(queryMatchData), remainingPredicateContext, true);

        MatchContext res = rangeMatcher.match(classifyArgs, matchContext);

        assertThat(res.validRange()).isEqualTo(expectedValidRange);
        if (res.validRange()) {
            assertThat(res.remainingPredicateContext()).isEqualTo(remainingPredicateContext);
        }
        else {
            assertThat(res.remainingPredicateContext()).isEmpty();   // no need to perform further filtering
        }
    }

    @Test
    public void testBasicRangeMatcherWithTransformedColumn()
    {
        ClassifyArgs classifyArgs = mock(ClassifyArgs.class);
        when(classifyArgs.isMinMaxFilter()).thenReturn(true);

        QueryMatchData queryMatchData = mock(QueryMatchData.class);
        WarmUpElement warmUpElement = mock(WarmUpElement.class);
        when(queryMatchData.getWarmUpElement()).thenReturn(warmUpElement);
        TransformFunction func = new TransformFunction(TransformFunction.TransformType.LOWER);
        TransformedColumn transformedColumn = new TransformedColumn("remainingColumn", "remainingColumn", func);
        when(warmUpElement.getVaradaColumn()).thenReturn(transformedColumn);

        WarmedWarmupTypes warmupTypes = mock(WarmedWarmupTypes.class);
        ImmutableListMultimap<VaradaColumn, WarmUpElement> basicWarmUpElements = ImmutableListMultimap.of(transformedColumn, warmUpElement);
        when(warmupTypes.basicWarmedElements()).thenReturn(basicWarmUpElements);
        when(warmupTypes.dataWarmedElements()).thenReturn(ImmutableMap.of());

        TreeMultimap<VaradaColumn, WarmUpElement> bloomMap = TreeMultimap.create(Comparator.comparing(VaradaColumn::toString), Comparator.comparing(WarmUpElement::getWarmUpType));
        when(warmupTypes.bloomWarmedElements()).thenReturn(bloomMap);
        when(warmupTypes.luceneWarmedElements()).thenReturn(ImmutableMap.of());
        when(classifyArgs.getWarmedWarmupTypes()).thenReturn(warmupTypes);

        PredicateContext context = mock(PredicateContext.class);
        rangeMatcher = new RangeMatcher(new GlobalConfiguration());
        Map<VaradaColumn, PredicateContext> remainingPredicateContext = Map.of(transformedColumn, context);
        MatchContext matchContext = new MatchContext(List.of(queryMatchData), remainingPredicateContext, true);

        MatchContext res = rangeMatcher.match(classifyArgs, matchContext);

        assertThat(res.validRange()).isEqualTo(true);
        assertThat(res.remainingPredicateContext()).isEqualTo(remainingPredicateContext);
    }
}
