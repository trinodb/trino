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

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.connector.TestingConnectorProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.query.PredicateContext;
import io.trino.plugin.varada.dispatcher.query.data.match.LuceneQueryMatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.expression.VaradaExpressionData;
import io.trino.plugin.varada.expression.VaradaPrimitiveConstant;
import io.trino.plugin.varada.expression.VaradaSliceConstant;
import io.trino.plugin.varada.expression.VaradaVariable;
import io.trino.plugin.varada.expression.rewrite.WarpExpression;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.CONTAINS;
import static io.trino.plugin.varada.storage.lucene.LuceneQueryUtils.createContainsQuery;
import static io.trino.plugin.varada.storage.lucene.LuceneQueryUtils.createLikeQuery;
import static io.trino.plugin.varada.storage.lucene.LuceneQueryUtils.createPrefixQuery;
import static io.trino.plugin.varada.storage.lucene.LuceneQueryUtils.createRangeQuery;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LuceneMatcherTest
        extends ClassifierTest
{
    private LuceneElementsMatcher luceneElementsMatcher;
    private RowGroupData rowGroupData;

    static Stream<Arguments> testVaradaExpressionsParams()
    {
        Slice comparisonValue = Slices.utf8Slice("str");
        Type type = VarcharType.createVarcharType(5);

        Query lessThan = createRangeQuery(Range.lessThan(type, comparisonValue));
        Query greaterThan = createRangeQuery(Range.greaterThan(type, comparisonValue));
        Query notEqualsQuery = new BooleanQuery.Builder()
                .add(lessThan, BooleanClause.Occur.SHOULD)
                .add(greaterThan, BooleanClause.Occur.SHOULD)
                .build();

        return Stream.of(
                arguments(CONTAINS.getName(), comparisonValue, createContainsQuery(comparisonValue)),
                arguments(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME.getName(), comparisonValue, createRangeQuery(Range.equal(type, comparisonValue))),
                arguments(StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME.getName(), comparisonValue, notEqualsQuery),
                arguments(StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME.getName(), comparisonValue, createRangeQuery(Range.lessThan(type, comparisonValue))),
                arguments(StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.getName(), comparisonValue, createRangeQuery(Range.lessThanOrEqual(type, comparisonValue))),
                arguments(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME.getName(), comparisonValue, createRangeQuery(Range.greaterThan(type, comparisonValue))),
                arguments(StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.getName(), comparisonValue, createRangeQuery(Range.greaterThanOrEqual(type, comparisonValue))));
    }

    @BeforeEach
    public void before()
    {
        init();
        this.predicateContextFactory = new PredicateContextFactory(new GlobalConfiguration(),
                new TestingConnectorProxiedConnectorTransformer());
        luceneElementsMatcher = new LuceneElementsMatcher(dispatcherProxiedConnectorTransformer);
        rowGroupData = mock(RowGroupData.class);
    }

    @Test
    public void testMultipleLuceneMatchColumns()
    {
        int totalLuceneMatches = 5;

        List<ColumnHandle> columnHandles = IntStream.range(0, totalLuceneMatches)
                .mapToObj(i -> mockColumnHandle("col" + i, varcharType, dispatcherProxiedConnectorTransformer))
                .collect(Collectors.toList());
        WarmedWarmupTypes warmUpElementByType = createColumnToWarmUpElementByType(columnHandles, WarmUpType.WARM_UP_TYPE_LUCENE);

        Map<ColumnHandle, Domain> columnDomains = columnHandles
                .stream().collect(Collectors.toMap(
                        Function.identity(),
                        columnHandle -> Domain.singleValue(VarcharType.createVarcharType(5), Slices.wrappedBuffer("str".getBytes(Charset.defaultCharset())))));
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(columnDomains);
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(tupleDomain);
        PredicateContextData predicateContext = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        Map<VaradaColumn, PredicateContext> remainingPredicateContext = predicateContext.getLeaves()
                .entrySet().stream().collect(Collectors.toMap(x -> x.getValue().getVaradaColumn(), Map.Entry::getValue));
        ClassifyArgs classifyArgs = new ClassifyArgs(dispatcherTableHandle,
                rowGroupData,
                mock(PredicateContextData.class),
                ImmutableMap.of(),
                warmUpElementByType,
                false,
                true,
                false);

        MatchContext matchContext = new MatchContext(Collections.emptyList(), remainingPredicateContext, true);

        MatchContext result = luceneElementsMatcher.match(classifyArgs, matchContext);

        assertThat(result.matchDataList().size()).isEqualTo(totalLuceneMatches);
        assertTrue(result.remainingPredicateContext().isEmpty());
    }

    @Test
    public void testAsciiRangeShouldBeHandledByLucene()
    {
        String columnName = "col1";
        ColumnHandle columnHandle = mockColumnHandle(columnName, varcharType, dispatcherProxiedConnectorTransformer);

        List<ColumnHandle> columnHandles = List.of(columnHandle);
        WarmedWarmupTypes warmUpElementByType = createColumnToWarmUpElementByType(columnHandles, WarmUpType.WARM_UP_TYPE_LUCENE);

        Slice caseSensitiveSlice = Slices.utf8Slice("Aa Bb");

        Range range = Range.lessThanOrEqual(varcharType, caseSensitiveSlice);

        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(varcharType, List.of(range));
        Domain domain = Domain.create(sortedRangeSet, true);
        Map<ColumnHandle, Domain> columnDomains = columnHandles
                .stream().collect(Collectors.toMap(
                        Function.identity(),
                        columnHandleTmp -> domain));
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(columnDomains);
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(tupleDomain);
        PredicateContextData predicateContext = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        Map<VaradaColumn, PredicateContext> remainingPredicateContext = predicateContext.getLeaves()
                .entrySet().stream().collect(Collectors.toMap(x -> x.getValue().getVaradaColumn(), Map.Entry::getValue));
        ClassifyArgs classifyArgs = new ClassifyArgs(dispatcherTableHandle,
                rowGroupData,
                mock(PredicateContextData.class),
                ImmutableMap.of(),
                warmUpElementByType,
                false,
                true,
                false);
        MatchContext matchContext = new MatchContext(Collections.emptyList(), remainingPredicateContext, true);
        MatchContext result = luceneElementsMatcher.match(classifyArgs, matchContext);

        assertThat(result.matchDataList().size()).isEqualTo(1);
    }

    @Test
    public void testAsciiRangeBetweenCapitalAndSmallLetters()
    {
        String columnName = "col1";
        ColumnHandle columnHandle = mockColumnHandle(columnName, varcharType, dispatcherProxiedConnectorTransformer);

        List<ColumnHandle> columnHandles = List.of(columnHandle);
        WarmedWarmupTypes warmUpElementByType = createColumnToWarmUpElementByType(columnHandles, WarmUpType.WARM_UP_TYPE_LUCENE);

        // '[' is > 'Z' but < 'a'
        Slice caseSensitiveSlice = Slices.utf8Slice("[");

        Range range = Range.lessThanOrEqual(varcharType, caseSensitiveSlice);

        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(varcharType, List.of(range));
        Domain domain = Domain.create(sortedRangeSet, true);
        Map<ColumnHandle, Domain> columnDomains = columnHandles
                .stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        columnHandleTmp -> domain));
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(columnDomains);

        when(dispatcherTableHandle.getFullPredicate()).thenReturn(tupleDomain);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        Map<VaradaColumn, PredicateContext> remainingPredicateContext = predicateContextData.getLeaves()
                .entrySet().stream().collect(Collectors.toMap(x -> x.getValue().getVaradaColumn(), Map.Entry::getValue));
        ClassifyArgs classifyArgs = new ClassifyArgs(dispatcherTableHandle,
                rowGroupData,
                mock(PredicateContextData.class),
                ImmutableMap.of(),
                warmUpElementByType,
                false,
                true,
                false);
        MatchContext matchContext = new MatchContext(Collections.emptyList(), remainingPredicateContext, true);

        MatchContext result = luceneElementsMatcher.match(classifyArgs, matchContext);

        assertThat(result.matchDataList().size()).isEqualTo(1);
    }

    @Test
    public void testNonAsciiRangeShouldBeHandledByLucene()
    {
        String columnName = "col1";
        ColumnHandle columnHandle = mockColumnHandle(columnName, varcharType, dispatcherProxiedConnectorTransformer);

        List<ColumnHandle> columnHandles = List.of(columnHandle);
        WarmedWarmupTypes warmUpElementByType = createColumnToWarmUpElementByType(columnHandles, WarmUpType.WARM_UP_TYPE_LUCENE);

        Slice caseSensitiveSlice = Slices.utf8Slice("2021-07");
        Slice lowSlice = Slices.utf8Slice("2021-06");

        Range range = Range.range(varcharType, lowSlice, false, caseSensitiveSlice, true);

        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(varcharType, List.of(range));
        Domain domain = Domain.create(sortedRangeSet, true);
        Map<ColumnHandle, Domain> columnDomains = columnHandles
                .stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        columnHandleTmp -> domain));
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(columnDomains);
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(tupleDomain);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        Map<VaradaColumn, PredicateContext> remainingPredicateContext = predicateContextData.getLeaves()
                .entrySet().stream().collect(Collectors.toMap(x -> x.getValue().getVaradaColumn(), Map.Entry::getValue));
        ClassifyArgs classifyArgs = new ClassifyArgs(dispatcherTableHandle,
                rowGroupData,
                mock(PredicateContextData.class),
                ImmutableMap.of(),
                warmUpElementByType,
                false,
                true,
                false);
        MatchContext matchContext = new MatchContext(Collections.emptyList(), remainingPredicateContext, true);

        MatchContext result = luceneElementsMatcher.match(classifyArgs, matchContext);

        assertThat(result.matchDataList().size()).isEqualTo(1);
        assertTrue(result.remainingPredicateContext().isEmpty());
    }

    @ParameterizedTest
    @MethodSource("testVaradaExpressionsParams")
    public void testVaradaExpressions(String comparisonFunctionName, Slice comparisonValue, Query comparisonQuery)
    {
        String columnName = "col1";
        ColumnHandle columnHandle = mockColumnHandle(columnName, varcharType, dispatcherProxiedConnectorTransformer);
        Type type = varcharType;
        VaradaVariable varadaVariable = new VaradaVariable(columnHandle, type);

        // col1 LIKE '%aa%' = TRUE
        Slice likePattern = Slices.utf8Slice("%aa%");
        VaradaCall likeEqualsTrueCall = new VaradaCall(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(new VaradaCall(LIKE_FUNCTION_NAME.getName(),
                                List.of(varadaVariable,
                                        new VaradaSliceConstant(likePattern, type)), type),
                        VaradaPrimitiveConstant.TRUE), type);
        Query likeQuery = createLikeQuery(likePattern);
        BooleanQuery mustLikeQuery = new BooleanQuery.Builder()
                .add(likeQuery, BooleanClause.Occur.MUST)
                .build();

        // STARTS_WITH(col1, 'b')
        Slice prefix = Slices.utf8Slice("b");
        VaradaCall startsWithCall = new VaradaCall("starts_with",
                List.of(varadaVariable,
                        new VaradaSliceConstant(prefix, type)), type);
        Query startsWithQuery = createPrefixQuery(prefix);

        // col1 =, !=, >, >=, <, <= 'str'
        VaradaCall comparisonFunctionCall = new VaradaCall(comparisonFunctionName,
                List.of(varadaVariable,
                        new VaradaSliceConstant(comparisonValue, type)), type);

        // <expression> AND <expression> AND <expression>
        VaradaCall andCall = new VaradaCall(StandardFunctions.AND_FUNCTION_NAME.getName(),
                List.of(likeEqualsTrueCall,
                        startsWithCall,
                        comparisonFunctionCall), type);
        BooleanQuery andQuery = new BooleanQuery.Builder()
                .add(likeQuery, BooleanClause.Occur.MUST)
                .add(startsWithQuery, BooleanClause.Occur.MUST)
                .add(comparisonQuery, BooleanClause.Occur.MUST)
                .build();

        // <expression> OR <expression> OR <expression> OR <expression>
        VaradaCall orCallWithExpressions = new VaradaCall(StandardFunctions.OR_FUNCTION_NAME.getName(),
                List.of(likeEqualsTrueCall,
                        startsWithCall,
                        comparisonFunctionCall), type);
        BooleanQuery orQuery = new BooleanQuery.Builder()
                .add(likeQuery, BooleanClause.Occur.SHOULD)
                .add(startsWithQuery, BooleanClause.Occur.SHOULD)
                .add(comparisonQuery, BooleanClause.Occur.SHOULD)
                .build();

        // A domain (domain is always ANDed with the expression)
        Range domainRange = Range.greaterThan(type, Slices.utf8Slice("AAA"));
        Domain domainWithNull = Domain.create(ValueSet.ofRanges(domainRange), true);
        BooleanQuery domainInnerQuery = new BooleanQuery.Builder()
                .add(createRangeQuery(domainRange), BooleanClause.Occur.SHOULD)
                .build();

        // <domain_query> (without expression)
        BooleanQuery domainOnlyQuery = new BooleanQuery.Builder()
                .add(domainInnerQuery, BooleanClause.Occur.MUST)
                .build();
        assertExpressionConversion(columnName, columnHandle, Optional.of(likeEqualsTrueCall), Optional.empty(), mustLikeQuery, false, false);
        assertExpressionConversion(columnName, columnHandle, Optional.of(andCall), Optional.empty(), andQuery, false, false);
        assertExpressionConversion(columnName, columnHandle, Optional.of(orCallWithExpressions), Optional.empty(), orQuery, true, true);
        assertExpressionConversion(columnName, columnHandle, Optional.empty(), Optional.of(domainWithNull), domainOnlyQuery, true, true);
    }

    @Test
    public void testVaradaExpressionEqualsAndNotEquals()
    {
        String columnName = "col1";
        ColumnHandle columnHandle = mockColumnHandle(columnName, varcharType, dispatcherProxiedConnectorTransformer);
        Type type = VarcharType.createVarcharType(5);
        VaradaVariable varadaVariable = new VaradaVariable(columnHandle, type);

        Slice likePattern = Slices.utf8Slice("%aa%");
        VaradaCall likeCall = new VaradaCall(LIKE_FUNCTION_NAME.getName(),
                List.of(varadaVariable,
                        new VaradaSliceConstant(likePattern, type)), type);
        VaradaCall equalsTrue = new VaradaCall(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(likeCall,
                        VaradaPrimitiveConstant.TRUE), type);
        VaradaCall equalsFalse = new VaradaCall(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(likeCall,
                        VaradaPrimitiveConstant.FALSE), type);
        VaradaCall notEqualsTrue = new VaradaCall(StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(likeCall,
                        VaradaPrimitiveConstant.TRUE), type);
        VaradaCall notEqualsFalse = new VaradaCall(StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(likeCall,
                        VaradaPrimitiveConstant.FALSE), type);

        Query likeQuery = createLikeQuery(likePattern);

        BooleanQuery trueQuery = new BooleanQuery.Builder()
                .add(likeQuery, BooleanClause.Occur.MUST)
                .build();

        BooleanQuery falseQuery = new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD)
                .add(likeQuery, BooleanClause.Occur.MUST_NOT)
                .build();

        assertExpressionConversion(columnName, columnHandle, Optional.of(equalsTrue), Optional.empty(), trueQuery, false, false);
        assertExpressionConversion(columnName, columnHandle, Optional.of(equalsFalse), Optional.empty(), falseQuery, false, false);
        assertExpressionConversion(columnName, columnHandle, Optional.of(notEqualsTrue), Optional.empty(), falseQuery, false, false);
        assertExpressionConversion(columnName, columnHandle, Optional.of(notEqualsFalse), Optional.empty(), trueQuery, false, false);
    }

    @Test
    public void testVaradaExpressionCantBeConverted()
    {
        String columnName = "col1";
        ColumnHandle columnHandle = mockColumnHandle(columnName, varcharType, dispatcherProxiedConnectorTransformer);
        Type type = VarcharType.createVarcharType(5);
        VaradaVariable varadaVariable = new VaradaVariable(columnHandle, type);
        Slice likePattern = Slices.utf8Slice("%aa%");

        VaradaCall likeCall = new VaradaCall(LIKE_FUNCTION_NAME.getName(),
                List.of(varadaVariable,
                        new VaradaSliceConstant(likePattern, type)), type);
        VaradaCall untranslatableCall = new VaradaCall("$unfamiliar_call",
                List.of(varadaVariable), type);
        VaradaCall orCall = new VaradaCall(StandardFunctions.OR_FUNCTION_NAME.getName(),
                List.of(likeCall,
                        untranslatableCall), type);
        VaradaCall andCall = new VaradaCall(StandardFunctions.AND_FUNCTION_NAME.getName(),
                List.of(likeCall,
                        untranslatableCall), type);
        VaradaCall andCallWithInvalidArgument = new VaradaCall(StandardFunctions.AND_FUNCTION_NAME.getName(),
                List.of(likeCall,
                        varadaVariable), type);
        VaradaCall andCallWhichCantBeConverted = new VaradaCall(StandardFunctions.AND_FUNCTION_NAME.getName(),
                List.of(untranslatableCall,
                        varadaVariable), type);

        Query likeQuery = createLikeQuery(likePattern);
        BooleanQuery mustLikeQuery = new BooleanQuery.Builder()
                .add(likeQuery, BooleanClause.Occur.MUST)
                .build();
        assertExpressionNotConverted(columnName, columnHandle, Optional.of(varadaVariable), Optional.empty());
        assertExpressionNotConverted(columnName, columnHandle, Optional.of(untranslatableCall), Optional.empty());
        assertExpressionNotConverted(columnName, columnHandle, Optional.of(orCall), Optional.empty());
        assertExpressionNotConverted(columnName, columnHandle, Optional.of(andCallWhichCantBeConverted), Optional.empty());
        assertExpressionConversion(columnName, columnHandle, Optional.of(andCall), Optional.empty(), mustLikeQuery, false, false);
        assertExpressionConversion(columnName, columnHandle, Optional.of(andCallWithInvalidArgument), Optional.empty(), mustLikeQuery, false, false);
    }

    @Test
    public void testNoPredicate()
    {
        String columnName = "col1";
        ColumnHandle columnHandle = mockColumnHandle(columnName, varcharType, dispatcherProxiedConnectorTransformer);
        assertExpressionNotConverted(columnName, columnHandle, Optional.of(VaradaPrimitiveConstant.TRUE), Optional.of(Domain.all(varcharType)));
    }

    @Test
    public void testOnlyIsNullVaradaExpression()
    {
        String columnName = "col1";
        ColumnHandle columnHandle = mockColumnHandle(columnName, varcharType, dispatcherProxiedConnectorTransformer);

        BooleanQuery emptyQuery = new BooleanQuery.Builder().build();

        assertExpressionConversion(columnName, columnHandle, Optional.of(VaradaPrimitiveConstant.FALSE), Optional.empty(), emptyQuery, true, true);
    }

    @Test
    public void testPreferBasicIndex()
    {
        String columnName = "col1";
        VaradaColumn varadaColumn = new RegularColumn(columnName);
        ColumnHandle columnHandle = mockColumnHandle(columnName, varcharType, dispatcherProxiedConnectorTransformer);
        Type type = VarcharType.createVarcharType(5);
        Domain domain = Domain.singleValue(type, Slices.utf8Slice("AAA"));
        Map<ColumnHandle, Domain> columnDomains = Map.of(columnHandle, domain);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(columnDomains);

        when(dispatcherTableHandle.getFullPredicate()).thenReturn(tupleDomain);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        Map<VaradaColumn, PredicateContext> remainingPredicateContext = predicateContextData.getLeaves()
                .entrySet().stream().collect(Collectors.toMap(x -> x.getValue().getVaradaColumn(), Map.Entry::getValue));
        WarmedWarmupTypes.Builder builder = new WarmedWarmupTypes.Builder();
        builder.add(createWarmUpElementFromColumnHandle(columnHandle, WarmUpType.WARM_UP_TYPE_LUCENE));
        builder.add(createWarmUpElementFromColumnHandle(columnHandle, WarmUpType.WARM_UP_TYPE_BASIC));

        // PredicateContextData predicateContextData = new PredicateContextData(ImmutableMap.copyOf(remainingPredicateContext));
        ClassifyArgs classifyArgs = new ClassifyArgs(dispatcherTableHandle,
                rowGroupData,
                mock(PredicateContextData.class),
                ImmutableMap.of(),
                builder.build(),
                false,
                true,
                false);

        MatchContext matchContext = new MatchContext(Collections.emptyList(), remainingPredicateContext, true);
        MatchContext result = luceneElementsMatcher.match(classifyArgs, matchContext);

        assertThat(result.matchDataList()).isEmpty();
        assertThat(result.remainingPredicateContext().get(varadaColumn).getDomain()).isEqualTo(domain);
    }

    private void assertExpressionConversion(String columnName,
            ColumnHandle columnHandle,
            Optional<VaradaExpression> expression,
            Optional<Domain> domain,
            BooleanQuery expectedQuery,
            boolean expressionWithCollectNulls,
            boolean nullAllowed)
    {
        System.out.println(columnName + ": " + expression + "   " + expressionWithCollectNulls + " " + domain + " " + expectedQuery + " " + domain + " " + nullAllowed + columnHandle);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.all();
        if (domain.isPresent()) {
            Map<ColumnHandle, Domain> columnDomains = Map.of(columnHandle, domain.orElseThrow());
            tupleDomain = TupleDomain.withColumnDomains(columnDomains);
        }
        VaradaExpressionData varadaExpressionData = null;
        if (expression.isPresent()) {
            RegularColumn regularColumn = new RegularColumn(columnName);
            varadaExpressionData = new VaradaExpressionData(expression.orElseThrow(), varcharType, expressionWithCollectNulls, Optional.empty(), regularColumn);
        }

        MatchContext result = executeMatch(columnHandle, tupleDomain, varadaExpressionData);

        assertExpressionConversion(result, expectedQuery, nullAllowed);
    }

    private void assertExpressionConversion(MatchContext result, BooleanQuery expectedQuery, boolean expectedNullAllowed)
    {
        assertThat(result.matchDataList().size()).isEqualTo(1);
        assertThat(result.matchDataList().get(0)).isInstanceOf(QueryMatchData.class);
        QueryMatchData queryMatchData = result.matchDataList().get(0);
        assertThat(queryMatchData).isInstanceOf(LuceneQueryMatchData.class);
        LuceneQueryMatchData luceneQueryMatchData = (LuceneQueryMatchData) queryMatchData;
        assertThat(luceneQueryMatchData.getQuery()).isEqualTo(expectedQuery);
        assertThat(luceneQueryMatchData.isCollectNulls()).isEqualTo(expectedNullAllowed);
        assertTrue(result.remainingPredicateContext().isEmpty());
    }

    private void assertExpressionNotConverted(String columnName,
            ColumnHandle columnHandle,
            Optional<VaradaExpression> expression,
            Optional<Domain> domain)
    {
        RegularColumn regularColumn = new RegularColumn(columnName);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.all();
        if (domain.isPresent()) {
            Map<ColumnHandle, Domain> columnDomains = Map.of(columnHandle, domain.orElseThrow());
            tupleDomain = TupleDomain.withColumnDomains(columnDomains);
        }
        VaradaExpressionData varadaExpressionData = null;
        if (expression.isPresent()) {
            varadaExpressionData = new VaradaExpressionData(expression.orElseThrow(), varcharType, false, Optional.empty(), regularColumn);
        }

        MatchContext result = executeMatch(columnHandle, tupleDomain, varadaExpressionData);

        assertThat(result.matchDataList()).isEmpty();
        if (domain.isEmpty() && expression.isEmpty()) {
            assertThat(result.remainingPredicateContext()).isEmpty();
        }
        else if (domain.isPresent() && expression.isPresent()) {
            assertThat(result.remainingPredicateContext().get(regularColumn).getDomain()).isEqualTo(domain.orElseThrow());
            assertThat(result.remainingPredicateContext().get(regularColumn).getExpression()).isEqualTo(expression.orElseThrow());
        }
        else if (domain.isPresent()) {
            assertThat(result.remainingPredicateContext().get(regularColumn).getDomain()).isEqualTo(domain.orElseThrow());
            assertThat(result.remainingPredicateContext().get(regularColumn).getExpression()).isNull();
        }
        else {
            assertThat(result.remainingPredicateContext().get(regularColumn).getDomain()).isEqualTo(Domain.all(varcharType));
            assertThat(result.remainingPredicateContext().get(regularColumn).getExpression()).isEqualTo(expression.orElseThrow());
        }
    }

    private MatchContext executeMatch(ColumnHandle columnHandle,
            TupleDomain<ColumnHandle> tupleDomain,
            VaradaExpressionData varadaExpressionData)
    {
        WarmedWarmupTypes warmUpElementByType = createColumnToWarmUpElementByType(List.of(columnHandle), WarmUpType.WARM_UP_TYPE_LUCENE);
        Optional<WarpExpression> warpExpression = Optional.empty();
        if (varadaExpressionData != null) {
            warpExpression = Optional.of(new WarpExpression(varadaExpressionData.getExpression(), List.of(varadaExpressionData)));
        }
        when(dispatcherTableHandle.getWarpExpression()).thenReturn(warpExpression);
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(tupleDomain);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        Map<VaradaColumn, PredicateContext> remainingPredicateContext = predicateContextData.getLeaves()
                .entrySet().stream().collect(Collectors.toMap(x -> x.getValue().getVaradaColumn(), Map.Entry::getValue));
        ClassifyArgs classifyArgs = new ClassifyArgs(dispatcherTableHandle,
                rowGroupData,
                mock(PredicateContextData.class),
                ImmutableMap.of(),
                warmUpElementByType,
                false,
                true,
                false);
        MatchContext matchContext = new MatchContext(Collections.emptyList(), remainingPredicateContext, true);
        return luceneElementsMatcher.match(classifyArgs, matchContext);
    }
}
