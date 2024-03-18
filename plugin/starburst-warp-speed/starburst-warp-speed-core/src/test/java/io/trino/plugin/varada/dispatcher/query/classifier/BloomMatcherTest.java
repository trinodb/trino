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
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.query.PredicateContext;
import io.trino.plugin.varada.dispatcher.query.data.match.BasicBloomQueryMatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.MatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.type.ColorType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BloomMatcherTest
        extends ClassifierTest
{
    private BloomMatcher bloomMatcher;
    private RowGroupData rowGroupData;

    static Stream<Arguments> allBloomWarmUpTypesParams()
    {
        return Stream.of(
                arguments(WarmUpType.WARM_UP_TYPE_BLOOM_LOW),
                arguments(WarmUpType.WARM_UP_TYPE_BLOOM_MEDIUM),
                arguments(WarmUpType.WARM_UP_TYPE_BLOOM_HIGH));
    }

    @BeforeEach
    public void before()
    {
        init();
        bloomMatcher = new BloomMatcher();
        rowGroupData = mock(RowGroupData.class);
    }

    @ParameterizedTest
    @MethodSource("allBloomWarmUpTypesParams")
    public void testMatchOnAValidDomain(WarmUpType warmUpType)
    {
        String columnName = "col1";
        VaradaColumn varadaColumn = new RegularColumn(columnName);
        ColumnHandle columnHandle = mockColumnHandle(columnName, IntegerType.INTEGER, dispatcherProxiedConnectorTransformer);
        Domain domain = Domain.singleValue(IntegerType.INTEGER, 7L);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(Map.of(columnHandle, domain));

        MatchContext result = executeMatch(columnHandle, warmUpType, tupleDomain);

        assertThat(result.matchDataList().size()).isEqualTo(1);
        assertThat(result.matchDataList().get(0)).isInstanceOf(QueryMatchData.class);
        MatchData queryMatchData = result.matchDataList().get(0);
        assertThat(queryMatchData).isInstanceOf(BasicBloomQueryMatchData.class);
        BasicBloomQueryMatchData basicBloomQueryMatchData = (BasicBloomQueryMatchData) queryMatchData;
        assertThat(basicBloomQueryMatchData.getType()).isEqualTo(IntegerType.INTEGER);
        assertThat(basicBloomQueryMatchData.getVaradaColumn()).isEqualTo(varadaColumn);
        assertThat(basicBloomQueryMatchData.getWarmUpElement().getWarmUpType()).isEqualTo(warmUpType);
        assertThat(result.remainingPredicateContext().get(varadaColumn).getDomain()).isEqualTo(domain);
    }

    @Test
    public void testPreferHighestBloom()
    {
        String columnName = "col1";
        VaradaColumn varadaColumn = new RegularColumn(columnName);
        ColumnHandle columnHandle = mockColumnHandle(columnName, IntegerType.INTEGER, dispatcherProxiedConnectorTransformer);
        Domain domain = Domain.singleValue(IntegerType.INTEGER, 7L);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(Map.of(columnHandle, domain));

        WarmedWarmupTypes.Builder builder = new WarmedWarmupTypes.Builder();
        builder.add(createWarmUpElementFromColumnHandle(columnHandle, WarmUpType.WARM_UP_TYPE_BLOOM_LOW));
        builder.add(createWarmUpElementFromColumnHandle(columnHandle, WarmUpType.WARM_UP_TYPE_BLOOM_MEDIUM));
        builder.add(createWarmUpElementFromColumnHandle(columnHandle, WarmUpType.WARM_UP_TYPE_BLOOM_HIGH));
        MatchContext result = executeMatch(tupleDomain, builder.build());

        assertThat(result.matchDataList().size()).isEqualTo(1);
        assertThat(result.matchDataList().get(0)).isInstanceOf(QueryMatchData.class);
        QueryMatchData queryMatchData = result.matchDataList().get(0);
        assertThat(queryMatchData).isInstanceOf(BasicBloomQueryMatchData.class);
        BasicBloomQueryMatchData basicBloomQueryMatchData = (BasicBloomQueryMatchData) queryMatchData;
        assertThat(basicBloomQueryMatchData.getType()).isEqualTo(IntegerType.INTEGER);
        assertThat(basicBloomQueryMatchData.getVaradaColumn()).isEqualTo(varadaColumn);
        assertThat(basicBloomQueryMatchData.getWarmUpElement().getWarmUpType()).isEqualTo(WarmUpType.WARM_UP_TYPE_BLOOM_HIGH);
        assertThat(result.remainingPredicateContext().get(varadaColumn).getDomain()).isEqualTo(domain); // In bloom, we always remain the tupleDomain
    }

    @Test
    public void testColumnWithoutBloomIndex()
    {
        String columnName = "col1";
        VaradaColumn varadaColumn = new RegularColumn(columnName);
        ColumnHandle columnHandle = mockColumnHandle(columnName, IntegerType.INTEGER, dispatcherProxiedConnectorTransformer);
        Domain domain = Domain.singleValue(IntegerType.INTEGER, 7L);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(Map.of(columnHandle, domain));
        WarmedWarmupTypes.Builder builder = new WarmedWarmupTypes.Builder();

        MatchContext result = executeMatch(tupleDomain, builder.build());

        assertThat(result.matchDataList()).isEmpty();
        assertThat(result.remainingPredicateContext().get(varadaColumn).getDomain()).isEqualTo(domain);
    }

    @Disabled("ColorType.COLOR is not supported in general, need to provide a valid nonOrderableType(Map, Array) ")
    @ParameterizedTest
    @MethodSource("allBloomWarmUpTypesParams")
    public void testDomainCantBeMatched(WarmUpType warmUpType)
    {
        String columnName = "col1";
        VaradaColumn varadaColumn = new RegularColumn(columnName);
        Type nonOrderableType = ColorType.COLOR;
        ColumnHandle columnHandle = mockColumnHandle(columnName, nonOrderableType, dispatcherProxiedConnectorTransformer);
        Domain domain = Domain.singleValue(nonOrderableType, 0L);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(Map.of(columnHandle, domain));

        MatchContext result = executeMatch(columnHandle, warmUpType, tupleDomain);

        assertThat(result.matchDataList()).isEmpty();
        assertThat(result.remainingPredicateContext().get(varadaColumn).getDomain()).isEqualTo(domain);
    }

    private MatchContext executeMatch(ColumnHandle columnHandle, WarmUpType warmUpType, TupleDomain<ColumnHandle> tupleDomain)
    {
        WarmedWarmupTypes.Builder builder = new WarmedWarmupTypes.Builder();
        builder.add(createWarmUpElementFromColumnHandle(columnHandle, warmUpType));
        return executeMatch(tupleDomain, builder.build());
    }

    private MatchContext executeMatch(TupleDomain<ColumnHandle> tupleDomain, WarmedWarmupTypes warmedWarmupTypes)
    {
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(tupleDomain);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        Map<VaradaColumn, PredicateContext> remainingPredicateContext = predicateContextData.getLeaves()
                .entrySet().stream().collect(Collectors.toMap(x -> x.getValue().getVaradaColumn(), Map.Entry::getValue));
        ClassifyArgs classifyArgs = new ClassifyArgs(dispatcherTableHandle,
                rowGroupData,
                mock(PredicateContextData.class),
                ImmutableMap.of(),
                warmedWarmupTypes,
                false,
                true,
                false);

        MatchContext matchContext = new MatchContext(Collections.emptyList(), remainingPredicateContext, true);

        return bloomMatcher.match(classifyArgs, matchContext);
    }
}
