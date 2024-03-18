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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.connector.TestingConnectorColumnHandle;
import io.trino.plugin.varada.connector.TestingConnectorProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.SimplifiedColumns;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.expression.DomainExpression;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.expression.VaradaExpressionData;
import io.trino.plugin.varada.expression.VaradaSliceConstant;
import io.trino.plugin.varada.expression.VaradaVariable;
import io.trino.plugin.varada.expression.rewrite.WarpExpression;
import io.trino.plugin.varada.util.DefaultFakeConnectorSession;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PredicateContextFactoryTest
{
    private PredicateContextFactory predicateContextFactory;

    @BeforeEach
    public void before()
    {
        DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer = new TestingConnectorProxiedConnectorTransformer();
        GlobalConfiguration globalConfiguration = new GlobalConfiguration();
        predicateContextFactory = new PredicateContextFactory(globalConfiguration, dispatcherProxiedConnectorTransformer);
    }

    /**
     * Domain (a, b) -> AND (a, b)
     */
    @Test
    public void test2ColumnsOnDomain()
    {
        ConnectorSession session = DefaultFakeConnectorSession.INSTANCE;
        DynamicFilter dynamicFilter = DynamicFilter.EMPTY;
        DispatcherTableHandle dispatcherTableHandle = mock(DispatcherTableHandle.class);
        when(dispatcherTableHandle.getSimplifiedColumns()).thenReturn(new SimplifiedColumns(Set.of()));
        Domain domain = Domain.multipleValues(IntegerType.INTEGER, List.of(1L, 2L));
        TestingConnectorColumnHandle c1 = new TestingConnectorColumnHandle(IntegerType.INTEGER, "a");
        TestingConnectorColumnHandle c2 = new TestingConnectorColumnHandle(IntegerType.INTEGER, "b");
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(c1, domain, c2, domain));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, dynamicFilter, dispatcherTableHandle);
        List<VaradaExpression> expectedChildren = List.of(
                convertDomainToDomainPredicateData(c1, domain),
                convertDomainToDomainPredicateData(c2, domain));
        VaradaCall expectedExpression = new VaradaCall(AND_FUNCTION_NAME.getName(), expectedChildren, BOOLEAN);
        assertThat(((VaradaCall) predicateContextData.getRootExpression()).getFunctionName()).isEqualTo(expectedExpression.getFunctionName());
        assertThat(((VaradaCall) predicateContextData.getRootExpression()).getArguments()).containsExactlyInAnyOrderElementsOf(expectedExpression.getArguments());
    }

    /**
     * Domain (a) -> a
     */
    @Test
    public void testSingleColumnOnDomain()
    {
        ConnectorSession session = DefaultFakeConnectorSession.INSTANCE;
        DynamicFilter dynamicFilter = DynamicFilter.EMPTY;
        DispatcherTableHandle dispatcherTableHandle = mock(DispatcherTableHandle.class);
        when(dispatcherTableHandle.getSimplifiedColumns()).thenReturn(new SimplifiedColumns(Set.of()));
        Domain domain = Domain.multipleValues(IntegerType.INTEGER, List.of(1L, 2L));
        TestingConnectorColumnHandle c1 = new TestingConnectorColumnHandle(IntegerType.INTEGER, "a");
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(c1, domain));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, dynamicFilter, dispatcherTableHandle);
        assertThat(predicateContextData.getRootExpression()).isEqualTo(convertDomainToDomainPredicateData(c1, domain));
    }

    /**
     * expression (a or b) ->  OR (a, b)
     */
    @Test
    public void orExpression()
    {
        ConnectorSession session = DefaultFakeConnectorSession.INSTANCE;
        DynamicFilter dynamicFilter = DynamicFilter.EMPTY;
        DispatcherTableHandle dispatcherTableHandle = mock(DispatcherTableHandle.class);
        when(dispatcherTableHandle.getSimplifiedColumns()).thenReturn(new SimplifiedColumns(Set.of()));
        TestingConnectorColumnHandle c1 = new TestingConnectorColumnHandle(IntegerType.INTEGER, "a");
        TestingConnectorColumnHandle c2 = new TestingConnectorColumnHandle(IntegerType.INTEGER, "b");
        Slice likePattern = Slices.utf8Slice("str%");
        VaradaCall expA = createLikeVaradaExpression(c1, likePattern);
        VaradaCall expB = createLikeVaradaExpression(c2, likePattern);
        VaradaExpression expression = new VaradaCall(OR_FUNCTION_NAME.getName(), List.of(expA, expB), BOOLEAN);

        WarpExpression warpExpression = new WarpExpression(expression,
                List.of(convertToVaradaExpressionData(expA),
                        convertToVaradaExpressionData(expB)));
        when(dispatcherTableHandle.getWarpExpression()).thenReturn(Optional.of(warpExpression));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(TupleDomain.all());
        PredicateContextData result = predicateContextFactory.create(session, dynamicFilter, dispatcherTableHandle);
        assertThat(result.getRootExpression()).isEqualTo(expression);
    }

    /**
     * expression (a and b) ->  AND (a, b)
     */
    @Test
    public void andExpression()
    {
        ConnectorSession session = DefaultFakeConnectorSession.INSTANCE;
        DynamicFilter dynamicFilter = DynamicFilter.EMPTY;
        DispatcherTableHandle dispatcherTableHandle = mock(DispatcherTableHandle.class);
        when(dispatcherTableHandle.getSimplifiedColumns()).thenReturn(new SimplifiedColumns(Set.of()));
        TestingConnectorColumnHandle c1 = new TestingConnectorColumnHandle(IntegerType.INTEGER, "a");
        TestingConnectorColumnHandle c2 = new TestingConnectorColumnHandle(IntegerType.INTEGER, "b");
        Slice likePattern = Slices.utf8Slice("str%");
        VaradaCall expA = createLikeVaradaExpression(c1, likePattern);
        VaradaCall expB = createLikeVaradaExpression(c2, likePattern);
        VaradaExpression expression = new VaradaCall(AND_FUNCTION_NAME.getName(), List.of(expA, expB), BOOLEAN);

        WarpExpression warpExpression = new WarpExpression(expression,
                List.of(convertToVaradaExpressionData(expA),
                        convertToVaradaExpressionData(expB)));
        when(dispatcherTableHandle.getWarpExpression()).thenReturn(Optional.of(warpExpression));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(TupleDomain.all());
        PredicateContextData result = predicateContextFactory.create(session, dynamicFilter, dispatcherTableHandle);
        assertThat(result.getRootExpression()).isEqualTo(expression);
    }

    /**
     * domain(x) expression (a and b) ->  AND (a, b, x)
     */
    @Test
    public void singleDomainWithAndExpression()
    {
        ConnectorSession session = DefaultFakeConnectorSession.INSTANCE;
        DynamicFilter dynamicFilter = DynamicFilter.EMPTY;
        DispatcherTableHandle dispatcherTableHandle = mock(DispatcherTableHandle.class);
        when(dispatcherTableHandle.getSimplifiedColumns()).thenReturn(new SimplifiedColumns(Set.of()));
        TestingConnectorColumnHandle c1 = new TestingConnectorColumnHandle(IntegerType.INTEGER, "a");
        TestingConnectorColumnHandle c2 = new TestingConnectorColumnHandle(IntegerType.INTEGER, "b");
        Domain domain = Domain.multipleValues(IntegerType.INTEGER, List.of(1L, 2L));
        TestingConnectorColumnHandle x = new TestingConnectorColumnHandle(IntegerType.INTEGER, "x");
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(x, domain));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        Slice likePattern = Slices.utf8Slice("str%");
        VaradaCall expA = createLikeVaradaExpression(c1, likePattern);
        VaradaCall expB = createLikeVaradaExpression(c2, likePattern);
        VaradaExpression expression = new VaradaCall(AND_FUNCTION_NAME.getName(), List.of(expA, expB), BOOLEAN);

        WarpExpression warpExpression = new WarpExpression(expression,
                List.of(convertToVaradaExpressionData(expA),
                        convertToVaradaExpressionData(expB)));
        when(dispatcherTableHandle.getWarpExpression()).thenReturn(Optional.of(warpExpression));
        PredicateContextData result = predicateContextFactory.create(session, dynamicFilter, dispatcherTableHandle);
        VaradaCall expectedExpression = new VaradaCall(AND_FUNCTION_NAME.getName(), List.of(expA, expB, convertDomainToDomainPredicateData(x, domain)), BOOLEAN);
        assertThat(((VaradaCall) result.getRootExpression()).getFunctionName()).isEqualTo(expectedExpression.getFunctionName());
        assertThat(((VaradaCall) result.getRootExpression()).getArguments()).containsExactlyInAnyOrderElementsOf(expectedExpression.getArguments());
    }

    /**
     * domain(x) expression (a or b) ->  AND (x, OR (a, b))
     */
    @Test
    public void singleDomainWithOrExpression()
    {
        ConnectorSession session = DefaultFakeConnectorSession.INSTANCE;
        DynamicFilter dynamicFilter = DynamicFilter.EMPTY;
        DispatcherTableHandle dispatcherTableHandle = mock(DispatcherTableHandle.class);
        when(dispatcherTableHandle.getSimplifiedColumns()).thenReturn(new SimplifiedColumns(Set.of()));
        TestingConnectorColumnHandle c1 = new TestingConnectorColumnHandle(IntegerType.INTEGER, "a");
        TestingConnectorColumnHandle c2 = new TestingConnectorColumnHandle(IntegerType.INTEGER, "b");
        Domain domain = Domain.multipleValues(IntegerType.INTEGER, List.of(1L, 2L));
        TestingConnectorColumnHandle x = new TestingConnectorColumnHandle(IntegerType.INTEGER, "x");
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(x, domain));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        Slice likePattern = Slices.utf8Slice("str%");
        VaradaCall expA = createLikeVaradaExpression(c1, likePattern);
        VaradaCall expB = createLikeVaradaExpression(c2, likePattern);
        VaradaExpression expression = new VaradaCall(OR_FUNCTION_NAME.getName(), List.of(expA, expB), BOOLEAN);

        WarpExpression warpExpression = new WarpExpression(expression,
                List.of(convertToVaradaExpressionData(expA),
                        convertToVaradaExpressionData(expB)));
        when(dispatcherTableHandle.getWarpExpression()).thenReturn(Optional.of(warpExpression));
        PredicateContextData result = predicateContextFactory.create(session, dynamicFilter, dispatcherTableHandle);
        VaradaCall expectedExpression = new VaradaCall(AND_FUNCTION_NAME.getName(), List.of(expression, convertDomainToDomainPredicateData(x, domain)), BOOLEAN);
        assertThat(((VaradaCall) result.getRootExpression()).getFunctionName()).isEqualTo(expectedExpression.getFunctionName());
        assertThat(((VaradaCall) result.getRootExpression()).getArguments()).containsExactlyInAnyOrderElementsOf(expectedExpression.getArguments());
    }

    /**
     * domain(x) expression (a) ->  AND (x, a))
     */
    @Test
    public void domainWithSingleExpression()
    {
        ConnectorSession session = DefaultFakeConnectorSession.INSTANCE;
        DynamicFilter dynamicFilter = DynamicFilter.EMPTY;
        DispatcherTableHandle dispatcherTableHandle = mock(DispatcherTableHandle.class);
        when(dispatcherTableHandle.getSimplifiedColumns()).thenReturn(new SimplifiedColumns(Set.of()));
        TestingConnectorColumnHandle a = new TestingConnectorColumnHandle(IntegerType.INTEGER, "a");
        Domain domain = Domain.multipleValues(IntegerType.INTEGER, List.of(1L, 2L));
        TestingConnectorColumnHandle x = new TestingConnectorColumnHandle(IntegerType.INTEGER, "x");
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(x, domain));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        Slice likePattern = Slices.utf8Slice("str%");
        VaradaCall expA = createLikeVaradaExpression(a, likePattern);

        WarpExpression warpExpression = new WarpExpression(expA, List.of(convertToVaradaExpressionData(expA)));
        when(dispatcherTableHandle.getWarpExpression()).thenReturn(Optional.of(warpExpression));
        PredicateContextData result = predicateContextFactory.create(session, dynamicFilter, dispatcherTableHandle);
        VaradaCall expectedExpression = new VaradaCall(AND_FUNCTION_NAME.getName(), List.of(expA,
                convertDomainToDomainPredicateData(x, domain)),
                BOOLEAN);
        assertThat(((VaradaCall) result.getRootExpression()).getFunctionName()).isEqualTo(expectedExpression.getFunctionName());
        assertThat(((VaradaCall) result.getRootExpression()).getArguments()).containsExactlyInAnyOrderElementsOf(expectedExpression.getArguments());
    }

    /**
     * domain(x, y) expression (a) ->  AND (x, y, a))
     */
    @Test
    public void domainsWithSingleExpression()
    {
        ConnectorSession session = DefaultFakeConnectorSession.INSTANCE;
        DynamicFilter dynamicFilter = DynamicFilter.EMPTY;
        DispatcherTableHandle dispatcherTableHandle = mock(DispatcherTableHandle.class);
        when(dispatcherTableHandle.getSimplifiedColumns()).thenReturn(new SimplifiedColumns(Set.of()));
        TestingConnectorColumnHandle a = new TestingConnectorColumnHandle(IntegerType.INTEGER, "a");
        Domain domain = Domain.multipleValues(IntegerType.INTEGER, List.of(1L, 2L));
        TestingConnectorColumnHandle x = new TestingConnectorColumnHandle(IntegerType.INTEGER, "x");
        TestingConnectorColumnHandle y = new TestingConnectorColumnHandle(IntegerType.INTEGER, "y");
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(x, domain, y, domain));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        Slice likePattern = Slices.utf8Slice("str%");
        VaradaCall expA = createLikeVaradaExpression(a, likePattern);

        WarpExpression warpExpression = new WarpExpression(expA, List.of(convertToVaradaExpressionData(expA)));
        when(dispatcherTableHandle.getWarpExpression()).thenReturn(Optional.of(warpExpression));
        PredicateContextData result = predicateContextFactory.create(session, dynamicFilter, dispatcherTableHandle);
        VaradaCall expectedExpression = new VaradaCall(AND_FUNCTION_NAME.getName(), List.of(expA,
                convertDomainToDomainPredicateData(x, domain),
                convertDomainToDomainPredicateData(y, domain)),
                BOOLEAN);
        assertThat(((VaradaCall) result.getRootExpression()).getFunctionName()).isEqualTo(expectedExpression.getFunctionName());
        assertThat(((VaradaCall) result.getRootExpression()).getArguments()).containsExactlyInAnyOrderElementsOf(expectedExpression.getArguments());
    }

    /**
     * domain(x, y) expression (a or b) ->  AND (x, y,  OR (a, b))
     */
    @Test
    public void domainsWithOrExpression()
    {
        ConnectorSession session = DefaultFakeConnectorSession.INSTANCE;
        DynamicFilter dynamicFilter = DynamicFilter.EMPTY;
        DispatcherTableHandle dispatcherTableHandle = mock(DispatcherTableHandle.class);
        when(dispatcherTableHandle.getSimplifiedColumns()).thenReturn(new SimplifiedColumns(Set.of()));
        TestingConnectorColumnHandle a = new TestingConnectorColumnHandle(IntegerType.INTEGER, "a");
        TestingConnectorColumnHandle b = new TestingConnectorColumnHandle(IntegerType.INTEGER, "b");
        Domain domain = Domain.multipleValues(IntegerType.INTEGER, List.of(1L, 2L));
        TestingConnectorColumnHandle x = new TestingConnectorColumnHandle(IntegerType.INTEGER, "x");
        TestingConnectorColumnHandle y = new TestingConnectorColumnHandle(IntegerType.INTEGER, "y");
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(x, domain, y, domain));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        Slice likePattern = Slices.utf8Slice("str%");
        VaradaCall expA = createLikeVaradaExpression(a, likePattern);
        VaradaCall expB = createLikeVaradaExpression(b, likePattern);
        VaradaExpression expression = new VaradaCall(OR_FUNCTION_NAME.getName(), List.of(expA, expB), BOOLEAN);

        WarpExpression warpExpression = new WarpExpression(expression,
                List.of(convertToVaradaExpressionData(expA),
                        convertToVaradaExpressionData(expB)));
        when(dispatcherTableHandle.getWarpExpression()).thenReturn(Optional.of(warpExpression));
        PredicateContextData result = predicateContextFactory.create(session, dynamicFilter, dispatcherTableHandle);
        VaradaCall expectedExpression = new VaradaCall(AND_FUNCTION_NAME.getName(), List.of(expression,
                convertDomainToDomainPredicateData(x, domain),
                convertDomainToDomainPredicateData(y, domain)),
                BOOLEAN);
        assertThat(((VaradaCall) result.getRootExpression()).getFunctionName()).isEqualTo(expectedExpression.getFunctionName());
        assertThat(((VaradaCall) result.getRootExpression()).getArguments()).containsExactlyInAnyOrderElementsOf(expectedExpression.getArguments());
    }

    /**
     * domain(x, y) expression (a and b) ->  AND (x, y,  a, b)
     */
    @Test
    public void domainsWithAndExpression()
    {
        ConnectorSession session = DefaultFakeConnectorSession.INSTANCE;
        DynamicFilter dynamicFilter = DynamicFilter.EMPTY;
        DispatcherTableHandle dispatcherTableHandle = mock(DispatcherTableHandle.class);
        when(dispatcherTableHandle.getSimplifiedColumns()).thenReturn(new SimplifiedColumns(Set.of()));
        TestingConnectorColumnHandle a = new TestingConnectorColumnHandle(IntegerType.INTEGER, "a");
        TestingConnectorColumnHandle b = new TestingConnectorColumnHandle(IntegerType.INTEGER, "b");
        Domain domain = Domain.multipleValues(IntegerType.INTEGER, List.of(1L, 2L));
        TestingConnectorColumnHandle x = new TestingConnectorColumnHandle(IntegerType.INTEGER, "x");
        TestingConnectorColumnHandle y = new TestingConnectorColumnHandle(IntegerType.INTEGER, "y");
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(x, domain, y, domain));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        Slice likePattern = Slices.utf8Slice("str%");
        VaradaCall expA = createLikeVaradaExpression(a, likePattern);
        VaradaCall expB = createLikeVaradaExpression(b, likePattern);
        VaradaExpression expression = new VaradaCall(AND_FUNCTION_NAME.getName(), List.of(expA, expB), BOOLEAN);

        WarpExpression warpExpression = new WarpExpression(expression,
                List.of(convertToVaradaExpressionData(expA),
                        convertToVaradaExpressionData(expB)));
        when(dispatcherTableHandle.getWarpExpression()).thenReturn(Optional.of(warpExpression));
        PredicateContextData result = predicateContextFactory.create(session, dynamicFilter, dispatcherTableHandle);
        VaradaCall expectedExpression = new VaradaCall(AND_FUNCTION_NAME.getName(), List.of(expA,
                expB,
                convertDomainToDomainPredicateData(x, domain),
                convertDomainToDomainPredicateData(y, domain)),
                BOOLEAN);
        assertThat(((VaradaCall) result.getRootExpression()).getFunctionName()).isEqualTo(expectedExpression.getFunctionName());
        assertThat(((VaradaCall) result.getRootExpression()).getArguments()).containsExactlyInAnyOrderElementsOf(expectedExpression.getArguments());
    }

    private VaradaExpressionData convertToVaradaExpressionData(VaradaCall varadaCall)
    {
        VaradaExpressionData varadaExpressionData = mock(VaradaExpressionData.class);
        when(varadaExpressionData.getExpression()).thenReturn(varadaCall);
        VaradaVariable varadaVariable = (VaradaVariable) varadaCall
                .getArguments().stream().filter(x -> x instanceof VaradaVariable).findFirst().orElseThrow();
        String columnName = ((TestingConnectorColumnHandle) varadaVariable.getColumnHandle()).name();
        Type columnType = ((TestingConnectorColumnHandle) varadaVariable.getColumnHandle()).type();
        when(varadaExpressionData.getVaradaColumn()).thenReturn(new RegularColumn(columnName));
        when(varadaExpressionData.getColumnType()).thenReturn(columnType);
        return varadaExpressionData;
    }

    private DomainExpression convertDomainToDomainPredicateData(TestingConnectorColumnHandle column, Domain domain)
    {
        VaradaVariable varadaVariable = new VaradaVariable(column, domain.getType());
        return new DomainExpression(varadaVariable, domain);
    }

    private VaradaCall createLikeVaradaExpression(TestingConnectorColumnHandle columnHandle, Slice likePattern)
    {
        Type variableType = columnHandle.type();

        return new VaradaCall(LIKE_FUNCTION_NAME.getName(),
                List.of(new VaradaVariable(columnHandle, variableType),
                        new VaradaSliceConstant(likePattern, VarcharType.VARCHAR)), BOOLEAN);
    }
}
