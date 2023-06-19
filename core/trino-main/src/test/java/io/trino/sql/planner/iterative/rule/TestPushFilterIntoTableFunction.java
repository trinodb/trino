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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableFunctionProcessor;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;

@ResourceLock("TestPushFilterIntoTableFunction")
public class TestPushFilterIntoTableFunction
        extends BaseRuleTest
{
    private static final String MOCK_CATALOG = "mock_catalog";
    private static final ConnectorTableFunctionHandle TABLE_FUNCTION_CONSUMES_ENTIRE_PREDICATE = new ConnectorTableFunctionHandle() {};

    public static final int PUSHDOWN_COLUMN = 1;
    private static final ConnectorTableFunctionHandle TABLE_FUNCTION_CONSUMES_PREDICATE_PARTIALLY = new ConnectorTableFunctionHandle() {};

    private static final ConnectorTableFunctionHandle RESULT_TABLE_FUNCTION_HANDLE = new ConnectorTableFunctionHandle() {};
    private PushFilterIntoTableFunction pushFilterIntoTableFunction;
    private CatalogHandle catalogHandle;
    private CatalogHandle mockCatalogHandle;

    @BeforeAll
    public void init()
    {
        pushFilterIntoTableFunction = new PushFilterIntoTableFunction(tester().getPlannerContext(), createTestingTypeAnalyzer(tester().getPlannerContext()));

        catalogHandle = tester().getCurrentCatalogHandle();
        MockConnectorFactory mockConnectorFactory = MockConnectorFactory.builder()
                .withApplyFilterForPtf((session, tableFunctionHandle, constraint) -> {
                    if (tableFunctionHandle.equals(TABLE_FUNCTION_CONSUMES_ENTIRE_PREDICATE)) {
                        return Optional.of(new ConstraintApplicationResult<>(RESULT_TABLE_FUNCTION_HANDLE, TupleDomain.all(), false));
                    }
                    if (tableFunctionHandle.equals(TABLE_FUNCTION_CONSUMES_PREDICATE_PARTIALLY)) {
                        return Optional.of(new ConstraintApplicationResult<>(
                                RESULT_TABLE_FUNCTION_HANDLE,
                                TupleDomain.fromFixedValues(ImmutableMap.of(PUSHDOWN_COLUMN, NullableValue.of(BIGINT, (long) 1))),
                                false));
                    }
                    return Optional.empty();
                })
                .build();
        tester().getQueryRunner().createCatalog(MOCK_CATALOG, mockConnectorFactory, ImmutableMap.of());
        mockCatalogHandle = tester().getQueryRunner().getCatalogHandle(MOCK_CATALOG);
    }

    @Test
    public void testDoesNotFireIfNoTableFunctionProcessor()
    {
        tester().assertThat(pushFilterIntoTableFunction)
                .on(p -> p.values(p.symbol("a", BIGINT)))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenApplyFilterReturnsEmptyResult()
    {
        tester().assertThat(pushFilterIntoTableFunction)
                .on(p -> p.filter(
                        expression("p = BIGINT '44' "),
                        p.tableFunctionProcessor(
                                builder -> builder
                                        .name("test_function")
                                        .properOutputs(p.symbol("p"))
                                        .source(p.values(p.symbol("x")))
                                        .catalogHandle(catalogHandle))))
                .doesNotFire();
    }

    @Test
    public void testRemovesPredicateWhenFunctionConsumesIt()
    {
        Session session = Session.builder(tester().getSession())
                .setCatalog(MOCK_CATALOG)
                .build();
        tester().assertThat(pushFilterIntoTableFunction)
                .withSession(session)
                .on(p -> p.filter(
                        expression("p = BIGINT '44' "),
                        p.tableFunctionProcessor(
                                builder -> builder
                                        .name("test_function")
                                        .properOutputs(p.symbol("p"))
                                        .source(p.values(p.symbol("x")))
                                        .catalogHandle(mockCatalogHandle)
                                        .connectorTableFunctionHandle(TABLE_FUNCTION_CONSUMES_ENTIRE_PREDICATE))))
                .matches(tableFunctionProcessor(
                        builder -> builder
                                .name("test_function")
                                .properOutputs(ImmutableList.of("p")),
                        values("x")));
    }

    @Test
    public void testNarrowsPredicateWhenFunctionPartiallyConsumesIt()
    {
        Session session = Session.builder(tester().getSession())
                .setCatalog(MOCK_CATALOG)
                .build();
        tester().assertThat(pushFilterIntoTableFunction)
                .withSession(session)
                .on(p -> p.filter(
                        expression("p = BIGINT '44' AND z = BIGINT '1'"),
                        p.tableFunctionProcessor(
                                builder -> builder
                                        .name("test_function")
                                        .properOutputs(p.symbol("p"), p.symbol("z"))
                                        .source(p.values(p.symbol("x")))
                                        .catalogHandle(mockCatalogHandle)
                                        .connectorTableFunctionHandle(TABLE_FUNCTION_CONSUMES_PREDICATE_PARTIALLY))))
                .matches(filter("z = BIGINT '1'", tableFunctionProcessor(
                        builder -> builder
                                .name("test_function")
                                .properOutputs(ImmutableList.of("p", "z")),
                        values("x"))));
    }
}
