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
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.sql.ir.Constant;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.TableScanNode;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestSimplifyCountOverNonNull
{
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";
    private static final Session MOCK_SESSION = testSessionBuilder().setCatalog(TEST_CATALOG_NAME).setSchema(TEST_SCHEMA).build();

    private static final ColumnMetadata REQUIRED_COLUMN = ColumnMetadata.builder()
            .setName("required_column")
            .setType(BIGINT)
            .setNullable(false)
            .build();
    private static final ColumnMetadata OPTIONAL_COLUMN = ColumnMetadata.builder()
            .setName("optional_column")
            .setType(BIGINT)
            .setNullable(true)
            .build();

    @Test
    public void testRewritesCountOverRequiredColumn()
    {
        try (RuleTester ruleTester = ruleTester()) {
            ruleTester.assertThat(new SimplifyCountOverNonNull(ruleTester.getPlannerContext()))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol required = p.symbol("required_column", BIGINT);
                        return p.aggregation(aggregation -> aggregation
                                .globalGrouping()
                                .addAggregation(p.symbol("count", BIGINT), PlanBuilder.aggregation("count", ImmutableList.of(required.toSymbolReference())), ImmutableList.of(BIGINT))
                                .source(p.tableScan(
                                        ruleTester.getCurrentCatalogTableHandle(TEST_SCHEMA, TEST_TABLE),
                                        ImmutableList.of(required),
                                        ImmutableMap.of(required, new MockConnectorColumnHandle("required_column", BIGINT)))));
                    })
                    .matches(aggregation(
                            ImmutableMap.of("count", aggregationFunction("count", ImmutableList.of())),
                            node(TableScanNode.class)));
        }
    }

    @Test
    public void testDoesNotFireOnOptionalColumn()
    {
        try (RuleTester ruleTester = ruleTester()) {
            ruleTester.assertThat(new SimplifyCountOverNonNull(ruleTester.getPlannerContext()))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol optional = p.symbol("optional_column", BIGINT);
                        return p.aggregation(aggregation -> aggregation
                                .globalGrouping()
                                .addAggregation(p.symbol("count", BIGINT), PlanBuilder.aggregation("count", ImmutableList.of(optional.toSymbolReference())), ImmutableList.of(BIGINT))
                                .source(p.tableScan(
                                        ruleTester.getCurrentCatalogTableHandle(TEST_SCHEMA, TEST_TABLE),
                                        ImmutableList.of(optional),
                                        ImmutableMap.of(optional, new MockConnectorColumnHandle("optional_column", BIGINT)))));
                    })
                    .doesNotFire();
        }
    }

    @Test
    public void testDoesNotFireOnCountDistinct()
    {
        try (RuleTester ruleTester = ruleTester()) {
            ruleTester.assertThat(new SimplifyCountOverNonNull(ruleTester.getPlannerContext()))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol required = p.symbol("required_column", BIGINT);
                        return p.aggregation(aggregation -> aggregation
                                .globalGrouping()
                                .addAggregation(p.symbol("count", BIGINT), PlanBuilder.aggregation("count", true, ImmutableList.of(required.toSymbolReference())), ImmutableList.of(BIGINT))
                                .source(p.tableScan(
                                        ruleTester.getCurrentCatalogTableHandle(TEST_SCHEMA, TEST_TABLE),
                                        ImmutableList.of(required),
                                        ImmutableMap.of(required, new MockConnectorColumnHandle("required_column", BIGINT)))));
                    })
                    .doesNotFire();
        }
    }

    @Test
    public void testRewritesCountOverFilteredSymbol()
    {
        try (RuleTester ruleTester = ruleTester()) {
            ruleTester.assertThat(new SimplifyCountOverNonNull(ruleTester.getPlannerContext()))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol value = p.symbol("value", BIGINT);
                        return p.aggregation(aggregation -> aggregation
                                .globalGrouping()
                                .addAggregation(p.symbol("count", BIGINT), PlanBuilder.aggregation("count", ImmutableList.of(value.toSymbolReference())), ImmutableList.of(BIGINT))
                                .source(p.filter(
                                        comparison(GREATER_THAN, value.toSymbolReference(), new Constant(BIGINT, 5L)),
                                        p.values(1, value))));
                    })
                    .matches(aggregation(
                            ImmutableMap.of("count", aggregationFunction("count", ImmutableList.of())),
                            node(FilterNode.class, values("value"))));
        }
    }

    private static RuleTester ruleTester()
    {
        MockConnectorFactory connectorFactory = MockConnectorFactory.builder()
                .withListSchemaNames(_ -> ImmutableList.of(TEST_SCHEMA))
                .withListTables((_, schema) -> TEST_SCHEMA.equals(schema) ? ImmutableList.of(TEST_TABLE) : ImmutableList.of())
                .withGetColumns(_ -> ImmutableList.of(REQUIRED_COLUMN, OPTIONAL_COLUMN))
                .build();
        return RuleTester.builder().withDefaultCatalogConnectorFactory(connectorFactory).build();
    }
}
