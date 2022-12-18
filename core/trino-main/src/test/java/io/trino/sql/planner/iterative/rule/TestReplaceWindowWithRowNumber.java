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
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.WindowNode.Frame.DEFAULT_FRAME;

public class TestReplaceWindowWithRowNumber
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        ResolvedFunction rowNumberFunction = tester().getMetadata().resolveFunction(tester().getSession(), QualifiedName.of("row_number"), fromTypes());
        tester().assertThat(new ReplaceWindowWithRowNumber(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    return p.window(
                            new DataOrganizationSpecification(ImmutableList.of(a), Optional.empty()),
                            ImmutableMap.of(rowNumberSymbol, newWindowNodeFunction(rowNumberFunction)),
                            p.values(a));
                })
                .matches(rowNumber(
                        pattern -> pattern
                                .maxRowCountPerPartition(Optional.empty())
                                .partitionBy(ImmutableList.of("a")),
                        values("a")));

        tester().assertThat(new ReplaceWindowWithRowNumber(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    return p.window(
                            new DataOrganizationSpecification(ImmutableList.of(), Optional.empty()),
                            ImmutableMap.of(rowNumberSymbol, newWindowNodeFunction(rowNumberFunction)),
                            p.values(a));
                })
                .matches(rowNumber(
                        pattern -> pattern
                                .maxRowCountPerPartition(Optional.empty())
                                .partitionBy(ImmutableList.of()),
                        values("a")));
    }

    @Test
    public void testDoNotFire()
    {
        ResolvedFunction rank = tester().getMetadata().resolveFunction(tester().getSession(), QualifiedName.of("rank"), fromTypes());
        tester().assertThat(new ReplaceWindowWithRowNumber(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rank1 = p.symbol("rank_1");
                    return p.window(
                            new DataOrganizationSpecification(ImmutableList.of(a), Optional.empty()),
                            ImmutableMap.of(rank1, newWindowNodeFunction(rank)),
                            p.values(a));
                })
                .doesNotFire();

        ResolvedFunction rowNumber = tester().getMetadata().resolveFunction(tester().getSession(), QualifiedName.of("row_number"), fromTypes());
        tester().assertThat(new ReplaceWindowWithRowNumber(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber1 = p.symbol("row_number_1");
                    Symbol rank1 = p.symbol("rank_1");
                    return p.window(
                            new DataOrganizationSpecification(ImmutableList.of(a), Optional.empty()),
                            ImmutableMap.of(rowNumber1, newWindowNodeFunction(rowNumber), rank1, newWindowNodeFunction(rank)),
                            p.values(a));
                })
                .doesNotFire();

        tester().assertThat(new ReplaceWindowWithRowNumber(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    OrderingScheme orderingScheme = new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST));
                    Symbol rowNumber1 = p.symbol("row_number_1");
                    return p.window(
                            new DataOrganizationSpecification(ImmutableList.of(a), Optional.of(orderingScheme)),
                            ImmutableMap.of(rowNumber1, newWindowNodeFunction(rowNumber)),
                            p.values(a));
                })
                .doesNotFire();
    }

    private static WindowNode.Function newWindowNodeFunction(ResolvedFunction resolvedFunction)
    {
        return new WindowNode.Function(
                resolvedFunction,
                ImmutableList.of(),
                DEFAULT_FRAME,
                false);
    }
}
