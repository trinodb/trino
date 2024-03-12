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
import io.trino.metadata.Metadata;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.LogicalExpression.Operator.AND;

public class TestMergeFilters
        extends BaseRuleTest
{
    private final Metadata metadata = createTestMetadataManager();

    @Test
    public void test()
    {
        tester().assertThat(new MergeFilters(metadata))
                .on(p ->
                        p.filter(
                                new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new LongLiteral("44")),
                                p.filter(
                                        new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new LongLiteral("42")),
                                        p.values(p.symbol("a"), p.symbol("b")))))
                .matches(filter(
                        new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new LongLiteral("42")), new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new LongLiteral("44")))),
                        values(ImmutableMap.of("a", 0, "b", 1))));
    }
}
