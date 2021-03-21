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
import io.trino.spi.connector.SchemaTableName;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.MergeWriterNode;
import org.testng.annotations.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneMergeSourceColumns
        extends BaseRuleTest
{
    @Test
    public void testPruneInputColumn()
    {
        tester().assertThat(new PruneMergeSourceColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol mergeRow = p.symbol("merge_row");
                    Symbol rowId = p.symbol("row_id");
                    Symbol partialRows = p.symbol("partial_rows");
                    Symbol fragment = p.symbol("fragment");
                    return p.merge(
                            new SchemaTableName("schema", "table"),
                            p.values(a, mergeRow, rowId),
                            mergeRow,
                            rowId,
                            ImmutableList.of(partialRows, fragment));
                })
                .matches(
                        node(
                                MergeWriterNode.class,
                                strictProject(
                                        ImmutableMap.of(
                                                "row_id", PlanMatchPattern.expression("row_id"),
                                                "merge_row", PlanMatchPattern.expression("merge_row")),
                                        values("a", "merge_row", "row_id"))));
    }

    @Test
    public void testDoNotPruneRowId()
    {
        tester().assertThat(new PruneMergeSourceColumns())
                .on(p -> {
                    Symbol mergeRow = p.symbol("merge_row");
                    Symbol rowId = p.symbol("row_id");
                    Symbol partialRows = p.symbol("partial_rows");
                    Symbol fragment = p.symbol("fragment");
                    return p.merge(
                            new SchemaTableName("schema", "table"),
                            p.values(mergeRow, rowId),
                            mergeRow,
                            rowId,
                            ImmutableList.of(partialRows, fragment));
                })
                .doesNotFire();
    }
}
