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
package io.trino.sql.planner;

import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.WindowNode;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Function;

import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static org.assertj.core.api.Assertions.assertThat;

/// The optimizer orders and partitions collated strings by their collation keys, and leaves
/// default-collation strings unchanged.
public class TestSortByCollationKeyPlans
        extends BasePlanTest
{
    private static final String COLLATED = "(VALUES 'b', 'A', 'a') t(x)";

    @Test
    public void testSort()
    {
        assertOrdersByKey("SELECT x COLLATE unicode_ci FROM %s ORDER BY 1".formatted(COLLATED), SortNode.class, node -> node.getOrderingScheme().orderBy());
        assertOrdersByString("SELECT x FROM %s ORDER BY 1".formatted(COLLATED), SortNode.class, node -> node.getOrderingScheme().orderBy());
    }

    @Test
    public void testTopN()
    {
        // both steps of a split TopN order by the key, because the rule runs before the split
        assertOrdersByKey("SELECT x COLLATE unicode_ci FROM %s ORDER BY 1 LIMIT 2".formatted(COLLATED), TopNNode.class, node -> node.getOrderingScheme().orderBy());
        assertOrdersByString("SELECT x FROM %s ORDER BY 1 LIMIT 2".formatted(COLLATED), TopNNode.class, node -> node.getOrderingScheme().orderBy());
    }

    @Test
    public void testWindow()
    {
        assertOrdersByKey("SELECT rank() OVER (PARTITION BY x COLLATE unicode_ci) FROM %s".formatted(COLLATED), WindowNode.class, WindowNode::getPartitionBy);
    }

    private <T extends PlanNode> void assertOrdersByKey(@Language("SQL") String sql, Class<T> nodeType, Function<T, List<Symbol>> keys)
    {
        List<T> nodes = find(sql, nodeType);
        assertThat(nodes).as(sql).isNotEmpty();
        for (T node : nodes) {
            assertThat(keys.apply(node)).as(sql).extracting(Symbol::type).containsOnly(VARBINARY);
        }
    }

    private <T extends PlanNode> void assertOrdersByString(@Language("SQL") String sql, Class<T> nodeType, Function<T, List<Symbol>> keys)
    {
        List<T> nodes = find(sql, nodeType);
        assertThat(nodes).as(sql).isNotEmpty();
        for (T node : nodes) {
            assertThat(keys.apply(node)).as(sql).extracting(Symbol::type).doesNotContain(VARBINARY);
        }
    }

    private <T extends PlanNode> List<T> find(@Language("SQL") String sql, Class<T> nodeType)
    {
        return searchFrom(plan(sql).getRoot())
                .where(nodeType::isInstance)
                .findAll().stream()
                .map(nodeType::cast)
                .toList();
    }
}
