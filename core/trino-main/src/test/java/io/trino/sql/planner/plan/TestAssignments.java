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
package io.trino.sql.planner.plan;

import com.google.common.collect.ImmutableList;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAssignments
{
    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 1, 2})
    public void testRewritePreservesOrderAndUnchangedEntries(int changedIndex)
    {
        List<Symbol> symbols = ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "b"), new Symbol(BIGINT, "c"));
        Assignments original = Assignments.identity(symbols);
        List<Expression> expressions = ImmutableList.copyOf(original.expressions());
        List<Expression> visited = new ArrayList<>();
        Constant replacement = new Constant(BIGINT, 7L);
        Assignments rewritten = original.rewrite(expression -> {
            visited.add(expression);
            return changedIndex >= 0 && expression == expressions.get(changedIndex) ? replacement : expression;
        });

        assertThat(visited).containsExactlyElementsOf(expressions);
        assertThat(rewritten.outputs()).containsExactlyElementsOf(symbols);
        assertThat(original.expressions()).containsExactlyElementsOf(expressions);
        for (int index = 0; index < symbols.size(); index++) {
            assertThat(rewritten.get(symbols.get(index))).isSameAs(index == changedIndex ? replacement : expressions.get(index));
        }
        if (changedIndex < 0) {
            assertThat(rewritten).isSameAs(original);
        }
    }

    @Test
    public void testRewriteRetainsEqualReplacement()
    {
        Symbol symbol = new Symbol(BIGINT, "a");
        Assignments original = Assignments.identity(symbol);
        Reference replacement = new Reference(BIGINT, "a");
        assertThat(original.rewrite(_ -> replacement).get(symbol)).isSameAs(replacement);
    }

    @Test
    public void testRewriteEmptyAssignments()
    {
        Assignments assignments = Assignments.of();
        assertThat(assignments.rewrite(_ -> {
            throw new AssertionError("empty assignments must not invoke the rewriter");
        })).isSameAs(assignments);
    }
}
