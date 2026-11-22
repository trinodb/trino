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
package io.trino.sql.planner.iterative;

import com.google.common.collect.ImmutableList;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ValuesNode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestLookup
{
    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

    @Test
    public void testNullResolver()
    {
        assertThatThrownBy(() -> Lookup.from(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("resolver is null");
    }

    @Test
    public void testNonGroupReference()
    {
        PlanNode node = new ValuesNode(idAllocator.getNextId(), 1);
        Lookup lookup = Lookup.from(_ -> {
            throw new AssertionError("Resolver must only be called for group references");
        });

        assertThat(lookup.resolve(node)).isSameAs(node);
    }

    @Test
    public void testMemoReplacement()
    {
        PlanNode original = new ValuesNode(idAllocator.getNextId(), 1);
        Memo memo = new Memo(idAllocator, original);
        Lookup lookup = Lookup.from(memo::resolve);
        GroupReference reference = new GroupReference(idAllocator.getNextId(), memo.getRootGroup(), original.getOutputSymbols());

        assertThat(lookup.resolve(reference)).isSameAs(original);

        PlanNode replacement = new ValuesNode(idAllocator.getNextId(), 2);
        memo.replace(memo.getRootGroup(), replacement, "test");

        assertThat(lookup.resolve(reference)).isSameAs(replacement);
    }

    @Test
    public void testNoLookup()
    {
        Lookup lookup = Lookup.noLookup();
        PlanNode node = new ValuesNode(idAllocator.getNextId(), 1);
        assertThat(lookup.resolve(node)).isSameAs(node);
        assertThatThrownBy(() -> lookup.resolve(new GroupReference(idAllocator.getNextId(), 1, ImmutableList.of())))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
