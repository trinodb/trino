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

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.ir.Booleans;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.ValuesNode;
import org.junit.jupiter.api.Test;

import java.lang.ref.WeakReference;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRuleIndex
{
    private final PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), PLANNER_CONTEXT, TEST_SESSION);

    @Test
    public void testWithPlanNodeHierarchy()
    {
        Rule<?> projectRule1 = new NoOpRule<>(Pattern.typeOf(ProjectNode.class));
        Rule<?> projectRule2 = new NoOpRule<>(Pattern.typeOf(ProjectNode.class));
        Rule<?> filterRule = new NoOpRule<>(Pattern.typeOf(FilterNode.class));
        Rule<?> anyRule = new NoOpRule<>(Pattern.any());

        RuleIndex ruleIndex = RuleIndex.builder()
                .register(projectRule1)
                .register(projectRule2)
                .register(filterRule)
                .register(anyRule)
                .build();

        ProjectNode projectNode = planBuilder.project(Assignments.of(), planBuilder.values());
        FilterNode filterNode = planBuilder.filter(Booleans.TRUE, planBuilder.values());
        ValuesNode valuesNode = planBuilder.values();

        assertThat(ruleIndex.getCandidates(projectNode)).containsExactly(projectRule1, projectRule2, anyRule);
        assertThat(ruleIndex.getCandidates(filterNode)).containsExactly(filterRule, anyRule);
        assertThat(ruleIndex.getCandidates(valuesNode)).containsExactly(anyRule);
    }

    @Test
    public void testIndependentIndexes()
    {
        Rule<?> firstRule = new NoOpRule<>(Pattern.typeOf(AB.class));
        Rule<?> secondRule = new NoOpRule<>(Pattern.typeOf(AB.class));
        RuleIndex firstIndex = RuleIndex.builder().register(firstRule).build();
        RuleIndex secondIndex = RuleIndex.builder().register(secondRule).build();
        RuleIndex emptyIndex = RuleIndex.builder().build();

        for (int iteration = 0; iteration < 2; iteration++) {
            assertThat(firstIndex.getCandidates(new AB())).containsExactly(firstRule);
            assertThat(secondIndex.getCandidates(new AB())).containsExactly(secondRule);
            assertThat(emptyIndex.getCandidates(new AB())).isEmpty();
        }
    }

    @Test
    public void testDoesNotRetainRules()
    {
        WeakReference<Rule<?>> rule = populateTemporaryIndex();
        assertEventually(() -> {
            System.gc();
            assertThat(rule.refersTo(null)).isTrue();
        });
    }

    private static WeakReference<Rule<?>> populateTemporaryIndex()
    {
        Rule<?> rule = new NoOpRule<>(Pattern.typeOf(CacheLifetimeNode.class));
        RuleIndex index = RuleIndex.builder().register(rule).build();
        assertThat(index.getCandidates(new CacheLifetimeNode())).containsExactly(rule);
        return new WeakReference<>(rule);
    }

    @Test
    public void testConcurrentAccess()
            throws Exception
    {
        Rule<?> specificRule = new NoOpRule<>(Pattern.typeOf(AB.class));
        Rule<?> interfaceRule = new NoOpRule<>(Pattern.typeOf(A.class));
        RuleIndex ruleIndex = RuleIndex.builder()
                .register(specificRule)
                .register(interfaceRule)
                .build();

        int threads = 8;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        Callable<Set<Rule<?>>> lookup = () -> {
            barrier.await();
            return ruleIndex.getCandidates(new AB());
        };
        try (var executor = Executors.newFixedThreadPool(threads)) {
            for (var result : executor.invokeAll(nCopies(threads, lookup))) {
                assertThat(result.get()).containsExactly(specificRule, interfaceRule);
            }
        }
    }

    @Test
    public void testInterfacesHierarchy()
    {
        Rule<?> a = new NoOpRule<>(Pattern.typeOf(A.class));
        Rule<?> b = new NoOpRule<>(Pattern.typeOf(B.class));
        Rule<?> ab = new NoOpRule<>(Pattern.typeOf(AB.class));

        RuleIndex ruleIndex = RuleIndex.builder()
                .register(a)
                .register(b)
                .register(ab)
                .build();

        assertThat(ruleIndex.getCandidates(new A() {})).containsExactlyInAnyOrder(a);
        assertThat(ruleIndex.getCandidates(new B() {})).containsExactlyInAnyOrder(b);
        assertThat(ruleIndex.getCandidates(new AB())).containsExactlyInAnyOrder(ab, a, b);
    }

    private static class NoOpRule<T>
            implements Rule<T>
    {
        private final Pattern<T> pattern;

        private NoOpRule(Pattern<T> pattern)
        {
            this.pattern = pattern;
        }

        @Override
        public Pattern<T> getPattern()
        {
            return pattern;
        }

        @Override
        public Result apply(T node, Captures captures, Context context)
        {
            return Result.empty();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("pattern", pattern)
                    .toString();
        }
    }

    private interface A {}

    private interface B {}

    private static class CacheLifetimeNode {}

    private static class AB
            implements A, B {}
}
