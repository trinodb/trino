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
package io.trino.sql.planner.iterative.rule.test;

import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.cost.CachingCostProvider;
import io.trino.cost.CachingStatsProvider;
import io.trino.cost.CachingTableStatsProvider;
import io.trino.cost.CostCalculator;
import io.trino.cost.CostProvider;
import io.trino.cost.RuntimeInfoProvider;
import io.trino.cost.StatsAndCosts;
import io.trino.cost.StatsCalculator;
import io.trino.cost.StatsProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.matching.Capture;
import io.trino.matching.Match;
import io.trino.matching.Pattern;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Memo;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.testing.PlanTester;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.MoreCollectors.toOptional;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.assertions.PlanAssert.assertPlan;
import static io.trino.sql.planner.planprinter.PlanPrinter.textLogicalPlan;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Fail.fail;

public class RuleAssert
{
    private final Rule<?> rule;
    private final PlanTester planTester;
    private final StatsCalculator statsCalculator;
    private final Session session;
    private final PlanNode plan;
    private final Set<Symbol> symbols;

    private final PlanNodeIdAllocator idAllocator;

    RuleAssert(Rule<?> rule, PlanTester planTester, StatsCalculator statsCalculator, Session session, PlanNodeIdAllocator idAllocator, PlanNode plan, Collection<Symbol> symbols)
    {
        this.rule = requireNonNull(rule, "rule is null");
        this.planTester = requireNonNull(planTester, "planTester is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        // verify session is in a transaction
        session.getRequiredTransactionId();
        this.session = session;
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.plan = requireNonNull(plan, "plan is null");
        this.symbols = ImmutableSet.copyOf(symbols);
    }

    public void doesNotFire()
    {
        try {
            RuleApplication ruleApplication = applyRule();

            if (ruleApplication.wasRuleApplied()) {
                fail(format(
                        """
                        Expected %s to not fire for:

                        %s

                        ==>

                        %s
                        """,
                        rule,
                        textLogicalPlan(plan, planTester.getPlannerContext().getMetadata(), planTester.getPlannerContext().getFunctionManager(), StatsAndCosts.empty(), session, 2, false),
                        textLogicalPlan(ruleApplication.result.getTransformedPlan().get(), planTester.getPlannerContext().getMetadata(), planTester.getPlannerContext().getFunctionManager(), StatsAndCosts.empty(), session, 2, false)));
            }
        }
        finally {
            planTester.getPlannerContext().getMetadata().cleanupQuery(session);
            planTester.getTransactionManager().asyncAbort(session.getRequiredTransactionId());
        }
    }

    public void matches(PlanMatchPattern pattern)
    {
        try {
            RuleApplication ruleApplication = applyRule();

            if (!ruleApplication.wasRuleApplied()) {
                fail(format(
                        "%s did not fire for:\n%s",
                        rule,
                        formatPlan(plan)));
            }

            PlanNode actual = ruleApplication.getTransformedPlan();

            if (actual == plan) { // plans are not comparable, so we can only ensure they are not the same instance
                fail(format(
                        """
                        %s: rule fired but return the original plan:
                        %s
                        """,
                        rule,
                        formatPlan(plan)));
            }

            if (!ImmutableSet.copyOf(plan.getOutputSymbols()).equals(ImmutableSet.copyOf(actual.getOutputSymbols()))) {
                fail(format(
                        """
                        %s: output schema of transformed and original plans are not equivalent
                        \texpected: %s
                        \tactual:   %s
                        """,
                        rule,
                        plan.getOutputSymbols(),
                        actual.getOutputSymbols()));
            }

            assertPlan(session, planTester.getPlannerContext().getMetadata(), planTester.getPlannerContext().getFunctionManager(), ruleApplication.statsProvider(), new Plan(actual, StatsAndCosts.empty()), ruleApplication.lookup(), pattern);
        }
        finally {
            planTester.getPlannerContext().getMetadata().cleanupQuery(session);
            planTester.getTransactionManager().asyncAbort(session.getRequiredTransactionId());
        }
    }

    private RuleApplication applyRule()
    {
        SymbolAllocator symbolAllocator = new SymbolAllocator(symbols);
        Memo memo = new Memo(idAllocator, plan);
        Lookup lookup = Lookup.from(planNode -> Stream.of(memo.resolve(planNode)));

        PlanNode memoRoot = memo.getNode(memo.getRootGroup());

        return applyRule(rule, memoRoot, ruleContext(statsCalculator, planTester.getEstimatedExchangesCostCalculator(), symbolAllocator, memo, lookup, session));
    }

    private static <T> RuleApplication applyRule(Rule<T> rule, PlanNode planNode, Rule.Context context)
    {
        Capture<T> planNodeCapture = newCapture();
        Pattern<T> pattern = rule.getPattern().capturedAs(planNodeCapture);
        Optional<Match> match = pattern.match(planNode, context.getLookup())
                .collect(toOptional());

        Rule.Result result;
        if (!rule.isEnabled(context.getSession()) || match.isEmpty()) {
            result = Rule.Result.empty();
        }
        else {
            result = rule.apply(match.get().capture(planNodeCapture), match.get().captures(), context);
        }

        return new RuleApplication(context.getLookup(), context.getStatsProvider(), result);
    }

    private String formatPlan(PlanNode plan)
    {
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, new CachingTableStatsProvider(planTester.getPlannerContext().getMetadata(), session));
        CostProvider costProvider = new CachingCostProvider(planTester.getCostCalculator(), statsProvider, session);
        return textLogicalPlan(plan, planTester.getPlannerContext().getMetadata(), planTester.getPlannerContext().getFunctionManager(), StatsAndCosts.create(plan, statsProvider, costProvider), session, 2, false);
    }

    private Rule.Context ruleContext(StatsCalculator statsCalculator, CostCalculator costCalculator, SymbolAllocator symbolAllocator, Memo memo, Lookup lookup, Session session)
    {
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, Optional.of(memo), lookup, session, new CachingTableStatsProvider(planTester.getPlannerContext().getMetadata(), session), RuntimeInfoProvider.noImplementation());
        CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.of(memo), session);

        return new Rule.Context()
        {
            @Override
            public Lookup getLookup()
            {
                return lookup;
            }

            @Override
            public PlanNodeIdAllocator getIdAllocator()
            {
                return idAllocator;
            }

            @Override
            public SymbolAllocator getSymbolAllocator()
            {
                return symbolAllocator;
            }

            @Override
            public Session getSession()
            {
                return session;
            }

            @Override
            public StatsProvider getStatsProvider()
            {
                return statsProvider;
            }

            @Override
            public CostProvider getCostProvider()
            {
                return costProvider;
            }

            @Override
            public void checkTimeoutNotExhausted() {}

            @Override
            public WarningCollector getWarningCollector()
            {
                return WarningCollector.NOOP;
            }
        };
    }

    private record RuleApplication(Lookup lookup, StatsProvider statsProvider, Rule.Result result)
    {
        private RuleApplication(Lookup lookup, StatsProvider statsProvider, Rule.Result result)
        {
            this.lookup = requireNonNull(lookup, "lookup is null");
            this.statsProvider = requireNonNull(statsProvider, "statsProvider is null");
            this.result = requireNonNull(result, "result is null");
        }

        private boolean wasRuleApplied()
        {
            return !result.isEmpty();
        }

        public PlanNode getTransformedPlan()
        {
            return result.getTransformedPlan().orElseThrow(() -> new IllegalStateException("Rule did not produce transformed plan"));
        }
    }
}
