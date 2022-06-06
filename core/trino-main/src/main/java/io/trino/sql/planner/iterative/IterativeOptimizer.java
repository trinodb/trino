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
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.cost.CachingCostProvider;
import io.trino.cost.CachingStatsProvider;
import io.trino.cost.CostCalculator;
import io.trino.cost.CostProvider;
import io.trino.cost.StatsAndCosts;
import io.trino.cost.StatsCalculator;
import io.trino.cost.StatsProvider;
import io.trino.cost.TableStatsProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.matching.Capture;
import io.trino.matching.Match;
import io.trino.matching.Pattern;
import io.trino.spi.TrinoException;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.planprinter.PlanPrinter;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.matching.Capture.newCapture;
import static io.trino.spi.StandardErrorCode.OPTIMIZER_TIMEOUT;
import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.joining;

public class IterativeOptimizer
        implements PlanOptimizer
{
    private static final Logger LOG = Logger.get(IterativeOptimizer.class);

    private final RuleStatsRecorder stats;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final List<PlanOptimizer> legacyRules;
    private final Set<Rule<?>> rules;
    private final RuleIndex ruleIndex;
    private final Predicate<Session> useLegacyRules;
    private final PlannerContext plannerContext;

    public IterativeOptimizer(PlannerContext plannerContext, RuleStatsRecorder stats, StatsCalculator statsCalculator, CostCalculator costCalculator, Set<Rule<?>> rules)
    {
        this(plannerContext, stats, statsCalculator, costCalculator, session -> false, ImmutableList.of(), rules);
    }

    public IterativeOptimizer(PlannerContext plannerContext, RuleStatsRecorder stats, StatsCalculator statsCalculator, CostCalculator costCalculator, Predicate<Session> useLegacyRules, List<PlanOptimizer> legacyRules, Set<Rule<?>> newRules)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.useLegacyRules = requireNonNull(useLegacyRules, "useLegacyRules is null");
        this.rules = requireNonNull(newRules, "rules is null");
        this.legacyRules = ImmutableList.copyOf(legacyRules);
        this.ruleIndex = RuleIndex.builder()
                .register(newRules)
                .build();

        stats.registerAll(newRules);
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector, TableStatsProvider tableStatsProvider)
    {
        // only disable new rules if we have legacy rules to fall back to
        if (useLegacyRules.test(session) && !legacyRules.isEmpty()) {
            for (PlanOptimizer optimizer : legacyRules) {
                plan = optimizer.optimize(plan, session, symbolAllocator.getTypes(), symbolAllocator, idAllocator, warningCollector, tableStatsProvider);
            }

            return plan;
        }

        Memo memo = new Memo(idAllocator, plan);
        Lookup lookup = Lookup.from(planNode -> Stream.of(memo.resolve(planNode)));

        Duration timeout = SystemSessionProperties.getOptimizerTimeout(session);
        Context context = new Context(memo, lookup, idAllocator, symbolAllocator, nanoTime(), timeout.toMillis(), session, warningCollector, tableStatsProvider);
        exploreGroup(memo.getRootGroup(), context);

        return memo.extract();
    }

    // Used for diagnostics.
    public Set<Rule<?>> getRules()
    {
        return rules;
    }

    private boolean exploreGroup(int group, Context context)
    {
        // tracks whether this group or any children groups change as
        // this method executes
        boolean progress = exploreNode(group, context);

        while (exploreChildren(group, context)) {
            progress = true;

            // if children changed, try current group again
            // in case we can match additional rules
            if (!exploreNode(group, context)) {
                // no additional matches, so bail out
                break;
            }
        }

        return progress;
    }

    private boolean exploreNode(int group, Context context)
    {
        PlanNode node = context.memo.getNode(group);

        boolean done = false;
        boolean progress = false;

        while (!done) {
            context.checkTimeoutNotExhausted();

            done = true;
            Iterator<Rule<?>> possiblyMatchingRules = ruleIndex.getCandidates(node).iterator();
            while (possiblyMatchingRules.hasNext()) {
                Rule<?> rule = possiblyMatchingRules.next();
                long timeStart = nanoTime();
                long timeEnd;
                boolean invoked = false;
                boolean applied = false;

                if (rule.isEnabled(context.session)) {
                    invoked = true;
                    Rule.Result result = transform(node, rule, context);
                    timeEnd = nanoTime();

                    if (result.getTransformedPlan().isPresent()) {
                        node = context.memo.replace(group, result.getTransformedPlan().get(), rule.getClass().getName());

                        applied = true;
                        done = false;
                        progress = true;
                    }
                }
                else {
                    timeEnd = nanoTime();
                }

                context.recordRuleInvocation(rule, invoked, applied, timeEnd - timeStart);
            }
        }

        return progress;
    }

    private <T> Rule.Result transform(PlanNode node, Rule<T> rule, Context context)
    {
        Capture<T> nodeCapture = newCapture();
        Pattern<T> pattern = rule.getPattern().capturedAs(nodeCapture);
        Iterator<Match> matches = pattern.match(node, context.lookup).iterator();
        while (matches.hasNext()) {
            Match match = matches.next();
            long duration;
            Rule.Result result;
            try {
                long start = nanoTime();
                result = rule.apply(match.capture(nodeCapture), match.captures(), ruleContext(context));

                if (LOG.isDebugEnabled() && !result.isEmpty()) {
                    LOG.debug(
                            "Rule: %s\nBefore:\n%s\nAfter:\n%s",
                            rule.getClass().getName(),
                            PlanPrinter.textLogicalPlan(
                                    node,
                                    context.symbolAllocator.getTypes(),
                                    plannerContext.getMetadata(),
                                    plannerContext.getFunctionManager(),
                                    StatsAndCosts.empty(),
                                    context.session,
                                    0,
                                    false),
                            PlanPrinter.textLogicalPlan(
                                    result.getTransformedPlan().get(),
                                    context.symbolAllocator.getTypes(),
                                    plannerContext.getMetadata(),
                                    plannerContext.getFunctionManager(),
                                    StatsAndCosts.empty(),
                                    context.session,
                                    0,
                                    false));
                }
                duration = nanoTime() - start;
            }
            catch (RuntimeException e) {
                stats.recordFailure(rule);
                throw e;
            }
            stats.record(rule, duration, !result.isEmpty());

            if (result.getTransformedPlan().isPresent()) {
                return result;
            }
        }

        return Rule.Result.empty();
    }

    private boolean exploreChildren(int group, Context context)
    {
        boolean progress = false;

        PlanNode expression = context.memo.getNode(group);
        for (PlanNode child : expression.getSources()) {
            checkState(child instanceof GroupReference, "Expected child to be a group reference. Found: " + child.getClass().getName());

            if (exploreGroup(((GroupReference) child).getGroupId(), context)) {
                progress = true;
            }
        }

        return progress;
    }

    private Rule.Context ruleContext(Context context)
    {
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, Optional.of(context.memo), context.lookup, context.session, context.symbolAllocator.getTypes(), context.tableStatsProvider);
        CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.of(context.memo), context.session, context.symbolAllocator.getTypes());

        return new Rule.Context()
        {
            @Override
            public Lookup getLookup()
            {
                return context.lookup;
            }

            @Override
            public PlanNodeIdAllocator getIdAllocator()
            {
                return context.idAllocator;
            }

            @Override
            public SymbolAllocator getSymbolAllocator()
            {
                return context.symbolAllocator;
            }

            @Override
            public Session getSession()
            {
                return context.session;
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
            public void checkTimeoutNotExhausted()
            {
                context.checkTimeoutNotExhausted();
            }

            @Override
            public WarningCollector getWarningCollector()
            {
                return context.warningCollector;
            }
        };
    }

    private static class Context
    {
        private final Memo memo;
        private final Lookup lookup;
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final long startTimeInNanos;
        private final long timeoutInMilliseconds;
        private final Session session;
        private final WarningCollector warningCollector;
        private final TableStatsProvider tableStatsProvider;

        private final Map<Rule<?>, RuleInvocationStats> ruleStats = new HashMap<>();

        public Context(
                Memo memo,
                Lookup lookup,
                PlanNodeIdAllocator idAllocator,
                SymbolAllocator symbolAllocator,
                long startTimeInNanos,
                long timeoutInMilliseconds,
                Session session,
                WarningCollector warningCollector,
                TableStatsProvider tableStatsProvider)
        {
            checkArgument(timeoutInMilliseconds >= 0, "Timeout has to be a non-negative number [milliseconds]");

            this.memo = memo;
            this.lookup = lookup;
            this.idAllocator = idAllocator;
            this.symbolAllocator = symbolAllocator;
            this.startTimeInNanos = startTimeInNanos;
            this.timeoutInMilliseconds = timeoutInMilliseconds;
            this.session = session;
            this.warningCollector = warningCollector;
            this.tableStatsProvider = tableStatsProvider;
        }

        public void checkTimeoutNotExhausted()
        {
            if ((NANOSECONDS.toMillis(nanoTime() - startTimeInNanos)) >= timeoutInMilliseconds) {
                String message = format("The optimizer exhausted the time limit of %d ms", timeoutInMilliseconds);
                if (ruleStats.isEmpty()) {
                    message += ": no rules invoked";
                }
                else {
                    long timeThresholdNanos = MILLISECONDS.toNanos(timeoutInMilliseconds) / 100;
                    List<Entry<Rule<?>, RuleInvocationStats>> topRulesByTime = ruleStats.entrySet().stream()
                            // Filter out rules that used less than 1% time that do not change the state
                            .filter(entry -> entry.getValue().getApplicationCount() > 0 || entry.getValue().getElapsedTimeNanos() > timeThresholdNanos)
                            .sorted(Comparator.<Entry<Rule<?>, RuleInvocationStats>, Long>comparing(entry -> entry.getValue().getElapsedTimeNanos()).reversed())
                            .limit(5)
                            .collect(toImmutableList());

                    if (topRulesByTime.isEmpty()) {
                        message += ": no rules used significant amount of time";
                    }
                    else {
                        message += ": Top rules: " + topRulesByTime.stream()
                                .map(entry -> format("%s: %s", entry.getKey(), entry.getValue()))
                                .collect(joining(",\n\t\t", "{\n\t\t", " }"));
                    }
                }
                throw new TrinoException(OPTIMIZER_TIMEOUT, message);
            }
        }

        public void recordRuleInvocation(Rule<?> rule, boolean invoked, boolean applied, long elapsedNanos)
        {
            ruleStats.computeIfAbsent(rule, ignored -> new RuleInvocationStats())
                    .recordRuleInvocation(invoked, applied, elapsedNanos);
        }

        private static class RuleInvocationStats
        {
            private long invocationCount;
            private long applicationCount;
            private long elapsedTimeNanos;

            void recordRuleInvocation(boolean invoked, boolean applied, long elapsedNanos)
            {
                if (invoked) {
                    invocationCount++;
                }
                if (applied) {
                    applicationCount++;
                }
                elapsedTimeNanos += elapsedNanos;
            }

            public long getInvocationCount()
            {
                return invocationCount;
            }

            public long getApplicationCount()
            {
                return applicationCount;
            }

            public long getElapsedTimeNanos()
            {
                return elapsedTimeNanos;
            }

            private long getElapsedTimeMillis()
            {
                return NANOSECONDS.toMillis(elapsedTimeNanos);
            }

            @Override
            public String toString()
            {
                long elapsedMillis = getElapsedTimeMillis();
                return format("%s ms, %s invocations, %s applications", elapsedMillis, invocationCount, applicationCount);
            }
        }
    }
}
