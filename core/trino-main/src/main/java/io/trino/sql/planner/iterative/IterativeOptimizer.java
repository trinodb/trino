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
import io.trino.execution.warnings.WarningCollector;
import io.trino.matching.Capture;
import io.trino.matching.Match;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.spi.TrinoException;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.planprinter.PlanPrinter;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.matching.Capture.newCapture;
import static io.trino.spi.StandardErrorCode.OPTIMIZER_TIMEOUT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

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
    private final Metadata metadata;

    public IterativeOptimizer(Metadata metadata, RuleStatsRecorder stats, StatsCalculator statsCalculator, CostCalculator costCalculator, Set<Rule<?>> rules)
    {
        this(metadata, stats, statsCalculator, costCalculator, session -> false, ImmutableList.of(), rules);
    }

    public IterativeOptimizer(Metadata metadata, RuleStatsRecorder stats, StatsCalculator statsCalculator, CostCalculator costCalculator, Predicate<Session> useLegacyRules, List<PlanOptimizer> legacyRules, Set<Rule<?>> newRules)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
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
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        // only disable new rules if we have legacy rules to fall back to
        if (useLegacyRules.test(session) && !legacyRules.isEmpty()) {
            for (PlanOptimizer optimizer : legacyRules) {
                plan = optimizer.optimize(plan, session, symbolAllocator.getTypes(), symbolAllocator, idAllocator, warningCollector);
            }

            return plan;
        }

        Memo memo = new Memo(idAllocator, plan);
        Lookup lookup = Lookup.from(planNode -> Stream.of(memo.resolve(planNode)));

        Duration timeout = SystemSessionProperties.getOptimizerTimeout(session);
        Context context = new Context(memo, lookup, idAllocator, symbolAllocator, System.nanoTime(), timeout.toMillis(), session, warningCollector);
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

                if (!rule.isEnabled(context.session)) {
                    continue;
                }

                Rule.Result result = transform(node, rule, context);

                if (result.getTransformedPlan().isPresent()) {
                    node = context.memo.replace(group, result.getTransformedPlan().get(), rule.getClass().getName());

                    done = false;
                    progress = true;
                }
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
                long start = System.nanoTime();
                result = rule.apply(match.capture(nodeCapture), match.captures(), ruleContext(context));

                if (LOG.isDebugEnabled() && !result.isEmpty()) {
                    LOG.debug(
                            "Rule: %s\nBefore:\n%s\nAfter:\n%s",
                            rule.getClass().getName(),
                            PlanPrinter.textLogicalPlan(node, context.symbolAllocator.getTypes(), metadata, StatsAndCosts.empty(), context.session, 0, false),
                            PlanPrinter.textLogicalPlan(result.getTransformedPlan().get(), context.symbolAllocator.getTypes(), metadata, StatsAndCosts.empty(), context.session, 0, false));
                }
                duration = System.nanoTime() - start;
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
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, Optional.of(context.memo), context.lookup, context.session, context.symbolAllocator.getTypes());
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

        public Context(
                Memo memo,
                Lookup lookup,
                PlanNodeIdAllocator idAllocator,
                SymbolAllocator symbolAllocator,
                long startTimeInNanos,
                long timeoutInMilliseconds,
                Session session,
                WarningCollector warningCollector)
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
        }

        public void checkTimeoutNotExhausted()
        {
            if ((NANOSECONDS.toMillis(System.nanoTime() - startTimeInNanos)) >= timeoutInMilliseconds) {
                throw new TrinoException(OPTIMIZER_TIMEOUT, format("The optimizer exhausted the time limit of %d ms", timeoutInMilliseconds));
            }
        }
    }
}
