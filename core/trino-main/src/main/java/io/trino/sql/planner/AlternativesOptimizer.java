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
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.matching.Capture;
import io.trino.matching.Match;
import io.trino.matching.Pattern;
import io.trino.spi.TrinoException;
import io.trino.spi.eventlistener.QueryPlanOptimizerStatistics;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Memo;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.RuleIndex;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.ChooseAlternativeNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.planprinter.PlanPrinter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.cache.CacheCommonSubqueries.isCacheChooseAlternativeNode;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.matching.Capture.newCapture;
import static io.trino.spi.StandardErrorCode.OPTIMIZER_TIMEOUT;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.plan.ChooseAlternativeNode.FilteredTableScan;
import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.joining;

/**
 * Runs the given rules and creates ChooseAlternativeNodes with the received alternatives.
 * Current algorithm:
 * 1. Traverse the plan in post-order (scan -> filter -> projection -> aggregation).
 * 2. If the child node is a ChooseAlternativeNode, create chains of nodes with each of the sources (alternatives), otherwise use the node as-is.
 * 3. Try to execute rules on each node chain and accumulate the alternatives.
 * 4. If there is more than one alternative, replace the node with a ChooseAlternativeNode.
 * 5. Stop traversing when encountering a node that can't have multiple alternatives (has more than 2 sources \ of an unsupported type).
 */
public class AlternativesOptimizer
        implements PlanOptimizer
{
    private static final Logger LOG = Logger.get(AlternativesOptimizer.class);

    private final PlannerContext plannerContext;
    private final RuleStatsRecorder stats;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final RuleIndex ruleIndex;

    public AlternativesOptimizer(
            PlannerContext plannerContext,
            RuleStatsRecorder stats,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            Set<Rule<?>> rules)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.ruleIndex = RuleIndex.builder()
                .register(requireNonNull(rules, "rules is null"))
                .build();

        stats.registerAll(rules);
    }

    @Override
    public PlanNode optimize(PlanNode plan, PlanOptimizer.Context optimizerContext)
    {
        Memo memo = new Memo(optimizerContext.idAllocator(), plan);
        Lookup lookup = Lookup.from(planNode -> Stream.of(memo.resolve(planNode)));

        Duration timeout = SystemSessionProperties.getOptimizerTimeout(optimizerContext.session());
        Context context = new Context(memo, lookup, optimizerContext, nanoTime(), timeout.toMillis());
        exploreGroup(memo.getRootGroup(), context, Optional.empty());
        optimizerContext.planOptimizersStatsCollector().add(context.getStatsCollector());

        return memo.extract();
    }

    private void exploreGroup(int group, Context context, Optional<FilteredTableScan> preCalculatedOriginalScan)
    {
        PlanNode node = context.memo.getNode(group);

        if (isCacheChooseAlternativeNode(node, context.lookup)) {
            // skip split level cache alternatives
            return;
        }

        // must be done before creating ChooseAlternativeNode, otherwise there will be multiple children, and the result would be Optional.empty()
        Optional<FilteredTableScan> originalTableScan = preCalculatedOriginalScan.or(() -> findFilteredScanInChain(node, context));

        // post-order traversal (scan -> filter -> projection -> aggregation) since the optimizer is not iterative
        for (PlanNode child : node.getSources()) {
            checkState(child instanceof GroupReference, "Expected child to be a group reference. Found: " + child.getClass().getName());

            exploreGroup(((GroupReference) child).getGroupId(), context, originalTableScan);
        }
        originalTableScan.ifPresent(tableScan -> exploreNodeAlternatives(group, tableScan, context));
    }

    /**
     * Explores a node with each of its alternatives.
     * Rule patterns are not aware of ChooseAlternativeNode and therefore on each iteration, we replace it
     * with all its alternatives (create "branches"), optimize each branch, and create a new ChooseAlternativeNode.
     *  (A)                                                                              (A)
     *  |  \                                                                           /     \
     * (B)  (C)                          (B) (B) (B)           Alternatives[(B1) (B2) (B3)]   (C)
     * |                                  |   |   |                          |    |    |
     * Alternatives[(1) (2) (3)]   =>    (1) (2) (3)     =>                 (1)  (2)  (3)
     *               |   |   |           |   |   |                           |    |    |
     *              ... ... ...         ... ... ...                         ...  ...  ...
     * We push ChooseAlternativeNode up to the point where there is a node that can't have
     * multiple alternatives, even when there are no new alternatives in practice.
     * For example, let's assume Filter and Project can have multiple alternatives but in practice,
     * there was no rule that returned multiple alternatives for project. We will still convert
     *             Project
     *               |
     * ChooseAlternative (Filter, TableScan)
     *                      |
     *                  TableScan'
     * to:
     * ChooseAlternative (Project, Project)
     *                      |         |
     *                   Filter    TableScan
     *                      |
     *                 TableScan'
     * That's because of the IterativeOptimizer that is executed after this optimizer.
     * Since rule patterns are not aware of ChooseAlternativeNode, it's simpler to just push it up in the tree as possible
     * and let the existing rules match on its children. Because otherwise, we'll need to replace ChooseAlternativeNode
     * with its alternatives and create several "branches" at IterativeOptimizer as well so rule patterns such
     * "project().with(source().matching(filter())" could be matched.
     * Instead, we do that here and use the standard IterativeOptimizer later.
     */
    private void exploreNodeAlternatives(int group, FilteredTableScan originalTableScan, Context context)
    {
        PlanNode node = context.memo.getNode(group);
        if (!canCreateAlternatives(node)) {
            return;
        }

        List<PlanNode> alternatives = new ArrayList<>();
        PlanNode child = node.getSources().get(0);
        PlanNode resolvedChild = context.lookup.resolve(child);
        if (resolvedChild instanceof ChooseAlternativeNode chooseAlternativeNode) {
            for (PlanNode alternative : chooseAlternativeNode.getSources()) {
                // node is copied so plan won't contain several nodes with the same id
                PlanNode branch = PlanCopier.copyPlan(node, context.optimizerContext.idAllocator(), context.lookup)
                        .replaceChildren(List.of(alternative));

                List<PlanNode> newAlternatives = exploreNode(branch, originalTableScan, context);
                if (newAlternatives.size() > 0) {
                    alternatives.addAll(newAlternatives);
                }
                else {
                    // don't lose the alternative if it wasn't replaced by some new alternatives
                    alternatives.add(branch);
                }
            }
        }
        else {
            alternatives = exploreNode(node, originalTableScan, context);
        }
        if (alternatives.size() > 1) {
            context.memo.replace(
                    group,
                    new ChooseAlternativeNode(context.optimizerContext.idAllocator().getNextId(), alternatives, originalTableScan),
                    this.getClass().getName());
        }
    }

    private static boolean canCreateAlternatives(PlanNode node)
    {
        return node.getSources().size() == 1 &&
                !(node instanceof ExchangeNode || // we only support alternatives within the stage and not on stages with children
                        node instanceof OutputNode); // suppose to be only 1 OutputNode (see VerifyOnlyOneOutputNode)
    }

    /**
     * Explores a single node alternative
     */
    private List<PlanNode> exploreNode(PlanNode node, FilteredTableScan originalTableScan, Context context)
    {
        // TODO: Currently, in the case of 2 rules that have been applied on the node and in the case of a single rule
        //   that has been applied more than once (its pattern matched more than once in transform()),
        //   we accumulate the alternatives. Also, if a rule is not invoked and another is applied,
        //   we don't try to invoke the first rule again. Maybe we should consider adding the following logic:
        //   1. Create a set of rules that were already applied.
        //   2. After applying a rule, add the rule to the set and keep the returned alternatives.
        //   3. For each not-yet applied rule, try to apply it on each of the alternatives.
        //   4. If a rule was applied on an alternative, replace it with the alternatives returned from the rule.
        //   5. After trying to apply the rule on all the alternatives - add it to the set.
        //   6. Continue running 3-5 in a loop, until no more rules are applied.
        List<PlanNode> allAlternatives = new ArrayList<>();
        Iterator<Rule<?>> possiblyMatchingRules = ruleIndex.getCandidates(node).iterator();
        while (possiblyMatchingRules.hasNext()) {
            context.checkTimeoutNotExhausted();

            Rule<?> rule = possiblyMatchingRules.next();
            long timeStart = nanoTime();
            long timeEnd;
            boolean invoked = false;
            boolean applied = false;

            if (rule.isEnabled(context.optimizerContext.session())) {
                invoked = true;
                List<PlanNode> currentAlternatives = transform(node, originalTableScan, rule, context);
                timeEnd = nanoTime();

                if (!currentAlternatives.isEmpty()) {
                    allAlternatives.addAll(currentAlternatives);
                    applied = true;
                }
            }
            else {
                timeEnd = nanoTime();
            }

            context.recordRuleInvocation(rule, invoked, applied, timeEnd - timeStart);
        }
        return allAlternatives;
    }

    private <T> List<PlanNode> transform(PlanNode node, FilteredTableScan originalTableScan, Rule<T> rule, Context context)
    {
        List<PlanNode> alternatives = new ArrayList<>();

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

                if (!result.isEmpty()) {
                    alternatives.add(result.getMainAlternative().orElse(node));
                    for (PlanNode alternative : result.getAdditionalAlternatives()) {
                        Optional<TableScanNode> scanNode = findTableScanInChain(alternative, context);
                        checkArgument(scanNode.isPresent(), "TableScanNode not found");
                        alternatives.add(alternative);
                    }
                }
                duration = nanoTime() - start;
            }
            catch (RuntimeException e) {
                stats.recordFailure(rule);
                context.getStatsCollector().recordFailure(rule);
                throw e;
            }
            stats.record(rule, duration, !result.isEmpty());
        }

        logTransformation(node, originalTableScan, rule, context, alternatives);
        return alternatives;
    }

    /**
     * Gets a PlanNode, and if it's a node chain (each node has only one child) that contains a TableScanNode - return it.
     */
    private Optional<TableScanNode> findTableScanInChain(PlanNode chain, Context context)
    {
        return searchFrom(chain, context.lookup)
                .recurseOnlyWhen(node -> node.getSources().size() < 2) // otherwise not a node chain
                .where(node -> node instanceof TableScanNode)
                .findFirst();
    }

    /**
     * Gets a PlanNode, and if it's a node chain (each node has only one child) that contains a TableScanNode,
     * return the TableScanNode along with its parent filter (if any).
     */
    private Optional<FilteredTableScan> findFilteredScanInChain(PlanNode chain, Context context)
    {
        if (chain instanceof FilterNode filterNode) {
            PlanNode source = context.lookup.resolve(filterNode.getSource());
            if (source instanceof TableScanNode tableScanNode) {
                return Optional.of(new FilteredTableScan(tableScanNode, Optional.of(filterNode.getPredicate())));
            }
        }
        if (chain instanceof TableScanNode tableScanNode) {
            return Optional.of(new FilteredTableScan(tableScanNode, Optional.empty()));
        }
        if (chain.getSources().size() != 1) {
            // not a node chain / not found
            return Optional.empty();
        }

        PlanNode child = context.lookup.resolve(chain.getSources().get(0));
        return findFilteredScanInChain(child, context);
    }

    private <T> void logTransformation(
            PlanNode node,
            FilteredTableScan originalTableScan,
            Rule<T> rule,
            Context context,
            List<PlanNode> alternatives)
    {
        if (LOG.isDebugEnabled() && !alternatives.isEmpty()) {
            LOG.debug(
                    "Rule: %s\nBefore:\n%s\nAfter:\n%s",
                    rule.getClass().getName(),
                    PlanPrinter.textLogicalPlan(
                            node,
                            plannerContext.getMetadata(),
                            plannerContext.getFunctionManager(),
                            StatsAndCosts.empty(),
                            context.optimizerContext.session(),
                            0,
                            false),
                    PlanPrinter.textLogicalPlan(
                            new ChooseAlternativeNode(new PlanNodeId("<TRANSIENT>"), alternatives, originalTableScan),
                            plannerContext.getMetadata(),
                            plannerContext.getFunctionManager(),
                            StatsAndCosts.empty(),
                            context.optimizerContext.session(),
                            0,
                            false));
        }
    }

    // Copied from io.trino.sql.planner.iterative.IterativeOptimizer
    private Rule.Context ruleContext(Context context)
    {
        StatsProvider statsProvider = new CachingStatsProvider(
                statsCalculator,
                Optional.of(context.memo),
                context.lookup,
                context.optimizerContext.session(),
                context.optimizerContext.tableStatsProvider(),
                context.optimizerContext.runtimeInfoProvider());
        CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.of(context.memo), context.optimizerContext.session());

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
                return context.optimizerContext.idAllocator();
            }

            @Override
            public SymbolAllocator getSymbolAllocator()
            {
                return context.optimizerContext.symbolAllocator();
            }

            @Override
            public Session getSession()
            {
                return context.optimizerContext.session();
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
                return context.optimizerContext.warningCollector();
            }
        };
    }

    // Based on a similar class at io.trino.sql.planner.iterative.IterativeOptimizer
    private static class Context
    {
        private final Memo memo;
        private final Lookup lookup;
        private final PlanOptimizer.Context optimizerContext;
        private final long startTimeInNanos;
        private final long timeoutInMilliseconds;

        private final PlanOptimizersStatsCollector statsCollector;

        public Context(
                Memo memo,
                Lookup lookup,
                PlanOptimizer.Context optimizerContext,
                long startTimeInNanos,
                long timeoutInMilliseconds)
        {
            checkArgument(timeoutInMilliseconds >= 0, "Timeout has to be a non-negative number [milliseconds]");

            this.memo = memo;
            this.lookup = lookup;
            this.optimizerContext = optimizerContext;
            this.startTimeInNanos = startTimeInNanos;
            this.timeoutInMilliseconds = timeoutInMilliseconds;
            this.statsCollector = createPlanOptimizersStatsCollector();
        }

        public void checkTimeoutNotExhausted()
        {
            if (NANOSECONDS.toMillis(nanoTime() - startTimeInNanos) >= timeoutInMilliseconds) {
                String message = format("The optimizer exhausted the time limit of %d ms", timeoutInMilliseconds);
                List<QueryPlanOptimizerStatistics> topRulesByTime = statsCollector.getTopRuleStats(5);
                if (topRulesByTime.isEmpty()) {
                    message += ": no rules invoked";
                }
                else {
                    message += ": Top rules: " + topRulesByTime.stream()
                            .map(ruleStats -> format(
                                    "%s: %s ms, %s invocations, %s applications",
                                    ruleStats.rule(),
                                    NANOSECONDS.toMillis(ruleStats.totalTime()),
                                    ruleStats.invocations(),
                                    ruleStats.applied()))
                            .collect(joining(",\n\t\t", "{\n\t\t", " }"));
                }
                throw new TrinoException(OPTIMIZER_TIMEOUT, message);
            }
        }

        public PlanOptimizersStatsCollector getStatsCollector()
        {
            return statsCollector;
        }

        void recordRuleInvocation(Rule<?> rule, boolean invoked, boolean applied, long elapsedNanos)
        {
            statsCollector.recordRule(rule, invoked, applied, elapsedNanos);
        }
    }
}
