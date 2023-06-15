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

import io.trino.Session;
import io.trino.cost.CachingCostProvider;
import io.trino.cost.CachingStatsProvider;
import io.trino.cost.CostCalculator;
import io.trino.cost.CostProvider;
import io.trino.cost.StatsCalculator;
import io.trino.cost.StatsProvider;
import io.trino.cost.TableStatsProvider;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.spi.TrinoException;
import io.trino.spi.eventlistener.QueryPlanOptimizerStatistics;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.SymbolAllocator;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.spi.StandardErrorCode.OPTIMIZER_TIMEOUT;
import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.joining;

public class Context
{
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final Memo memo;
    private final Lookup lookup;
    private final PlanNodeIdAllocator idAllocator;
    private final SymbolAllocator symbolAllocator;
    private final long startTimeInNanos;
    private final long timeoutInMilliseconds;
    private final Session session;
    private final WarningCollector warningCollector;
    private final TableStatsProvider tableStatsProvider;

    private final PlanOptimizersStatsCollector statsCollector;

    public Context(
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
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

        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.memo = requireNonNull(memo, "memo is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        this.startTimeInNanos = startTimeInNanos;
        this.timeoutInMilliseconds = timeoutInMilliseconds;
        this.session = requireNonNull(session, "session is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.tableStatsProvider = requireNonNull(tableStatsProvider, "tableStatsProvider is null");

        this.statsCollector = createPlanOptimizersStatsCollector();
    }

    public Memo getMemo()
    {
        return memo;
    }

    public Lookup getLookup()
    {
        return lookup;
    }

    public PlanNodeIdAllocator getIdAllocator()
    {
        return idAllocator;
    }

    public SymbolAllocator getSymbolAllocator()
    {
        return symbolAllocator;
    }

    public Session getSession()
    {
        return session;
    }

    public WarningCollector getWarningCollector()
    {
        return warningCollector;
    }

    public TableStatsProvider getTableStatsProvider()
    {
        return tableStatsProvider;
    }

    public PlanOptimizersStatsCollector getStatsCollector()
    {
        return statsCollector;
    }

    public void checkTimeoutNotExhausted()
    {
        if ((NANOSECONDS.toMillis(nanoTime() - startTimeInNanos)) >= timeoutInMilliseconds) {
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
                                ruleStats.totalTime(),
                                ruleStats.invocations(),
                                ruleStats.applied()))
                        .collect(joining(",\n\t\t", "{\n\t\t", " }"));
            }
            throw new TrinoException(OPTIMIZER_TIMEOUT, message);
        }
    }

    public void recordRuleInvocation(Rule<?> rule, boolean invoked, boolean applied, long elapsedNanos)
    {
        statsCollector.recordRule(rule, invoked, applied, elapsedNanos);
    }

    public Rule.Context toRuleContext()
    {
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, Optional.of(memo), lookup, session, symbolAllocator.getTypes(), tableStatsProvider);
        CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.of(memo), session, symbolAllocator.getTypes());
        Context self = this;

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
            public void checkTimeoutNotExhausted()
            {
                self.checkTimeoutNotExhausted();
            }

            @Override
            public WarningCollector getWarningCollector()
            {
                return warningCollector;
            }
        };
    }
}
