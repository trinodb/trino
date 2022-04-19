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
package io.trino.sql.analyzer;

import io.trino.Session;
import io.trino.cost.CostCalculator;
import io.trino.cost.StatsCalculator;
import io.trino.execution.warnings.WarningCollector;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.LogicalPlanner;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.PlanOptimizersFactory;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Statement;

import java.util.List;

import static io.trino.sql.ParameterUtils.parameterExtractor;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static java.util.Objects.requireNonNull;

public class QueryAnalyzer
{
    private final QueryAnalyzerFactory queryAnalyzerFactory;
    private final List<PlanOptimizer> planOptimizers;
    private final PlannerContext plannerContext;
    private final AnalyzerFactory analyzerFactory;
    private final StatementAnalyzerFactory statementAnalyzerFactory;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;

    QueryAnalyzer(
            QueryAnalyzerFactory queryAnalyzerFactory,
            PlanOptimizersFactory planOptimizersFactory,
            PlannerContext plannerContext,
            AnalyzerFactory analyzerFactory,
            StatementAnalyzerFactory statementAnalyzerFactory,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator)
    {
        this.queryAnalyzerFactory = requireNonNull(queryAnalyzerFactory, "queryAnalyzerFactory is null");
        this.planOptimizers = requireNonNull(planOptimizersFactory.get(), "planOptimizers is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.analyzerFactory = requireNonNull(analyzerFactory, "analyzerFactory is null");
        this.statementAnalyzerFactory = requireNonNull(statementAnalyzerFactory, "statementAnalyzerFactory is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
    }

    public Analysis analyze(Session session, Statement statement, List<Expression> parameters, WarningCollector warningCollector)
    {
        Analyzer analyzer = analyzerFactory.createAnalyzer(session, parameters, parameterExtractor(statement, parameters), warningCollector, queryAnalyzerFactory);
        return analyzer.analyze(statement);
    }

    public Plan getLogicalPlan(Session session, Analysis analysis, WarningCollector warningCollector)
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

        // plan statement
        LogicalPlanner logicalPlanner = new LogicalPlanner(
                session,
                planOptimizers,
                idAllocator,
                plannerContext,
                new TypeAnalyzer(plannerContext, statementAnalyzerFactory),
                statsCalculator,
                costCalculator,
                warningCollector);
        return logicalPlanner.plan(analysis, OPTIMIZED_AND_VALIDATED, true);
    }
}
