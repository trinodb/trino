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
import io.trino.spi.TrinoException;
import io.trino.sql.PlannerContext;
import io.trino.sql.SqlFormatter;
import io.trino.sql.planner.LogicalPlanner;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanFragmenter;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.PlanOptimizersFactory;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.planprinter.PlanPrinter;
import io.trino.sql.tree.CreateMaterializedView;
import io.trino.sql.tree.CreateSchema;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.CreateView;
import io.trino.sql.tree.DropSchema;
import io.trino.sql.tree.ExplainType.Type;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Prepare;
import io.trino.sql.tree.Statement;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.sql.ParameterUtils.parameterExtractor;
import static io.trino.sql.analyzer.QueryType.EXPLAIN;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.planprinter.IoPlanPrinter.textIoPlan;
import static io.trino.sql.planner.planprinter.PlanPrinter.jsonDistributedPlan;
import static io.trino.sql.planner.planprinter.PlanPrinter.jsonLogicalPlan;
import static io.trino.util.StatementUtils.isDataDefinitionStatement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class QueryExplainer
{
    private final List<PlanOptimizer> planOptimizers;
    private final PlanFragmenter planFragmenter;
    private final PlannerContext plannerContext;
    private final AnalyzerFactory analyzerFactory;
    private final StatementAnalyzerFactory statementAnalyzerFactory;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;

    QueryExplainer(
            PlanOptimizersFactory planOptimizersFactory,
            PlanFragmenter planFragmenter,
            PlannerContext plannerContext,
            AnalyzerFactory analyzerFactory,
            StatementAnalyzerFactory statementAnalyzerFactory,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator)
    {
        this.planOptimizers = requireNonNull(planOptimizersFactory.get(), "planOptimizers is null");
        this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.analyzerFactory = requireNonNull(analyzerFactory, "analyzerFactory is null");
        this.statementAnalyzerFactory = requireNonNull(statementAnalyzerFactory, "statementAnalyzerFactory is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
    }

    public void validate(Session session, Statement statement, List<Expression> parameters, WarningCollector warningCollector)
    {
        analyze(session, statement, parameters, warningCollector);
    }

    public String getPlan(Session session, Statement statement, Type planType, List<Expression> parameters, WarningCollector warningCollector)
    {
        Optional<String> explain = explainDataDefinition(statement, parameters);
        if (explain.isPresent()) {
            return explain.get();
        }

        switch (planType) {
            case LOGICAL:
                Plan plan = getLogicalPlan(session, statement, parameters, warningCollector);
                return PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes(), plannerContext.getMetadata(), plannerContext.getFunctionManager(), plan.getStatsAndCosts(), session, 0, false);
            case DISTRIBUTED:
                SubPlan subPlan = getDistributedPlan(session, statement, parameters, warningCollector);
                return PlanPrinter.textDistributedPlan(subPlan, plannerContext.getMetadata(), plannerContext.getFunctionManager(), session, false);
            case IO:
                return textIoPlan(getLogicalPlan(session, statement, parameters, warningCollector), plannerContext, session);
            case VALIDATE:
                // unsupported
                break;
        }
        throw new IllegalArgumentException("Unhandled plan type: " + planType);
    }

    public String getGraphvizPlan(Session session, Statement statement, Type planType, List<Expression> parameters, WarningCollector warningCollector)
    {
        Optional<String> explain = explainDataDefinition(statement, parameters);
        if (explain.isPresent()) {
            // todo format as graphviz
            return explain.get();
        }

        switch (planType) {
            case LOGICAL:
                Plan plan = getLogicalPlan(session, statement, parameters, warningCollector);
                return PlanPrinter.graphvizLogicalPlan(plan.getRoot(), plan.getTypes());
            case DISTRIBUTED:
                SubPlan subPlan = getDistributedPlan(session, statement, parameters, warningCollector);
                return PlanPrinter.graphvizDistributedPlan(subPlan);
            case VALIDATE:
            case IO:
                // unsupported
        }
        throw new IllegalArgumentException("Unhandled plan type: " + planType);
    }

    public String getJsonPlan(Session session, Statement statement, Type planType, List<Expression> parameters, WarningCollector warningCollector)
    {
        Optional<String> explain = explainDataDefinition(statement, parameters);
        if (explain.isPresent()) {
            // todo format as json
            return explain.get();
        }

        switch (planType) {
            case IO:
                Plan plan = getLogicalPlan(session, statement, parameters, warningCollector);
                return textIoPlan(plan, plannerContext, session);
            case LOGICAL:
                Plan logicalPlan = getLogicalPlan(session, statement, parameters, warningCollector);
                return jsonLogicalPlan(logicalPlan.getRoot(), logicalPlan.getTypes(), plannerContext.getMetadata(), logicalPlan.getStatsAndCosts(),
                        plannerContext.getFunctionManager(), session);
            case DISTRIBUTED:
                SubPlan subPlan = getDistributedPlan(session, statement, parameters, warningCollector);
                return jsonDistributedPlan(subPlan, plannerContext.getMetadata(), plannerContext.getFunctionManager(), session);
            case VALIDATE:
                // unsupported
                break;
        }
        throw new TrinoException(NOT_SUPPORTED, format("Unsupported explain plan type %s for JSON format", planType));
    }

    public Plan getLogicalPlan(Session session, Statement statement, List<Expression> parameters, WarningCollector warningCollector)
    {
        // analyze statement
        Analysis analysis = analyze(session, statement, parameters, warningCollector);

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

    private Analysis analyze(Session session, Statement statement, List<Expression> parameters, WarningCollector warningCollector)
    {
        Analyzer analyzer = analyzerFactory.createAnalyzer(session, parameters, parameterExtractor(statement, parameters), warningCollector);
        return analyzer.analyze(statement, EXPLAIN);
    }

    private SubPlan getDistributedPlan(Session session, Statement statement, List<Expression> parameters, WarningCollector warningCollector)
    {
        Plan plan = getLogicalPlan(session, statement, parameters, warningCollector);
        return planFragmenter.createSubPlans(session, plan, false, warningCollector);
    }

    private static <T extends Statement> Optional<String> explainDataDefinition(T statement, List<Expression> parameters)
    {
        if (!isDataDefinitionStatement(statement.getClass())) {
            return Optional.empty();
        }

        if (statement instanceof CreateSchema) {
            return Optional.of("CREATE SCHEMA " + ((CreateSchema) statement).getSchemaName());
        }
        if (statement instanceof DropSchema) {
            return Optional.of("DROP SCHEMA " + ((DropSchema) statement).getSchemaName());
        }
        if (statement instanceof CreateTable) {
            return Optional.of("CREATE TABLE " + ((CreateTable) statement).getName());
        }
        if (statement instanceof CreateView) {
            return Optional.of("CREATE VIEW " + ((CreateView) statement).getName());
        }
        if (statement instanceof CreateMaterializedView) {
            return Optional.of("CREATE MATERIALIZED VIEW " + ((CreateMaterializedView) statement).getName());
        }
        if (statement instanceof Prepare) {
            return Optional.of("PREPARE " + ((Prepare) statement).getName());
        }

        StringBuilder builder = new StringBuilder();
        builder.append(SqlFormatter.formatSql(statement));
        if (!parameters.isEmpty()) {
            builder.append("\n")
                    .append("Parameters: ")
                    .append(parameters);
        }

        return Optional.of(builder.toString());
    }
}
