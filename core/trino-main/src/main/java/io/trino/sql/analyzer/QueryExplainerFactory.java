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

import com.google.inject.Inject;
import io.trino.client.NodeVersion;
import io.trino.cost.CostCalculator;
import io.trino.cost.StatsCalculator;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.PlanFragmenter;
import io.trino.sql.planner.PlanOptimizersFactory;

import static java.util.Objects.requireNonNull;

public class QueryExplainerFactory
{
    private final PlanOptimizersFactory planOptimizersFactory;
    private final PlanFragmenter planFragmenter;
    private final PlannerContext plannerContext;
    private final StatementAnalyzerFactory statementAnalyzerFactory;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final NodeVersion version;

    @Inject
    public QueryExplainerFactory(
            PlanOptimizersFactory planOptimizersFactory,
            PlanFragmenter planFragmenter,
            PlannerContext plannerContext,
            StatementAnalyzerFactory statementAnalyzerFactory,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            NodeVersion version)
    {
        this.planOptimizersFactory = requireNonNull(planOptimizersFactory, "planOptimizersFactory is null");
        this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
        this.plannerContext = requireNonNull(plannerContext, "metadata is null");
        this.statementAnalyzerFactory = requireNonNull(statementAnalyzerFactory, "statementAnalyzerFactory is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.version = requireNonNull(version, "version is null");
    }

    public QueryExplainer createQueryExplainer(AnalyzerFactory analyzerFactory)
    {
        return new QueryExplainer(
                planOptimizersFactory,
                planFragmenter,
                plannerContext,
                analyzerFactory,
                statementAnalyzerFactory,
                statsCalculator,
                costCalculator,
                version);
    }
}
