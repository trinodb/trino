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
package io.trino.sql.rewrite;

import com.google.inject.Inject;
import io.trino.Session;
import io.trino.execution.QueryPreparer;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.analyzer.QueryExplainer;
import io.trino.sql.analyzer.QueryExplainerFactory;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Explain;
import io.trino.sql.tree.ExplainAnalyze;
import io.trino.sql.tree.ExplainFormat;
import io.trino.sql.tree.ExplainOption;
import io.trino.sql.tree.ExplainType;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Statement;

import java.util.List;
import java.util.Map;

import static io.trino.sql.QueryUtil.singleValueQuery;
import static io.trino.sql.tree.ExplainFormat.Type.JSON;
import static io.trino.sql.tree.ExplainFormat.Type.TEXT;
import static io.trino.sql.tree.ExplainType.Type.DISTRIBUTED;
import static io.trino.sql.tree.ExplainType.Type.IO;
import static io.trino.sql.tree.ExplainType.Type.VALIDATE;
import static java.util.Objects.requireNonNull;

public final class ExplainRewrite
        implements StatementRewrite.Rewrite
{
    private final QueryExplainerFactory queryExplainerFactory;
    private final QueryPreparer queryPreparer;

    @Inject
    public ExplainRewrite(QueryExplainerFactory queryExplainerFactory, QueryPreparer queryPreparer)
    {
        this.queryExplainerFactory = requireNonNull(queryExplainerFactory, "queryExplainerFactory is null");
        this.queryPreparer = requireNonNull(queryPreparer, "queryPreparer is null");
    }

    @Override
    public Statement rewrite(
            AnalyzerFactory analyzerFactory,
            Session session,
            Statement node,
            List<Expression> parameter,
            Map<NodeRef<Parameter>, Expression> parameterLookup,
            WarningCollector warningCollector,
            PlanOptimizersStatsCollector planOptimizersStatsCollector)
    {
        return (Statement) new Visitor(session, queryPreparer, queryExplainerFactory.createQueryExplainer(analyzerFactory), warningCollector, planOptimizersStatsCollector).process(node, null);
    }

    private static final class Visitor
            extends AstVisitor<Node, Void>
    {
        private final Session session;
        private final QueryPreparer queryPreparer;
        private final QueryExplainer queryExplainer;
        private final WarningCollector warningCollector;
        private final PlanOptimizersStatsCollector planOptimizersStatsCollector;

        public Visitor(
                Session session,
                QueryPreparer queryPreparer,
                QueryExplainer queryExplainer,
                WarningCollector warningCollector,
                PlanOptimizersStatsCollector planOptimizersStatsCollector)
        {
            this.session = requireNonNull(session, "session is null");
            this.queryPreparer = requireNonNull(queryPreparer, "queryPreparer is null");
            this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
            this.planOptimizersStatsCollector = planOptimizersStatsCollector;
        }

        @Override
        protected Node visitExplainAnalyze(ExplainAnalyze node, Void context)
        {
            Statement statement = (Statement) process(node.getStatement(), context);
            return new ExplainAnalyze(node.getLocation(), statement, node.isVerbose());
        }

        @Override
        protected Node visitExplain(Explain node, Void context)
        {
            ExplainType.Type planType = DISTRIBUTED;
            ExplainFormat.Type planFormat = TEXT;
            List<ExplainOption> options = node.getOptions();

            for (ExplainOption option : options) {
                if (option instanceof ExplainType) {
                    planType = ((ExplainType) option).getType();
                    // Use JSON as the default format for EXPLAIN (TYPE IO).
                    if (planType == IO) {
                        planFormat = JSON;
                    }
                    break;
                }
            }

            for (ExplainOption option : options) {
                if (option instanceof ExplainFormat) {
                    planFormat = ((ExplainFormat) option).getType();
                    break;
                }
            }

            return getQueryPlan(node, planType, planFormat);
        }

        private Node getQueryPlan(Explain node, ExplainType.Type planType, ExplainFormat.Type planFormat)
                throws IllegalArgumentException
        {
            PreparedQuery preparedQuery = queryPreparer.prepareQuery(session, node.getStatement());

            if (planType == VALIDATE) {
                queryExplainer.validate(session, preparedQuery.getStatement(), preparedQuery.getParameters(), warningCollector, planOptimizersStatsCollector);
                return singleValueQuery("Valid", true);
            }

            String plan;
            switch (planFormat) {
                case GRAPHVIZ:
                    plan = queryExplainer.getGraphvizPlan(session, preparedQuery.getStatement(), planType, preparedQuery.getParameters(), warningCollector, planOptimizersStatsCollector);
                    break;
                case JSON:
                    plan = queryExplainer.getJsonPlan(session, preparedQuery.getStatement(), planType, preparedQuery.getParameters(), warningCollector, planOptimizersStatsCollector);
                    break;
                case TEXT:
                    plan = queryExplainer.getPlan(session, preparedQuery.getStatement(), planType, preparedQuery.getParameters(), warningCollector, planOptimizersStatsCollector);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid Explain Format: " + planFormat);
            }
            return singleValueQuery("Query Plan", plan);
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }
    }
}
