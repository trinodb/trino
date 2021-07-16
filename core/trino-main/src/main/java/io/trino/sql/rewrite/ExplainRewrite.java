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

import io.trino.Session;
import io.trino.cost.StatsCalculator;
import io.trino.execution.QueryPreparer;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.spi.security.GroupProvider;
import io.trino.sql.analyzer.QueryExplainer;
import io.trino.sql.parser.SqlParser;
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
import java.util.Optional;

import static io.trino.sql.QueryUtil.singleValueQuery;
import static io.trino.sql.tree.ExplainFormat.Type.JSON;
import static io.trino.sql.tree.ExplainFormat.Type.TEXT;
import static io.trino.sql.tree.ExplainType.Type.DISTRIBUTED;
import static io.trino.sql.tree.ExplainType.Type.IO;
import static io.trino.sql.tree.ExplainType.Type.VALIDATE;
import static java.util.Objects.requireNonNull;

final class ExplainRewrite
        implements StatementRewrite.Rewrite
{
    @Override
    public Statement rewrite(
            Session session,
            Metadata metadata,
            SqlParser parser,
            Optional<QueryExplainer> queryExplainer,
            Statement node,
            List<Expression> parameter,
            Map<NodeRef<Parameter>, Expression> parameterLookup,
            GroupProvider groupProvider,
            AccessControl accessControl,
            WarningCollector warningCollector,
            StatsCalculator statsCalculator)
    {
        return (Statement) new Visitor(session, parser, queryExplainer, warningCollector).process(node, null);
    }

    private static final class Visitor
            extends AstVisitor<Node, Void>
    {
        private final Session session;
        private final QueryPreparer queryPreparer;
        private final Optional<QueryExplainer> queryExplainer;
        private final WarningCollector warningCollector;

        public Visitor(
                Session session,
                SqlParser parser,
                Optional<QueryExplainer> queryExplainer,
                WarningCollector warningCollector)
        {
            this.session = requireNonNull(session, "session is null");
            this.queryPreparer = new QueryPreparer(requireNonNull(parser, "parser is null"));
            this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
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
                queryExplainer.get().analyze(session, preparedQuery.getStatement(), preparedQuery.getParameters(), warningCollector);
                return singleValueQuery("Valid", true);
            }

            String plan;
            switch (planFormat) {
                case GRAPHVIZ:
                    plan = queryExplainer.get().getGraphvizPlan(session, preparedQuery.getStatement(), planType, preparedQuery.getParameters(), warningCollector);
                    break;
                case JSON:
                    plan = queryExplainer.get().getJsonPlan(session, preparedQuery.getStatement(), planType, preparedQuery.getParameters(), warningCollector);
                    break;
                case TEXT:
                    plan = queryExplainer.get().getPlan(session, preparedQuery.getStatement(), planType, preparedQuery.getParameters(), warningCollector);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid Explain Format: " + planFormat.toString());
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
