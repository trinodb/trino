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

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.spi.type.Type;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Analyzer;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.DescribeInput;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Limit;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.SystemSessionProperties.isOmitDateTimeTypePrecision;
import static io.trino.execution.ParameterExtractor.extractParameters;
import static io.trino.sql.ParsingUtil.createParsingOptions;
import static io.trino.sql.QueryUtil.aliased;
import static io.trino.sql.QueryUtil.ascending;
import static io.trino.sql.QueryUtil.identifier;
import static io.trino.sql.QueryUtil.ordering;
import static io.trino.sql.QueryUtil.row;
import static io.trino.sql.QueryUtil.selectList;
import static io.trino.sql.QueryUtil.simpleQuery;
import static io.trino.sql.QueryUtil.values;
import static io.trino.sql.analyzer.QueryType.DESCRIBE;
import static io.trino.type.TypeUtils.getDisplayLabel;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.util.Objects.requireNonNull;

public final class DescribeInputRewrite
        implements StatementRewrite.Rewrite
{
    private final SqlParser parser;

    @Inject
    public DescribeInputRewrite(SqlParser parser)
    {
        this.parser = requireNonNull(parser, "parser is null");
    }

    @Override
    public Statement rewrite(
            AnalyzerFactory analyzerFactory,
            Session session,
            Statement node,
            List<Expression> parameters,
            Map<NodeRef<Parameter>, Expression> parameterLookup,
            WarningCollector warningCollector)
    {
        return (Statement) new Visitor(session, parser, analyzerFactory, parameters, parameterLookup, warningCollector).process(node, null);
    }

    private static final class Visitor
            extends AstVisitor<Node, Void>
    {
        private final Session session;
        private final SqlParser parser;
        private final AnalyzerFactory analyzerFactory;
        private final List<Expression> parameters;
        private final Map<NodeRef<Parameter>, Expression> parameterLookup;
        private final WarningCollector warningCollector;

        public Visitor(
                Session session,
                SqlParser parser,
                AnalyzerFactory analyzerFactory,
                List<Expression> parameters,
                Map<NodeRef<Parameter>, Expression> parameterLookup,
                WarningCollector warningCollector)
        {
            this.session = requireNonNull(session, "session is null");
            this.parser = requireNonNull(parser, "parser is null");
            this.analyzerFactory = requireNonNull(analyzerFactory, "analyzerFactory is null");
            this.parameters = parameters;
            this.parameterLookup = parameterLookup;
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        }

        @Override
        protected Node visitDescribeInput(DescribeInput node, Void context)
        {
            String sqlString = session.getPreparedStatement(node.getName().getValue());
            Statement statement = parser.createStatement(sqlString, createParsingOptions(session));

            // create  analysis for the query we are describing.
            Analyzer analyzer = analyzerFactory.createAnalyzer(session, parameters, parameterLookup, warningCollector);
            Analysis analysis = analyzer.analyze(statement, DESCRIBE);

            // get all parameters in query
            List<Parameter> parameters = extractParameters(statement);

            ImmutableList.Builder<Row> builder = ImmutableList.builder();
            for (int i = 0; i < parameters.size(); i++) {
                builder.add(createDescribeInputRow(session, i, parameters.get(i), analysis));
            }

            // return the positions and types of all parameters
            Row[] rows = builder.build().toArray(Row[]::new);
            Optional<Node> limit = Optional.empty();
            if (rows.length == 0) {
                rows = new Row[] {row(new NullLiteral(), new NullLiteral())};
                limit = Optional.of(new Limit(new LongLiteral("0")));
            }

            return simpleQuery(
                    selectList(identifier("Position"), identifier("Type")),
                    aliased(
                            values(rows),
                            "Parameter Input",
                            ImmutableList.of("Position", "Type")),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(ordering(ascending("Position"))),
                    Optional.empty(),
                    limit);
        }

        private static Row createDescribeInputRow(Session session, int position, Parameter parameter, Analysis queryAnalysis)
        {
            Type type = queryAnalysis.getCoercion(parameter);
            if (type == null) {
                type = UNKNOWN;
            }

            return row(
                    new LongLiteral(Integer.toString(position)),
                    new StringLiteral(getDisplayLabel(type, isOmitDateTimeTypePrecision(session))));
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }
    }
}
