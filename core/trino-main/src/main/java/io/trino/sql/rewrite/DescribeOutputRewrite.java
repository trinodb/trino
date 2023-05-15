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
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.type.FixedWidthType;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Analyzer;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.analyzer.Field;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.DescribeOutput;
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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.SystemSessionProperties.isOmitDateTimeTypePrecision;
import static io.trino.sql.ParsingUtil.createParsingOptions;
import static io.trino.sql.QueryUtil.aliased;
import static io.trino.sql.QueryUtil.identifier;
import static io.trino.sql.QueryUtil.row;
import static io.trino.sql.QueryUtil.selectList;
import static io.trino.sql.QueryUtil.simpleQuery;
import static io.trino.sql.QueryUtil.values;
import static io.trino.sql.analyzer.QueryType.DESCRIBE;
import static io.trino.type.TypeUtils.getDisplayLabel;
import static java.util.Objects.requireNonNull;

public final class DescribeOutputRewrite
        implements StatementRewrite.Rewrite
{
    private final SqlParser parser;

    @Inject
    public DescribeOutputRewrite(SqlParser parser)
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
            WarningCollector warningCollector,
            PlanOptimizersStatsCollector planOptimizersStatsCollector)
    {
        return (Statement) new Visitor(session, parser, analyzerFactory, parameters, parameterLookup, warningCollector, planOptimizersStatsCollector).process(node, null);
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
        private final PlanOptimizersStatsCollector planOptimizersStatsCollector;

        public Visitor(
                Session session,
                SqlParser parser,
                AnalyzerFactory analyzerFactory,
                List<Expression> parameters,
                Map<NodeRef<Parameter>, Expression> parameterLookup,
                WarningCollector warningCollector,
                PlanOptimizersStatsCollector planOptimizersStatsCollector)
        {
            this.session = requireNonNull(session, "session is null");
            this.parser = requireNonNull(parser, "parser is null");
            this.analyzerFactory = analyzerFactory;
            this.parameters = parameters;
            this.parameterLookup = parameterLookup;
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
            this.planOptimizersStatsCollector = requireNonNull(planOptimizersStatsCollector, "planOptimizersStatsCollector is null");
        }

        @Override
        protected Node visitDescribeOutput(DescribeOutput node, Void context)
        {
            String sqlString = session.getPreparedStatement(node.getName().getValue());
            Statement statement = parser.createStatement(sqlString, createParsingOptions(session));

            Analyzer analyzer = analyzerFactory.createAnalyzer(session, parameters, parameterLookup, warningCollector, planOptimizersStatsCollector);
            Analysis analysis = analyzer.analyze(statement, DESCRIBE);

            Optional<Node> limit = Optional.empty();
            Row[] rows = analysis.getRootScope().getRelationType().getVisibleFields().stream().map(field -> createDescribeOutputRow(field, analysis)).toArray(Row[]::new);
            if (rows.length == 0) {
                NullLiteral nullLiteral = new NullLiteral();
                rows = new Row[] {row(nullLiteral, nullLiteral, nullLiteral, nullLiteral, nullLiteral, nullLiteral, nullLiteral)};
                limit = Optional.of(new Limit(new LongLiteral("0")));
            }
            return simpleQuery(
                    selectList(
                            identifier("Column Name"),
                            identifier("Catalog"),
                            identifier("Schema"),
                            identifier("Table"),
                            identifier("Type"),
                            identifier("Type Size"),
                            identifier("Aliased")),
                    aliased(
                            values(rows),
                            "Statement Output",
                            ImmutableList.of("Column Name", "Catalog", "Schema", "Table", "Type", "Type Size", "Aliased")),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    limit);
        }

        private Row createDescribeOutputRow(Field field, Analysis analysis)
        {
            LongLiteral typeSize = new LongLiteral("0");
            if (field.getType() instanceof FixedWidthType) {
                typeSize = new LongLiteral(String.valueOf(((FixedWidthType) field.getType()).getFixedSize()));
            }

            String columnName;
            if (field.getName().isPresent()) {
                columnName = field.getName().get();
            }
            else {
                int columnIndex = ImmutableList.copyOf(analysis.getOutputDescriptor().getVisibleFields()).indexOf(field);
                columnName = "_col" + columnIndex;
            }

            Optional<QualifiedObjectName> originTable = field.getOriginTable();

            return row(
                    new StringLiteral(columnName),
                    new StringLiteral(originTable.map(QualifiedObjectName::getCatalogName).orElse("")),
                    new StringLiteral(originTable.map(QualifiedObjectName::getSchemaName).orElse("")),
                    new StringLiteral(originTable.map(QualifiedObjectName::getObjectName).orElse("")),
                    new StringLiteral(getDisplayLabel(field.getType(), isOmitDateTimeTypePrecision(session))),
                    typeSize,
                    new BooleanLiteral(String.valueOf(field.isAliased())));
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }
    }
}
