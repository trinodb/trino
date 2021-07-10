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
package io.trino.execution;

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.Session;
import io.trino.cost.StatsCalculator;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.security.AccessControl;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.security.GroupProvider;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Analyzer;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.CreateView;
import io.trino.sql.tree.Expression;
import io.trino.transaction.TransactionManager;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.connector.ConnectorViewDefinition.ViewColumn;
import static io.trino.sql.ParameterUtils.parameterExtractor;
import static io.trino.sql.SqlFormatterUtil.getFormattedSql;
import static io.trino.sql.tree.CreateView.Security.INVOKER;
import static java.util.Objects.requireNonNull;

public class CreateViewTask
        implements DataDefinitionTask<CreateView>
{
    private final SqlParser sqlParser;
    private final GroupProvider groupProvider;
    private final StatsCalculator statsCalculator;

    @Inject
    public CreateViewTask(SqlParser sqlParser, GroupProvider groupProvider, StatsCalculator statsCalculator)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
    }

    @Override
    public String getName()
    {
        return "CREATE VIEW";
    }

    @Override
    public String explain(CreateView statement, List<Expression> parameters)
    {
        return "CREATE VIEW " + statement.getName();
    }

    @Override
    public ListenableFuture<Void> execute(
            CreateView statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName name = createQualifiedObjectName(session, statement, statement.getName());

        accessControl.checkCanCreateView(session.toSecurityContext(), name);

        String sql = getFormattedSql(statement.getQuery(), sqlParser);

        Analysis analysis = new Analyzer(session, metadata, sqlParser, groupProvider, accessControl, Optional.empty(), parameters, parameterExtractor(statement, parameters), stateMachine.getWarningCollector(), statsCalculator)
                .analyze(statement);

        List<ViewColumn> columns = analysis.getOutputDescriptor(statement.getQuery())
                .getVisibleFields().stream()
                .map(field -> new ViewColumn(field.getName().get(), field.getType().getTypeId()))
                .collect(toImmutableList());

        // use DEFINER security by default
        Optional<String> owner = Optional.of(session.getUser());
        if (statement.getSecurity().orElse(null) == INVOKER) {
            owner = Optional.empty();
        }

        ConnectorViewDefinition definition = new ConnectorViewDefinition(
                sql,
                session.getCatalog(),
                session.getSchema(),
                columns,
                statement.getComment(),
                owner,
                owner.isEmpty());

        metadata.createView(session, name, definition, statement.isReplace());

        stateMachine.setOutput(analysis.getTarget());
        stateMachine.setReferencedTables(analysis.getReferencedTables());

        return immediateVoidFuture();
    }
}
