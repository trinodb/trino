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
import io.trino.connector.CatalogName;
import io.trino.cost.StatsCalculator;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.security.AccessControl;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.security.GroupProvider;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Analyzer;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.CreateMaterializedView;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.transaction.TransactionManager;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.metadata.MetadataUtil.getRequiredCatalogHandle;
import static io.trino.sql.NodeUtils.mapFromProperties;
import static io.trino.sql.ParameterUtils.parameterExtractor;
import static io.trino.sql.SqlFormatterUtil.getFormattedSql;
import static java.util.Objects.requireNonNull;

public class CreateMaterializedViewTask
        implements DataDefinitionTask<CreateMaterializedView>
{
    private final SqlParser sqlParser;
    private final GroupProvider groupProvider;
    private final StatsCalculator statsCalculator;

    @Inject
    public CreateMaterializedViewTask(SqlParser sqlParser, GroupProvider groupProvider, StatsCalculator statsCalculator)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
    }

    @Override
    public String getName()
    {
        return "CREATE MATERIALIZED VIEW";
    }

    @Override
    public String explain(CreateMaterializedView statement, List<Expression> parameters)
    {
        return "CREATE MATERIALIZED VIEW " + statement.getName();
    }

    @Override
    public ListenableFuture<Void> execute(
            CreateMaterializedView statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName name = createQualifiedObjectName(session, statement, statement.getName());
        Map<NodeRef<Parameter>, Expression> parameterLookup = parameterExtractor(statement, parameters);

        String sql = getFormattedSql(statement.getQuery(), sqlParser);

        Analysis analysis = new Analyzer(session, metadata, sqlParser, groupProvider, accessControl, Optional.empty(), parameters, parameterLookup, stateMachine.getWarningCollector(), statsCalculator)
                .analyze(statement);

        List<ConnectorMaterializedViewDefinition.Column> columns = analysis.getOutputDescriptor(statement.getQuery())
                .getVisibleFields().stream()
                .map(field -> new ConnectorMaterializedViewDefinition.Column(field.getName().get(), field.getType().getTypeId()))
                .collect(toImmutableList());

        String owner = session.getUser();

        CatalogName catalogName = getRequiredCatalogHandle(metadata, session, statement, name.getCatalogName());

        Map<String, Expression> sqlProperties = mapFromProperties(statement.getProperties());
        Map<String, Object> properties = metadata.getMaterializedViewPropertyManager().getProperties(
                catalogName,
                name.getCatalogName(),
                sqlProperties,
                session,
                metadata,
                accessControl,
                parameterLookup);

        ConnectorMaterializedViewDefinition definition = new ConnectorMaterializedViewDefinition(
                sql,
                Optional.empty(),
                session.getCatalog(),
                session.getSchema(),
                columns,
                statement.getComment(),
                owner,
                properties);

        metadata.createMaterializedView(session, name, definition, statement.isReplace(), statement.isNotExists());

        stateMachine.setOutput(analysis.getTarget());
        stateMachine.setReferencedTables(analysis.getReferencedTables());

        return immediateVoidFuture();
    }
}
