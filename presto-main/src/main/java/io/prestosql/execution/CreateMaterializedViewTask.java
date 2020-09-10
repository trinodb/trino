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
package io.prestosql.execution;

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.Session;
import io.prestosql.connector.CatalogName;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorMaterializedViewDefinition;
import io.prestosql.sql.analyzer.Analysis;
import io.prestosql.sql.analyzer.Analyzer;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.CreateMaterializedView;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.Parameter;
import io.prestosql.sql.tree.Statement;
import io.prestosql.transaction.TransactionManager;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.sql.NodeUtils.mapFromProperties;
import static io.prestosql.sql.ParameterUtils.parameterExtractor;
import static io.prestosql.sql.SqlFormatterUtil.getFormattedSql;
import static java.util.Objects.requireNonNull;

public class CreateMaterializedViewTask
        implements DataDefinitionTask<CreateMaterializedView>
{
    private final SqlParser sqlParser;

    @Inject
    public CreateMaterializedViewTask(SqlParser sqlParser, FeaturesConfig featuresConfig)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        requireNonNull(featuresConfig, "featuresConfig is null");
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
    public ListenableFuture<?> execute(
            CreateMaterializedView statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName name = createQualifiedObjectName(session, statement, statement.getName());
        Map<NodeRef<Parameter>, Expression> parameterLookup = parameterExtractor(statement, parameters);

        String sql = getFormattedSql(statement.getQuery(), sqlParser);

        Analysis analysis = analyzeStatement(statement, session, metadata, accessControl, parameters, parameterLookup, stateMachine.getWarningCollector());

        List<ConnectorMaterializedViewDefinition.Column> columns = analysis.getOutputDescriptor(statement.getQuery())
                .getVisibleFields().stream()
                .map(field -> new ConnectorMaterializedViewDefinition.Column(field.getName().get(), field.getType().getTypeId()))
                .collect(toImmutableList());

        Optional<String> owner = Optional.of(session.getUser());

        CatalogName catalogName = metadata.getCatalogHandle(session, name.getCatalogName())
                .orElseThrow(() -> new PrestoException(NOT_FOUND, "Catalog does not exist: " + name.getCatalogName()));

        Map<String, Expression> sqlProperties = mapFromProperties(statement.getProperties());
        Map<String, Object> properties = metadata.getTablePropertyManager().getProperties(
                catalogName,
                name.getCatalogName(),
                sqlProperties,
                session,
                metadata,
                accessControl,
                parameterLookup);

        ConnectorMaterializedViewDefinition definition = new ConnectorMaterializedViewDefinition(
                sql,
                null,
                session.getCatalog(),
                session.getSchema(),
                columns,
                statement.getComment(),
                owner,
                properties);

        metadata.createMaterializedView(session, name, definition, statement.isReplace(), statement.isNotExists());

        return immediateFuture(null);
    }

    private Analysis analyzeStatement(
            Statement statement,
            Session session,
            Metadata metadata,
            AccessControl accessControl,
            List<Expression> parameters,
            Map<NodeRef<Parameter>, Expression> parameterLookup,
            WarningCollector warningCollector)
    {
        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, accessControl, Optional.empty(), parameters, parameterLookup, warningCollector);
        return analyzer.analyze(statement);
    }
}
