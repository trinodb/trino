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
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.MaterializedViewDefinition;
import io.trino.metadata.MaterializedViewPropertyManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ViewColumn;
import io.trino.security.AccessControl;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.CreateMaterializedView;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.metadata.MetadataUtil.getRequiredCatalogHandle;
import static io.trino.sql.ParameterUtils.parameterExtractor;
import static io.trino.sql.SqlFormatterUtil.getFormattedSql;
import static java.util.Objects.requireNonNull;

public class CreateMaterializedViewTask
        implements DataDefinitionTask<CreateMaterializedView>
{
    private final PlannerContext plannerContext;
    private final AccessControl accessControl;
    private final SqlParser sqlParser;
    private final AnalyzerFactory analyzerFactory;
    private final MaterializedViewPropertyManager materializedViewPropertyManager;
    private final boolean disableSetPropertiesSecurityCheckForCreateDdl;

    @Inject
    public CreateMaterializedViewTask(
            PlannerContext plannerContext,
            AccessControl accessControl,
            SqlParser sqlParser,
            AnalyzerFactory analyzerFactory,
            MaterializedViewPropertyManager materializedViewPropertyManager,
            FeaturesConfig featuresConfig)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.analyzerFactory = requireNonNull(analyzerFactory, "analyzerFactory is null");
        this.materializedViewPropertyManager = requireNonNull(materializedViewPropertyManager, "materializedViewPropertyManager is null");
        this.disableSetPropertiesSecurityCheckForCreateDdl = featuresConfig.isDisableSetPropertiesSecurityCheckForCreateDdl();
    }

    @Override
    public String getName()
    {
        return "CREATE MATERIALIZED VIEW";
    }

    @Override
    public ListenableFuture<Void> execute(
            CreateMaterializedView statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName name = createQualifiedObjectName(session, statement, statement.getName());
        Map<NodeRef<Parameter>, Expression> parameterLookup = parameterExtractor(statement, parameters);

        String sql = getFormattedSql(statement.getQuery(), sqlParser);

        Analysis analysis = analyzerFactory.createAnalyzer(session, parameters, parameterLookup, stateMachine.getWarningCollector())
                .analyze(statement);

        List<ViewColumn> columns = analysis.getOutputDescriptor(statement.getQuery())
                .getVisibleFields().stream()
                .map(field -> new ViewColumn(field.getName().get(), field.getType().getTypeId()))
                .collect(toImmutableList());

        CatalogName catalogName = getRequiredCatalogHandle(plannerContext.getMetadata(), session, statement, name.getCatalogName());

        Map<String, Object> properties = materializedViewPropertyManager.getProperties(
                catalogName,
                statement.getProperties(),
                session,
                plannerContext,
                accessControl,
                parameterLookup,
                true);

        MaterializedViewDefinition definition = new MaterializedViewDefinition(
                sql,
                session.getCatalog(),
                session.getSchema(),
                columns,
                statement.getComment(),
                session.getIdentity(),
                Optional.empty(),
                properties);

        if (!disableSetPropertiesSecurityCheckForCreateDdl) {
            accessControl.checkCanCreateMaterializedView(session.toSecurityContext(), name, properties);
        }
        else {
            accessControl.checkCanCreateMaterializedView(session.toSecurityContext(), name);
        }
        plannerContext.getMetadata().createMaterializedView(session, name, definition, statement.isReplace(), statement.isNotExists());

        stateMachine.setOutput(analysis.getTarget());
        stateMachine.setReferencedTables(analysis.getReferencedTables());

        return immediateVoidFuture();
    }
}
