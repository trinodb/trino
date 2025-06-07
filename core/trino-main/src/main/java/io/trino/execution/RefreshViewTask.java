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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ViewColumn;
import io.trino.metadata.ViewDefinition;
import io.trino.security.AccessControl;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.RefreshView;
import io.trino.sql.tree.Statement;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class RefreshViewTask
        implements DataDefinitionTask<RefreshView>
{
    private final PlannerContext plannerContext;
    private final AccessControl accessControl;
    private final SqlParser sqlParser;
    private final AnalyzerFactory analyzerFactory;

    @Inject
    public RefreshViewTask(
            PlannerContext plannerContext,
            AccessControl accessControl,
            SqlParser sqlParser,
            AnalyzerFactory analyzerFactory)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.analyzerFactory = requireNonNull(analyzerFactory, "analyzerFactory is null");
    }

    @Override
    public String getName()
    {
        return "REFRESH VIEW";
    }

    @Override
    public ListenableFuture<Void> execute(
            RefreshView refreshView,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Metadata metadata = plannerContext.getMetadata();
        Session session = stateMachine.getSession();
        QualifiedObjectName viewName = createQualifiedObjectName(session, refreshView, refreshView.getName());
        if (metadata.isMaterializedView(session, viewName)) {
            throw semanticException(
                    TABLE_NOT_FOUND,
                    refreshView,
                    "View '%s' does not exist, but a materialized view with that name exists. Did you mean ALTER MATERIALIZED VIEW %s RENAME TO ...?", viewName, viewName);
        }

        if (!metadata.isView(session, viewName)) {
            if (metadata.getTableHandle(session, viewName).isPresent()) {
                throw semanticException(
                        TABLE_NOT_FOUND,
                        refreshView,
                        "View '%s' does not exist, but a table with that name exists. Did you mean ALTER TABLE %s RENAME TO ...?", viewName, viewName);
            }

            throw semanticException(TABLE_NOT_FOUND, refreshView, "View '%s' does not exist", viewName);
        }

        accessControl.checkCanRefreshView(session.toSecurityContext(), viewName);

        ViewDefinition viewDefinition = metadata.getView(session, viewName)
                .orElseThrow(() -> semanticException(TABLE_NOT_FOUND, refreshView, "View '%s' not found", viewName));

        Session viewSession = stateMachine.getSession();
        if (!viewDefinition.isRunAsInvoker()) {
            checkArgument(viewDefinition.getRunAsIdentity().isPresent(), "View owner detail is missing");
            viewSession = viewSession.createViewSession(viewDefinition.getCatalog(), viewDefinition.getSchema(), viewDefinition.getRunAsIdentity().get(), viewDefinition.getPath());
        }

        Statement viewDefinitionSql = sqlParser.createStatement(viewDefinition.getOriginalSql());

        Analysis analysis = analyzerFactory.createAnalyzer(viewSession, parameters, ImmutableMap.of(), stateMachine.getWarningCollector(), stateMachine.getPlanOptimizersStatsCollector())
                .analyze(viewDefinitionSql);

        List<ViewColumn> columns = analysis.getOutputDescriptor(viewDefinitionSql)
                .getVisibleFields().stream()
                .map(field -> new ViewColumn(field.getName().get(), field.getType().getTypeId(), Optional.empty()))
                .collect(toImmutableList());

        ViewDefinition viewDefinitionWithNewColumns = new ViewDefinition(
                viewDefinition.getOriginalSql(),
                viewDefinition.getCatalog(),
                viewDefinition.getSchema(),
                columns,
                viewDefinition.getComment(),
                viewDefinition.getRunAsIdentity(),
                viewDefinition.getPath());

        metadata.refreshView(session, viewName, viewDefinitionWithNewColumns);

        return immediateVoidFuture();
    }
}
