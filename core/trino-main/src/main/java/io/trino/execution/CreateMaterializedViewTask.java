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
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.MaterializedViewDefinition;
import io.trino.metadata.MaterializedViewPropertyManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ViewColumn;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.CreateMaterializedView;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.execution.ParameterExtractor.bindParameters;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.metadata.MetadataUtil.getRequiredCatalogHandle;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.connector.ConnectorCapabilities.MATERIALIZED_VIEW_GRACE_PERIOD;
import static io.trino.sql.SqlFormatterUtil.getFormattedSql;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class CreateMaterializedViewTask
        implements DataDefinitionTask<CreateMaterializedView>
{
    private final PlannerContext plannerContext;
    private final AccessControl accessControl;
    private final SqlParser sqlParser;
    private final AnalyzerFactory analyzerFactory;
    private final MaterializedViewPropertyManager materializedViewPropertyManager;

    @Inject
    public CreateMaterializedViewTask(
            PlannerContext plannerContext,
            AccessControl accessControl,
            SqlParser sqlParser,
            AnalyzerFactory analyzerFactory,
            MaterializedViewPropertyManager materializedViewPropertyManager)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.analyzerFactory = requireNonNull(analyzerFactory, "analyzerFactory is null");
        this.materializedViewPropertyManager = requireNonNull(materializedViewPropertyManager, "materializedViewPropertyManager is null");
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
        Map<NodeRef<Parameter>, Expression> parameterLookup = bindParameters(statement, parameters);

        String sql = getFormattedSql(statement.getQuery(), sqlParser);

        Analysis analysis = analyzerFactory.createAnalyzer(session, parameters, parameterLookup, stateMachine.getWarningCollector(), stateMachine.getPlanOptimizersStatsCollector())
                .analyze(statement);

        List<ViewColumn> columns = analysis.getOutputDescriptor(statement.getQuery())
                .getVisibleFields().stream()
                .map(field -> new ViewColumn(field.getName().get(), field.getType().getTypeId(), Optional.empty()))
                .collect(toImmutableList());

        String catalogName = name.getCatalogName();
        CatalogHandle catalogHandle = getRequiredCatalogHandle(plannerContext.getMetadata(), session, statement, catalogName);

        Map<String, Object> properties = materializedViewPropertyManager.getProperties(
                catalogName,
                catalogHandle,
                statement.getProperties(),
                session,
                plannerContext,
                accessControl,
                parameterLookup,
                true);

        Optional<Duration> gracePeriod = statement.getGracePeriod()
                .map(expression -> {
                    if (!plannerContext.getMetadata().getConnectorCapabilities(session, catalogHandle).contains(MATERIALIZED_VIEW_GRACE_PERIOD)) {
                        throw semanticException(NOT_SUPPORTED, statement, "Catalog '%s' does not support GRACE PERIOD", catalogName);
                    }

                    Type type = analysis.getType(expression);
                    if (type != INTERVAL_DAY_TIME) {
                        throw new TrinoException(TYPE_MISMATCH, "Unsupported grace period type %s, expected %s".formatted(type.getDisplayName(), INTERVAL_DAY_TIME.getDisplayName()));
                    }
                    Long milliseconds = (Long) evaluateConstantExpression(
                            expression,
                            analysis.getCoercions(),
                            analysis.getTypeOnlyCoercions(),
                            plannerContext,
                            session,
                            accessControl,
                            analysis.getColumnReferences(),
                            parameterLookup);
                    // Sanity check. Impossible per grammar.
                    verify(milliseconds != null, "Grace period cannot be null");
                    return Duration.ofMillis(milliseconds);
                });

        MaterializedViewDefinition definition = new MaterializedViewDefinition(
                sql,
                session.getCatalog(),
                session.getSchema(),
                columns,
                gracePeriod,
                statement.getComment(),
                session.getIdentity(),
                Optional.empty(),
                properties);

        Set<String> specifiedPropertyKeys = statement.getProperties().stream()
                // property names are case-insensitive and normalized to lower case
                .map(property -> property.getName().getValue().toLowerCase(ENGLISH))
                .collect(toImmutableSet());
        Map<String, Object> explicitlySetProperties = properties.keySet().stream()
                .peek(key -> verify(key.equals(key.toLowerCase(ENGLISH)), "Property name '%s' not in lower-case", key))
                .filter(specifiedPropertyKeys::contains)
                .collect(toImmutableMap(Function.identity(), properties::get));
        accessControl.checkCanCreateMaterializedView(session.toSecurityContext(), name, explicitlySetProperties);
        plannerContext.getMetadata().createMaterializedView(session, name, definition, statement.isReplace(), statement.isNotExists());

        stateMachine.setOutput(analysis.getTarget());
        stateMachine.setReferencedTables(analysis.getReferencedTables());

        return immediateVoidFuture();
    }
}
