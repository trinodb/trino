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
package io.trino.sql.analyzer;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.metadata.MaterializedViewDefinition;
import io.trino.metadata.MaterializedViewDependencies;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.security.GroupProvider;
import io.trino.spi.security.Identity;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Query;

import java.util.Optional;

import static io.trino.execution.warnings.WarningCollector.NOOP;
import static io.trino.sql.analyzer.DeterminismEvaluator.containsCurrentTimeFunctions;
import static java.util.Objects.requireNonNull;

/**
 * Determines the freshness of materialized views. When the connector cannot tell the freshness from what it has
 * recorded alone (see {@link MaterializedViewFreshness#requiresDependencies()}), the materialized view query is analyzed
 * to find the relations it depends on, and the connector is asked again with those relations.
 */
public class MaterializedViewFreshnessChecker
{
    private static final Logger log = Logger.get(MaterializedViewFreshnessChecker.class);

    private final Metadata metadata;
    private final StatementAnalyzerFactory statementAnalyzerFactory;
    private final SqlParser sqlParser;
    private final GroupProvider groupProvider;

    MaterializedViewFreshnessChecker(Metadata metadata, StatementAnalyzerFactory statementAnalyzerFactory, SqlParser sqlParser, GroupProvider groupProvider)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.statementAnalyzerFactory = requireNonNull(statementAnalyzerFactory, "statementAnalyzerFactory is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
    }

    public MaterializedViewFreshness getMaterializedViewFreshness(Session session, QualifiedObjectName name, boolean considerGracePeriod)
    {
        MaterializedViewFreshness freshness = metadata.getMaterializedViewFreshness(session, name, considerGracePeriod);
        if (!freshness.requiresDependencies()) {
            return freshness;
        }
        return metadata.getMaterializedView(session, name)
                .flatMap(definition -> getMaterializedViewFreshnessWithDependencies(session, name, definition, considerGracePeriod))
                .orElse(freshness);
    }

    /**
     * Asks the connector for the freshness again, providing the relations the materialized view query depends on.
     * Returns empty when the query cannot be analyzed, in which case the freshness the connector reported before still stands.
     */
    public Optional<MaterializedViewFreshness> getMaterializedViewFreshnessWithDependencies(Session session, QualifiedObjectName name, MaterializedViewDefinition definition, boolean considerGracePeriod)
    {
        return analyzeDependencies(session, name, definition)
                .map(dependencies -> metadata.getMaterializedViewFreshness(session, name, considerGracePeriod, dependencies));
    }

    /**
     * Analyzes the materialized view query, expanding views and materialized views, to find the relations it depends on.
     * Returns empty when the query cannot be analyzed, e.g. because a relation it depends on no longer exists.
     */
    public Optional<MaterializedViewDependencies> analyzeDependencies(Session session, QualifiedObjectName name, MaterializedViewDefinition definition)
    {
        try {
            Query query = (Query) sqlParser.createStatement(definition.getOriginalSql(), TypeDescriptorTranslator::normalizeStoredIntervalType);
            if (!query.getFunctions().isEmpty()) {
                return Optional.empty();
            }

            Analysis analysis = new Analysis(query, ImmutableMap.of(), QueryType.OTHERS);
            MaterializedViewDependencyCollector collector = new MaterializedViewDependencyCollector();
            analysis.setMaterializedViewDependencyCollector(collector);

            // Analyze as the materialized view owner, the same way the query is analyzed when the materialized view is expanded
            Identity identity = definition.getRunAsIdentity()
                    .map(owner -> Identity.from(owner)
                            .withGroups(groupProvider.getGroups(owner.getUser()))
                            .build())
                    .orElse(session.getIdentity());
            Session viewSession = session.createViewSession(definition.getCatalog(), definition.getSchema(), identity, definition.getPath());

            // Only metadata is accessed, and access to the materialized view itself was already checked. Row filters and
            // column masks depend on the user running the query, and are not dependencies of the materialized view.
            statementAnalyzerFactory.withSpecializedAccessControl(new AllowAllAccessControl())
                    .createStatementAnalyzer(analysis, viewSession, NOOP, CorrelationSupport.ALLOWED)
                    .analyze(query);

            // Same as what is recorded when refreshing the materialized view, see LogicalPlanner
            boolean hasTableFunctions = !analysis.getPolymorphicTableFunctions().isEmpty();
            boolean hasNonDeterministicFunctions = analysis.getResolvedFunctions().stream().anyMatch(function -> !function.deterministic())
                    || containsCurrentTimeFunctions(query);
            return Optional.of(new MaterializedViewDependencies(collector.build(), hasTableFunctions, hasNonDeterministicFunctions));
        }
        catch (TrinoException | ParsingException e) {
            log.debug(e, "Failed to analyze dependencies of materialized view %s", name);
            return Optional.empty();
        }
    }
}
