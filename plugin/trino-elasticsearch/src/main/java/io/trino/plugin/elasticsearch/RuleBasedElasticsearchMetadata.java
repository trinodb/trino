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
package io.trino.plugin.elasticsearch;

import com.google.inject.Inject;
import io.trino.plugin.base.expression.ConnectorExpressions;
import io.trino.plugin.elasticsearch.client.ElasticsearchClient;
import io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.TypeManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.elasticsearch.ElasticsearchRemotePredicateTranslator.canonicalize;
import static io.trino.plugin.elasticsearch.ElasticsearchRemotePredicateTranslator.combine;
import static io.trino.plugin.elasticsearch.ElasticsearchRemotePredicateTranslator.withRemotePredicate;
import static io.trino.plugin.elasticsearch.ElasticsearchSessionProperties.getFullTextPushdownMode;

/**
 * Rule-based Elasticsearch metadata facade.
 *
 * <p>P0 predicate recognition is planned directly into {@link ElasticsearchRemotePredicate}. The legacy metadata
 * implementation remains behind the facade only as a compatibility fallback for predicates that this planner does
 * not own. Any legacy predicate state produced by that fallback is immediately canonicalized into the same IR.</p>
 */
public class RuleBasedElasticsearchMetadata
        extends CasePreservingElasticsearchMetadata
{
    @Inject
    public RuleBasedElasticsearchMetadata(TypeManager typeManager, ElasticsearchClient client, ElasticsearchConfig config)
    {
        super(typeManager, client, config);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint constraint)
    {
        ElasticsearchTableHandle input = (ElasticsearchTableHandle) table;
        ElasticsearchPredicatePushdownPlanner.Result predicatePlan = ElasticsearchPredicatePushdownPlanner.plan(
                session,
                constraint,
                getFullTextPushdownMode(session));
        Constraint preparedConstraint = predicatePlan.remainingConstraint();

        Optional<ElasticsearchRemotePredicate> inheritedPredicate = combine(input.remotePredicate(), predicatePlan.remotePredicate());
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> legacyResult = super.applyFilter(session, table, preparedConstraint);

        if (legacyResult.isPresent()) {
            ConstraintApplicationResult<ConnectorTableHandle> result = legacyResult.orElseThrow();
            ElasticsearchTableHandle canonicalHandle = canonicalize((ElasticsearchTableHandle) result.getHandle(), inheritedPredicate);
            ConnectorExpression remainingExpression = appendResidualExpressions(
                    result.getRemainingExpression().orElse(preparedConstraint.getExpression()),
                    predicatePlan.residualExpressions());
            if (canonicalHandle.equals(input)) {
                return Optional.empty();
            }
            return Optional.of(new ConstraintApplicationResult<>(
                    canonicalHandle,
                    result.getRemainingFilter().intersect(predicatePlan.residualFilter()),
                    remainingExpression,
                    result.isPrecalculateStatistics()));
        }

        if (predicatePlan.remotePredicate().isEmpty()) {
            return Optional.empty();
        }

        ElasticsearchTableHandle rewrittenHandle = withRemotePredicate(input, inheritedPredicate);
        if (rewrittenHandle.equals(input)) {
            return Optional.empty();
        }
        return Optional.of(new ConstraintApplicationResult<>(
                rewrittenHandle,
                preparedConstraint.getSummary().intersect(predicatePlan.residualFilter()),
                appendResidualExpressions(preparedConstraint.getExpression(), predicatePlan.residualExpressions()),
                false));
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle table, long limit)
    {
        ElasticsearchTableHandle input = (ElasticsearchTableHandle) table;
        return super.applyLimit(session, table, limit)
                .map(result -> new LimitApplicationResult<>(
                        withRemotePredicate((ElasticsearchTableHandle) result.getHandle(), input.remotePredicate()),
                        result.isLimitGuaranteed(),
                        result.isPrecalculateStatistics()));
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        ElasticsearchTableHandle input = (ElasticsearchTableHandle) table;
        return super.applyAggregation(session, table, aggregates, assignments, groupingSets)
                .map(result -> new AggregationApplicationResult<>(
                        withRemotePredicate((ElasticsearchTableHandle) result.getHandle(), input.remotePredicate()),
                        result.getProjections(),
                        result.getAssignments(),
                        result.getGroupingColumnMapping(),
                        result.isPrecalculateStatistics()));
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;
        if (handle.remotePredicate().isPresent()) {
            // The legacy statistics path does not yet render remotePredicate. Returning no statistics is conservative:
            // an unfiltered estimate would be incorrect and can make the optimizer choose a bad join/order strategy.
            return TableStatistics.empty();
        }
        return super.getTableStatistics(session, table);
    }

    private static ConnectorExpression appendResidualExpressions(
            ConnectorExpression expression,
            List<ConnectorExpression> residualExpressions)
    {
        if (residualExpressions.isEmpty()) {
            return expression;
        }
        List<ConnectorExpression> conjuncts = new ArrayList<>(ConnectorExpressions.extractConjuncts(expression));
        conjuncts.addAll(residualExpressions);
        return ConnectorExpressions.and(conjuncts);
    }
}
