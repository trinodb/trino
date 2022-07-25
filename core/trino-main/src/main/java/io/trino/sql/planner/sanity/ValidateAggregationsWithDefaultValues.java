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
package io.trino.sql.planner.sanity;

import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.optimizations.ActualProperties;
import io.trino.sql.planner.optimizations.PropertyDerivations;
import io.trino.sql.planner.optimizations.StreamPropertyDerivations;
import io.trino.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.sanity.PlanSanityChecker.Checker;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.INTERMEDIATE;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.util.Optionals.combine;
import static java.util.Objects.requireNonNull;

/**
 * When an aggregation has an empty grouping set then a default value needs to be returned in the output (e.g: 0 for COUNT(*)).
 * In case if the aggregation is split into FINAL and PARTIAL, then default values are produced by PARTIAL
 * aggregations. In order for the default values not to be duplicated, FINAL aggregation needs to be
 * separated from PARTIAL aggregation by a remote repartition exchange or the FINAL aggregation needs to be executed
 * on a single node. In case both FINAL and PARTIAL aggregations are executed on a single node, then those need to separated
 * by a local repartition exchange or the FINAL aggregation needs to be executed in a single thread.
 */
public class ValidateAggregationsWithDefaultValues
        implements Checker
{
    private final boolean forceSingleNode;

    public ValidateAggregationsWithDefaultValues(boolean forceSingleNode)
    {
        this.forceSingleNode = forceSingleNode;
    }

    @Override
    public void validate(PlanNode planNode,
            Session session,
            PlannerContext plannerContext,
            TypeAnalyzer typeAnalyzer,
            TypeProvider types,
            WarningCollector warningCollector)
    {
        planNode.accept(new Visitor(session, plannerContext, typeAnalyzer, types), null);
    }

    private class Visitor
            extends PlanVisitor<Optional<SeenExchanges>, Void>
    {
        final Session session;
        final PlannerContext plannerContext;
        final TypeAnalyzer typeAnalyzer;
        final TypeProvider types;

        Visitor(Session session, PlannerContext plannerContext, TypeAnalyzer typeAnalyzer, TypeProvider types)
        {
            this.session = requireNonNull(session, "session is null");
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
            this.types = requireNonNull(types, "types is null");
        }

        @Override
        protected Optional<SeenExchanges> visitPlan(PlanNode node, Void context)
        {
            return aggregatedSeenExchanges(node.getSources());
        }

        @Override
        public Optional<SeenExchanges> visitAggregation(AggregationNode node, Void context)
        {
            Optional<SeenExchanges> seenExchangesOptional = aggregatedSeenExchanges(node.getSources());

            if (node.getStep() == PARTIAL) {
                return Optional.of(new SeenExchanges(false, false));
            }

            if (node.getStep() == INTERMEDIATE) {
                return seenExchangesOptional;
            }

            // We only validate FINAL aggregations with empty grouping set
            if (node.getStep() != FINAL || !node.hasEmptyGroupingSet()) {
                return Optional.empty();
            }

            checkState(seenExchangesOptional.isPresent(), "No partial aggregation below final aggregation");
            SeenExchanges seenExchanges = seenExchangesOptional.get();

            if (seenExchanges.remoteRepartitionExchange) {
                // Final aggregation separated from partial by remote repartition exchange.
                return Optional.empty();
            }

            // No remote repartition exchange between final and partial aggregation.
            // Make sure that final aggregation operators are executed on a single node.
            ActualProperties globalProperties = PropertyDerivations.derivePropertiesRecursively(node, plannerContext, session, types, typeAnalyzer);
            checkArgument(forceSingleNode || globalProperties.isSingleNode(),
                    "Final aggregation with default value not separated from partial aggregation by remote hash exchange");

            if (!seenExchanges.localRepartitionExchange) {
                // No local repartition exchange between final and partial aggregation.
                // Make sure that final aggregation operators are executed by single thread.
                StreamProperties localProperties = StreamPropertyDerivations.derivePropertiesRecursively(node, plannerContext, session, types, typeAnalyzer);
                checkArgument(localProperties.isSingleStream(),
                        "Final aggregation with default value not separated from partial aggregation by local hash exchange");
            }

            return Optional.empty();
        }

        @Override
        public Optional<SeenExchanges> visitExchange(ExchangeNode node, Void context)
        {
            Optional<SeenExchanges> seenExchangesOptional = aggregatedSeenExchanges(node.getSources());
            if (seenExchangesOptional.isEmpty()) {
                // No partial aggregation below
                return Optional.empty();
            }

            if (node.getType() != REPARTITION) {
                return seenExchangesOptional;
            }

            SeenExchanges seenExchanges = seenExchangesOptional.get();
            if (node.getScope() == REMOTE) {
                return Optional.of(new SeenExchanges(false, true));
            }

            return Optional.of(new SeenExchanges(true, seenExchanges.remoteRepartitionExchange));
        }

        private Optional<SeenExchanges> aggregatedSeenExchanges(List<PlanNode> nodes)
        {
            return nodes.stream()
                    .map(source -> source.accept(this, null))
                    .reduce((accumulatorOptional, seenExchangesOptional) -> combine(accumulatorOptional, seenExchangesOptional,
                            (accumulator, seenExchanges) -> new SeenExchanges(
                                    accumulator.localRepartitionExchange && seenExchanges.localRepartitionExchange,
                                    accumulator.remoteRepartitionExchange && seenExchanges.remoteRepartitionExchange)))
                    .orElse(Optional.empty());
        }
    }

    private static class SeenExchanges
    {
        final boolean localRepartitionExchange;
        final boolean remoteRepartitionExchange;

        SeenExchanges(boolean localRepartitionExchange, boolean remoteRepartitionExchange)
        {
            this.localRepartitionExchange = localRepartitionExchange;
            this.remoteRepartitionExchange = remoteRepartitionExchange;
        }
    }
}
