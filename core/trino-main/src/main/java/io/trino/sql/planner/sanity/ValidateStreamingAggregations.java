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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.spi.connector.GroupingProperty;
import io.trino.spi.connector.LocalProperty;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.optimizations.LocalProperties;
import io.trino.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.sanity.PlanSanityChecker.Checker;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.planner.optimizations.StreamPropertyDerivations.derivePropertiesRecursively;

/**
 * Verifies that input of streaming aggregations is grouped on the grouping keys
 */
public class ValidateStreamingAggregations
        implements Checker
{
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

    private static final class Visitor
            extends PlanVisitor<Void, Void>
    {
        private final Session session;
        private final PlannerContext plannerContext;
        private final TypeAnalyzer typeAnalyzer;
        private final TypeProvider types;

        private Visitor(Session session,
                PlannerContext plannerContext,
                TypeAnalyzer typeAnalyzer,
                TypeProvider types)
        {
            this.session = session;
            this.plannerContext = plannerContext;
            this.typeAnalyzer = typeAnalyzer;
            this.types = types;
        }

        @Override
        protected Void visitPlan(PlanNode node, Void context)
        {
            node.getSources().forEach(source -> source.accept(this, context));
            return null;
        }

        @Override
        public Void visitAggregation(AggregationNode node, Void context)
        {
            if (node.getPreGroupedSymbols().isEmpty()) {
                return null;
            }

            StreamProperties properties = derivePropertiesRecursively(node.getSource(), plannerContext, session, types, typeAnalyzer);

            List<LocalProperty<Symbol>> desiredProperties = ImmutableList.of(new GroupingProperty<>(node.getPreGroupedSymbols()));
            Iterator<Optional<LocalProperty<Symbol>>> matchIterator = LocalProperties.match(properties.getLocalProperties(), desiredProperties).iterator();
            Optional<LocalProperty<Symbol>> unsatisfiedRequirement = Iterators.getOnlyElement(matchIterator);
            checkArgument(unsatisfiedRequirement.isEmpty(), "Streaming aggregation with input not grouped on the grouping keys");
            return null;
        }
    }
}
