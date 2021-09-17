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

import com.google.common.base.VerifyException;
import com.google.common.collect.PeekingIterator;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.spi.connector.ConstantProperty;
import io.trino.spi.connector.LocalProperty;
import io.trino.spi.connector.SortingProperty;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.sanity.PlanSanityChecker.Checker;

import static com.google.common.collect.Iterators.peekingIterator;
import static io.trino.sql.planner.optimizations.LocalProperties.normalizeAndPrune;
import static io.trino.sql.planner.optimizations.StreamPropertyDerivations.derivePropertiesRecursively;
import static java.lang.String.format;

/**
 * Verifies that input of order-sensitive Limit is ordered by the pre-sorted symbols
 */
public class ValidateLimitWithPresortedInput
        implements Checker
{
    @Override
    public void validate(PlanNode planNode,
            Session session,
            Metadata metadata,
            TypeOperators typeOperators,
            TypeAnalyzer typeAnalyzer,
            TypeProvider types,
            WarningCollector warningCollector)
    {
        planNode.accept(new Visitor(session, metadata, typeOperators, typeAnalyzer, types), null);
    }

    private static final class Visitor
            extends PlanVisitor<Void, Void>
    {
        private final Session session;
        private final Metadata metadata;
        private final TypeOperators typeOperators;
        private final TypeAnalyzer typeAnalyzer;
        private final TypeProvider types;

        private Visitor(Session session,
                Metadata metadata,
                TypeOperators typeOperators,
                TypeAnalyzer typeAnalyzer,
                TypeProvider types)
        {
            this.session = session;
            this.metadata = metadata;
            this.typeOperators = typeOperators;
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
        public Void visitLimit(LimitNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            if (node.getPreSortedInputs().isEmpty()) {
                return null;
            }

            StreamProperties properties = derivePropertiesRecursively(node.getSource(), metadata, typeOperators, session, types, typeAnalyzer);

            // We do not use LocalProperties#match to fully verify sorting properties
            // as OrderingScheme of input is not tracked by LimitNode
            PeekingIterator<Symbol> expected = peekingIterator(node.getPreSortedInputs().iterator());

            for (LocalProperty<Symbol> property : normalizeAndPrune(properties.getLocalProperties())) {
                if (!expected.hasNext()) {
                    // all properties satisfied
                    break;
                }

                Symbol column = expected.peek();
                if (property instanceof SortingProperty && ((SortingProperty<Symbol>) property).getColumn().equals(column) ||
                        property instanceof ConstantProperty && ((ConstantProperty<Symbol>) property).getColumn().equals(column)) {
                    expected.next();
                    continue;
                }

                if (property instanceof ConstantProperty) {
                    // unrelated constant columns don't matter for the purpose of satisfying sorting requirements
                    continue;
                }

                break;
            }

            if (expected.hasNext()) {
                throw new VerifyException(format("Expected Limit input to be sorted by: %s, but was %s", node.getPreSortedInputs(), properties.getLocalProperties()));
            }

            return null;
        }
    }
}
