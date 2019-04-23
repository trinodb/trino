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
package io.prestosql.sql.planner.sanity;

import com.google.common.collect.ListMultimap;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.planner.SimplePlanVisitor;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.AggregationNode.Aggregation;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.UnionNode;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.type.TypeCoercion;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static java.util.Objects.requireNonNull;

/**
 * Ensures that all the expressions and FunctionCalls matches their output symbols
 */
public final class TypeValidator
        implements PlanSanityChecker.Checker
{
    public TypeValidator() {}

    @Override
    public void validate(PlanNode plan, Session session, Metadata metadata, TypeAnalyzer typeAnalyzer, TypeProvider types, WarningCollector warningCollector)
    {
        plan.accept(new Visitor(session, metadata, typeAnalyzer, types, warningCollector), null);
    }

    private static class Visitor
            extends SimplePlanVisitor<Void>
    {
        private final Session session;
        private final Metadata metadata;
        private final TypeCoercion typeCoercion;
        private final TypeAnalyzer typeAnalyzer;
        private final TypeProvider types;
        private final WarningCollector warningCollector;

        public Visitor(Session session, Metadata metadata, TypeAnalyzer typeAnalyzer, TypeProvider types, WarningCollector warningCollector)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.typeCoercion = new TypeCoercion(metadata::getType);
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
            this.types = requireNonNull(types, "types is null");
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        }

        @Override
        public Void visitAggregation(AggregationNode node, Void context)
        {
            visitPlan(node, context);

            AggregationNode.Step step = node.getStep();

            for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();
                Aggregation aggregation = entry.getValue();
                switch (step) {
                    case SINGLE:
                        checkSignature(symbol, aggregation.getSignature());
                        checkCall(symbol, aggregation.getSignature().getName(), aggregation.getArguments());
                        break;
                    case FINAL:
                        checkSignature(symbol, aggregation.getSignature());
                        break;
                }
            }

            return null;
        }

        @Override
        public Void visitWindow(WindowNode node, Void context)
        {
            visitPlan(node, context);

            checkWindowFunctions(node.getWindowFunctions());

            return null;
        }

        @Override
        public Void visitProject(ProjectNode node, Void context)
        {
            visitPlan(node, context);

            for (Map.Entry<Symbol, Expression> entry : node.getAssignments().entrySet()) {
                Type expectedType = types.get(entry.getKey());
                if (entry.getValue() instanceof SymbolReference) {
                    SymbolReference symbolReference = (SymbolReference) entry.getValue();
                    verifyTypeSignature(entry.getKey(), expectedType.getTypeSignature(), types.get(Symbol.from(symbolReference)).getTypeSignature());
                    continue;
                }
                Type actualType = typeAnalyzer.getType(session, types, entry.getValue());
                verifyTypeSignature(entry.getKey(), expectedType.getTypeSignature(), actualType.getTypeSignature());
            }

            return null;
        }

        @Override
        public Void visitUnion(UnionNode node, Void context)
        {
            visitPlan(node, context);

            ListMultimap<Symbol, Symbol> symbolMapping = node.getSymbolMapping();
            for (Symbol keySymbol : symbolMapping.keySet()) {
                List<Symbol> valueSymbols = symbolMapping.get(keySymbol);
                Type expectedType = types.get(keySymbol);
                for (Symbol valueSymbol : valueSymbols) {
                    verifyTypeSignature(keySymbol, expectedType.getTypeSignature(), types.get(valueSymbol).getTypeSignature());
                }
            }

            return null;
        }

        private void checkWindowFunctions(Map<Symbol, WindowNode.Function> functions)
        {
            functions.forEach((symbol, function) -> {
                checkSignature(symbol, function.getSignature());
                checkCall(symbol, function.getSignature().getName(), function.getArguments());
            });
        }

        private void checkSignature(Symbol symbol, Signature signature)
        {
            TypeSignature expectedTypeSignature = types.get(symbol).getTypeSignature();
            TypeSignature actualTypeSignature = signature.getReturnType();
            verifyTypeSignature(symbol, expectedTypeSignature, actualTypeSignature);
        }

        private void checkCall(Symbol symbol, FunctionCall call)
        {
            Type expectedType = types.get(symbol);
            Type actualType = typeAnalyzer.getType(session, types, call);
            verifyTypeSignature(symbol, expectedType.getTypeSignature(), actualType.getTypeSignature());
        }

        private void checkCall(Symbol symbol, String name, List<Expression> arguments)
        {
            Type expectedType = types.get(symbol);

            Signature function = metadata.resolveFunction(QualifiedName.of(name), typeAnalyzer.getCallArgumentTypes(session, types, arguments));
            Type actualType = metadata.getType(function.getReturnType());

            verifyTypeSignature(symbol, expectedType.getTypeSignature(), actualType.getTypeSignature());
        }

        private void verifyTypeSignature(Symbol symbol, TypeSignature expected, TypeSignature actual)
        {
            // UNKNOWN should be considered as a wildcard type, which matches all the other types
            if (!actual.equals(UNKNOWN.getTypeSignature()) && !typeCoercion.isTypeOnlyCoercion(metadata.getType(actual), metadata.getType(expected))) {
                checkArgument(expected.equals(actual), "type of symbol '%s' is expected to be %s, but the actual type is %s", symbol, expected, actual);
            }
        }
    }
}
