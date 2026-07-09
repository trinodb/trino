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

import com.google.common.collect.ListMultimap;
import com.google.common.graph.SuccessorsFunction;
import com.google.common.graph.Traverser;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.type.FunctionType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.ExpressionExtractor;
import io.trino.sql.planner.SimplePlanVisitor;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.type.TypeCoercion;
import io.trino.type.UnknownType;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.ir.Cast.Kind.CONVERT;
import static io.trino.sql.ir.Cast.Kind.REINTERPRET;

/**
 * Ensures that all the expressions and FunctionCalls matches their output symbols
 */
public final class TypeValidator
        implements PlanSanityChecker.Checker
{
    @Override
    public void validate(
            PlanNode plan,
            Session session,
            PlannerContext plannerContext,
            WarningCollector warningCollector)
    {
        plan.accept(new Visitor(), null);

        TypeCoercion typeCoercion = new TypeCoercion(plannerContext.getTypeManager()::getType);
        for (Expression expression : ExpressionExtractor.extractExpressions(plan)) {
            for (Expression node : Traverser.forTree((SuccessorsFunction<Expression>) Expression::children).depthFirstPreOrder(expression)) {
                if (node instanceof Cast cast) {
                    Cast.Kind expected = typeCoercion.isTypeOnlyCoercion(cast.expression().type(), cast.type()) ? REINTERPRET : CONVERT;
                    checkArgument(cast.kind() == expected, "Cast from %s to %s must have kind %s but was %s", cast.expression().type(), cast.type(), expected, cast.kind());
                }
            }
        }
    }

    private static class Visitor
            extends SimplePlanVisitor<Void>
    {
        @Override
        public Void visitAggregation(AggregationNode node, Void context)
        {
            visitPlan(node, context);

            AggregationNode.Step step = node.getStep();

            for (Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();
                Aggregation aggregation = entry.getValue();
                switch (step) {
                    case SINGLE -> {
                        checkSignature(symbol, aggregation.getResolvedFunction().signature());
                        checkCall(symbol, aggregation.getResolvedFunction().signature(), aggregation.getArguments());
                    }
                    case FINAL -> checkSignature(symbol, aggregation.getResolvedFunction().signature());
                    case PARTIAL, INTERMEDIATE -> {
                        // TODO
                    }
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

            for (Entry<Symbol, Expression> entry : node.getAssignments().entrySet()) {
                Type expectedType = entry.getKey().type();
                if (entry.getValue() instanceof Reference reference) {
                    Symbol symbol = Symbol.from(reference);
                    verifyTypeDescriptor(entry.getKey(), expectedType, symbol.type());
                    continue;
                }
                Type actualType = entry.getValue().type();
                verifyTypeDescriptor(entry.getKey(), expectedType, actualType);
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
                Type expectedType = keySymbol.type();
                for (Symbol valueSymbol : valueSymbols) {
                    verifyTypeDescriptor(keySymbol, expectedType, valueSymbol.type());
                }
            }

            return null;
        }

        private void checkWindowFunctions(Map<Symbol, WindowNode.Function> functions)
        {
            functions.forEach((symbol, function) -> {
                checkSignature(symbol, function.getResolvedFunction().signature());
                checkCall(symbol, function.getResolvedFunction().signature(), function.getArguments());
            });
        }

        private void checkSignature(Symbol symbol, BoundSignature signature)
        {
            Type expectedType = symbol.type();
            Type actualType = signature.getReturnType();
            verifyTypeDescriptor(symbol, expectedType, actualType);
        }

        private void checkCall(Symbol symbol, BoundSignature signature, List<Expression> arguments)
        {
            Type expectedType = symbol.type();
            Type actualType = signature.getReturnType();
            verifyTypeDescriptor(symbol, expectedType, actualType);

            checkArgument(signature.getArgumentTypes().size() == arguments.size(),
                    "expected %s arguments, but found %s arguments",
                    signature.getArgumentTypes().size(),
                    arguments.size());

            for (int i = 0; i < arguments.size(); i++) {
                Type expectedTypeDescriptor = signature.getArgumentTypes().get(i);
                if (expectedTypeDescriptor instanceof FunctionType) {
                    continue;
                }
                Type actualTypeDescriptor = arguments.get(i).type();
                verifyTypeDescriptor(symbol, expectedTypeDescriptor, actualTypeDescriptor);
            }
        }

        private void verifyTypeDescriptor(Symbol symbol, Type expected, Type actual)
        {
            if (actual instanceof RowType actualRowType && expected instanceof RowType expectedRowType) {
                // ignore the field names when comparing row types -- TODO: maybe we should be more strict about this and require they match
                List<Type> actualFieldTypes = actualRowType.getFields().stream()
                        .map(RowType.Field::getType)
                        .toList();

                List<Type> expectedFieldType = expectedRowType.getFields().stream()
                        .map(RowType.Field::getType)
                        .toList();

                checkArgument(expectedFieldType.equals(actualFieldTypes), "type of symbol '%s' is expected to be %s, but the actual type is %s", symbol.name(), expected, actual);
            }
            else if (!(actual instanceof UnknownType)) { // UNKNOWN should be considered as a wildcard type, which matches all the other types
                checkArgument(expected.equals(actual), "type of symbol '%s' is expected to be %s, but the actual type is %s", symbol.name(), expected, actual);
            }
        }
    }
}
