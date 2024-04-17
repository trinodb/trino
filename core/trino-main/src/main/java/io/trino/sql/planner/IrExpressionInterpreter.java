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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Array;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Bind;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Comparison.Operator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IrVisitor;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Not;
import io.trino.sql.ir.NullIf;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.Switch;
import io.trino.sql.ir.WhenClause;
import io.trino.type.TypeCoercion;
import io.trino.util.FastutilSetHelper;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.metadata.OperatorNameUtil.mangleOperatorName;
import static io.trino.operator.scalar.JsonStringToArrayCast.JSON_STRING_TO_ARRAY_NAME;
import static io.trino.operator.scalar.JsonStringToMapCast.JSON_STRING_TO_MAP_NAME;
import static io.trino.operator.scalar.JsonStringToRowCast.JSON_STRING_TO_ROW_NAME;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.NEGATION;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.sql.DynamicFilters.isDynamicFilter;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.NULL_BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class IrExpressionInterpreter
{
    private static final CatalogSchemaFunctionName FAIL_NAME = builtinFunctionName("fail");
    private static final MethodHandle LAMBDA_EVALUATOR;

    static {
        try {
            LAMBDA_EVALUATOR = MethodHandles.lookup()
                    .findVirtual(Visitor.class, "evaluate", methodType(Object.class, Expression.class, Map.class, new Object[0].getClass()));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final Expression expression;
    private final PlannerContext plannerContext;
    private final Metadata metadata;
    private final ConnectorSession connectorSession;
    private final InterpretedFunctionInvoker functionInvoker;
    private final TypeCoercion typeCoercion;

    private final IdentityHashMap<List<Expression>, Set<?>> inListCache = new IdentityHashMap<>();

    public IrExpressionInterpreter(Expression expression, PlannerContext plannerContext, Session session)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.metadata = plannerContext.getMetadata();
        this.connectorSession = session.toConnectorSession();
        this.functionInvoker = new InterpretedFunctionInvoker(plannerContext.getFunctionManager());
        this.typeCoercion = new TypeCoercion(plannerContext.getTypeManager()::getType);
    }

    public Object evaluate()
    {
        Expression result = new Visitor(true).processWithExceptionHandling(expression, NoOpSymbolResolver.INSTANCE);
        verify(result instanceof Constant, "Expected constant, but got: " + result);
        return ((Constant) result).value();
    }

    public Expression optimize(SymbolResolver inputs)
    {
        return new Visitor(false).processWithExceptionHandling(expression, inputs);
    }

    public Expression optimize()
    {
        return optimize(NoOpSymbolResolver.INSTANCE);
    }

    private class Visitor
            extends IrVisitor<Expression, SymbolResolver>
    {
        private final boolean evaluate;

        private Visitor(boolean evaluate)
        {
            this.evaluate = evaluate;
        }

        private Expression processWithExceptionHandling(Expression expression, SymbolResolver context)
        {
            try {
                return process(expression, context);
            }
            catch (TrinoException e) {
                if (!evaluate) {
                    // Certain operations like 0 / 0 or likeExpression may throw exceptions.
                    // When optimizing, do not throw the exception, but delay it until the expression is actually executed.
                    // This is to take advantage of the possibility that some other optimization removes the erroneous
                    // expression from the plan.
                    return expression;
                }
                // Do not suppress exceptions during expression execution.
                throw e;
            }
        }

        @Override
        protected Expression visitReference(Reference node, SymbolResolver context)
        {
            Optional<Constant> binding = context.getValue(Symbol.from(node));
            if (binding.isPresent()) {
                return binding.get();
            }

            return node;
        }

        @Override
        protected Expression visitConstant(Constant node, SymbolResolver context)
        {
            return node;
        }

        @Override
        protected Expression visitIsNull(IsNull node, SymbolResolver context)
        {
            Expression value = processWithExceptionHandling(node.value(), context);

            if (value instanceof Constant constant) {
                return new Constant(BOOLEAN, constant.value() == null);
            }

            return new IsNull(value);
        }

        @Override
        protected Expression visitCase(Case node, SymbolResolver context)
        {
            List<Expression> operands = node.whenClauses().stream()
                    .map(WhenClause::getOperand)
                    .map(operand -> processWithExceptionHandling(operand, context))
                    .toList();

            List<WhenClause> newClauses = new ArrayList<>();
            Expression newDefault = null;
            for (int i = 0; i < node.whenClauses().size() && newDefault == null; i++) {
                Expression operand = operands.get(i);

                if (operand instanceof Constant(Type type, Boolean value) && value) {
                    newDefault = processWithExceptionHandling(node.whenClauses().get(i).getResult(), context);
                }
                else if (!(operand instanceof Constant)) {
                    newClauses.add(new WhenClause(operand, processWithExceptionHandling(node.whenClauses().get(i).getResult(), context)));
                }
            }

            if (newDefault == null) {
                newDefault = processWithExceptionHandling(node.defaultValue(), context);
            }

            if (!newClauses.isEmpty()) {
                return new Case(newClauses, newDefault);
            }

            return newDefault;
        }

        @Override
        protected Expression visitSwitch(Switch node, SymbolResolver context)
        {
            ResolvedFunction equals = metadata.resolveOperator(EQUAL, ImmutableList.of(node.operand().type(), node.operand().type()));

            Expression operand = processWithExceptionHandling(node.operand(), context);

            List<Expression> candidates = node.whenClauses().stream()
                    .map(WhenClause::getOperand)
                    .map(candidate -> processWithExceptionHandling(candidate, context))
                    .toList();

            List<WhenClause> newClauses = new ArrayList<>();
            Expression newDefault = null;
            for (int i = 0; i < node.whenClauses().size() && newDefault == null; i++) {
                if (operand instanceof Constant operandConstant && operandConstant.value() != null &&
                        candidates.get(i) instanceof Constant candidateConstant && candidateConstant.value() != null) {
                    if (Boolean.TRUE.equals(functionInvoker.invoke(equals, connectorSession, ImmutableList.of(operandConstant.value(), candidateConstant.value())))) {
                        newDefault = processWithExceptionHandling(node.whenClauses().get(i).getResult(), context);
                    }
                }
                else if (!isConstantNull(operand) && !isConstantNull(candidates.get(i))) {
                    newClauses.add(new WhenClause(candidates.get(i), processWithExceptionHandling(node.whenClauses().get(i).getResult(), context)));
                }
            }

            if (newDefault == null) {
                newDefault = processWithExceptionHandling(node.defaultValue(), context);
            }

            if (!newClauses.isEmpty()) {
                return new Switch(operand, newClauses, newDefault);
            }

            return newDefault;
        }

        @Override
        protected Expression visitCoalesce(Coalesce node, SymbolResolver context)
        {
            Set<Expression> seen = new HashSet<>();
            List<Expression> newOperands = processArguments(node, context, seen);

            if (newOperands.isEmpty()) {
                return new Constant(node.type(), null);
            }

            if (newOperands.size() == 1) {
                return getOnlyElement(newOperands);
            }

            return new Coalesce(newOperands);
        }

        private List<Expression> processArguments(Coalesce node, SymbolResolver context, Set<Expression> seen)
        {
            List<Expression> newOperands = new ArrayList<>();
            for (Expression operand : node.operands()) {
                Expression value = processWithExceptionHandling(operand, context);
                if (value instanceof Constant constant) {
                    if (constant.value() != null) {
                        newOperands.add(value);
                        break;
                    }
                }
                else if (!(value instanceof Coalesce) && !isDeterministic(value) || seen.add(value)) {
                    // Skip duplicates unless they are non-deterministic.
                    newOperands.add(value);
                }
                else if (value instanceof Coalesce inner) {
                    // The nested Coalesce was processed recursively and flattened. It does not contain null.
                    newOperands.addAll(processArguments(inner, context, seen));
                }
            }
            return newOperands;
        }

        @Override
        protected Expression visitIn(In node, SymbolResolver context)
        {
            Expression value = processWithExceptionHandling(node.value(), context);

            if (isConstantNull(value)) {
                return node.valueList().isEmpty() ? FALSE : NULL_BOOLEAN;
            }

            List<Expression> valueList = node.valueList();
            if ((value instanceof Constant(Type type, Object constantValue)) &&
                    !(type instanceof ArrayType) && // equals/hashcode doesn't work for complex types that may contain nulls
                    !(type instanceof MapType) &&
                    !(type instanceof RowType)) {
                Set<?> set = inListCache.get(valueList);

                // We use the presence of the node in the map to indicate that we've already done
                // the analysis below. If the value is null, it means that we can't apply the HashSet
                // optimization
                if (!inListCache.containsKey(valueList)) {
                    boolean nonNullConstants = valueList.stream().allMatch(Constant.class::isInstance) &&
                            valueList.stream()
                                    .map(Constant.class::cast)
                                    .map(Constant::value)
                                    .noneMatch(Objects::isNull);
                    if (nonNullConstants) {
                        Set<Object> values = valueList.stream()
                                .map(expression -> ((Constant) processWithExceptionHandling(expression, context)).value())
                                .collect(Collectors.toSet());

                        set = FastutilSetHelper.toFastutilHashSet(
                                values,
                                type,
                                plannerContext.getFunctionManager().getScalarFunctionImplementation(metadata.resolveOperator(HASH_CODE, ImmutableList.of(type)), simpleConvention(FAIL_ON_NULL, NEVER_NULL)).getMethodHandle(),
                                plannerContext.getFunctionManager().getScalarFunctionImplementation(metadata.resolveOperator(EQUAL, ImmutableList.of(type, type)), simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL)).getMethodHandle());
                    }
                    inListCache.put(valueList, set);
                }

                if (set != null) {
                    return new Constant(BOOLEAN, set.contains(constantValue));
                }
            }

            ResolvedFunction equalsOperator = metadata.resolveOperator(EQUAL, ImmutableList.of(node.value().type(), node.value().type()));

            Set<Expression> seen = new HashSet<>();
            List<Expression> values = node.valueList().stream()
                    .map(item -> processWithExceptionHandling(item, context))
                    .toList();

            Constant match = null;
            boolean hasNullMatch = false;
            List<Expression> newList = new ArrayList<>();
            for (Expression item : values) {
                if (value instanceof Constant constantValue && item instanceof Constant constantItem) {
                    Boolean equal = (Boolean) functionInvoker.invoke(equalsOperator, connectorSession, constantValue.value(), constantItem.value());
                    if (Boolean.TRUE.equals(equal)) {
                        if (evaluate) {
                            return TRUE;
                        }
                        else {
                            match = constantItem;
                        }
                    }
                    else if (equal == null) {
                        hasNullMatch = true;
                    }
                }
                else {
                    if (!seen.contains(item)) {
                        newList.add(item);
                    }

                    if (isDeterministic(item)) {
                        seen.add(item);
                    }
                }
            }

            if (match != null) {
                if (newList.isEmpty()) {
                    return TRUE;
                }

                // if the list is not empty, there are either unresolved expressions
                // or expressions that would fail upon evaluation. Leave them in the
                // list and place the match at the end to force the other expressions
                // to be evaluated at some point
                newList.add(match);
            }

            if (newList.isEmpty()) {
                return hasNullMatch ? NULL_BOOLEAN : FALSE;
            }

            if (newList.size() == 1) {
                return hasNullMatch ? NULL_BOOLEAN : new Comparison(Operator.EQUAL, value, newList.getFirst());
            }

            return new In(value, newList);
        }

        @Override
        protected Expression visitComparison(Comparison node, SymbolResolver context)
        {
            Operator operator = node.operator();
            Expression left = processWithExceptionHandling(node.left(), context);
            Expression right = processWithExceptionHandling(node.right(), context);

            if (operator == Operator.IS_DISTINCT_FROM) {
                if (left instanceof Constant(Type leftType, Object leftValue) && right instanceof Constant(Type rightType, Object rightValue)) {
                    return new Constant(
                            BOOLEAN,
                            invokeOperator(IS_DISTINCT_FROM, ImmutableList.of(leftType, rightType), Arrays.asList(leftValue, rightValue)));
                }

                if (isConstantNull(left)) {
                    return new Not(new IsNull(right));
                }

                if (isConstantNull(right)) {
                    return new Not(new IsNull(left));
                }
            }

            if (left instanceof Constant(Type leftType, Object leftValue) && right instanceof Constant(Type rightType, Object rightValue)) {
                List<Type> types = ImmutableList.of(leftType, rightType);
                return new Constant(
                        BOOLEAN,
                        switch (operator) {
                            case EQUAL -> invokeOperator(EQUAL, types, Arrays.asList(leftValue, rightValue));
                            case NOT_EQUAL -> switch (invokeOperator(EQUAL, types, Arrays.asList(leftValue, rightValue))) {
                                case null -> null;
                                case Boolean result -> !result;
                                default -> throw new IllegalStateException("Unexpected value for boolean expression");
                            };
                            case LESS_THAN -> invokeOperator(LESS_THAN, types, Arrays.asList(leftValue, rightValue));
                            case LESS_THAN_OR_EQUAL -> invokeOperator(LESS_THAN_OR_EQUAL, types, Arrays.asList(leftValue, rightValue));
                            case GREATER_THAN -> invokeOperator(LESS_THAN, types, Arrays.asList(rightValue, leftValue));
                            case GREATER_THAN_OR_EQUAL -> invokeOperator(LESS_THAN_OR_EQUAL, types, Arrays.asList(rightValue, leftValue));
                            default -> throw new IllegalStateException("Unexpected operator: " + operator);
                        });
            }

            return new Comparison(operator, left, right);
        }

        @Override
        protected Expression visitBetween(Between node, SymbolResolver context)
        {
            Expression value = processWithExceptionHandling(node.value(), context);
            Expression min = processWithExceptionHandling(node.min(), context);
            Expression max = processWithExceptionHandling(node.max(), context);

            ResolvedFunction lessThanOrEqual = metadata.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(value.type(), value.type()));

            if (min instanceof Constant minConstant && max instanceof Constant maxConstant) {
                if (!isConstantNull(minConstant) && !isConstantNull(maxConstant) && !Boolean.TRUE.equals(functionInvoker.invoke(lessThanOrEqual, connectorSession, minConstant.value(), maxConstant.value()))) {
                    return ifExpression(new Not(new IsNull(value)), FALSE);
                }

                if (value instanceof Constant valueConstant) {
                    Boolean minComparison = (Boolean) functionInvoker.invoke(lessThanOrEqual, connectorSession, minConstant.value(), valueConstant.value());
                    Boolean maxComparison = (Boolean) functionInvoker.invoke(lessThanOrEqual, connectorSession, valueConstant.value(), maxConstant.value());

                    if (Boolean.TRUE.equals(minComparison) && Boolean.TRUE.equals(maxComparison)) {
                        return TRUE;
                    }
                    else if (Boolean.FALSE.equals(minComparison) || Boolean.FALSE.equals(maxComparison)) {
                        return FALSE;
                    }
                    return NULL_BOOLEAN;
                }
            }
            else if (min instanceof Constant minConstant && value instanceof Constant valueConstant) {
                Boolean comparison = (Boolean) functionInvoker.invoke(lessThanOrEqual, connectorSession, minConstant.value(), valueConstant.value());
                return Boolean.FALSE.equals(comparison) ? FALSE : new Comparison(Operator.LESS_THAN, value, max);
            }
            else if (max instanceof Constant maxConstant && value instanceof Constant valueConstant) {
                Boolean comparison = (Boolean) functionInvoker.invoke(lessThanOrEqual, connectorSession, valueConstant.value(), maxConstant.value());
                return Boolean.FALSE.equals(comparison) ? FALSE : new Comparison(Operator.LESS_THAN, min, value);
            }

            return new Between(value, min, max);
        }

        @Override
        protected Expression visitNullIf(NullIf node, SymbolResolver context)
        {
            Expression first = processWithExceptionHandling(node.first(), context);
            if (isConstantNull(first)) {
                return new Constant(first.type(), null);
            }

            Expression second = processWithExceptionHandling(node.second(), context);
            if (isConstantNull(second)) {
                return first;
            }

            if (first instanceof Constant(Type firstType, Object firstValue) && second instanceof Constant(Type secondType, Object secondValue)) {
                Type commonType = typeCoercion.getCommonSuperType(firstType, secondType).get();

                ResolvedFunction firstCast = metadata.getCoercion(firstType, commonType);
                ResolvedFunction secondCast = metadata.getCoercion(secondType, commonType);

                // cast(first as <common type>) == cast(second as <common type>)
                boolean equal = Boolean.TRUE.equals(invokeOperator(
                        EQUAL,
                        ImmutableList.of(commonType, commonType),
                        ImmutableList.of(
                                functionInvoker.invoke(firstCast, connectorSession, ImmutableList.of(firstValue)),
                                functionInvoker.invoke(secondCast, connectorSession, ImmutableList.of(secondValue)))));

                return equal ? new Constant(firstType, null) : first;
            }

            return new NullIf(first, second);
        }

        @Override
        protected Expression visitNot(Not node, SymbolResolver context)
        {
            Expression argument = processWithExceptionHandling(node.value(), context);

            return switch (argument) {
                case Constant constant when constant.value() == null -> NULL_BOOLEAN;
                case Constant(Type type, Boolean value) -> new Constant(BOOLEAN, !value);
                default -> new Not(argument);
            };
        }

        @Override
        protected Expression visitLogical(Logical node, SymbolResolver context)
        {
            List<Expression> terms = node.terms().stream()
                    .map(term -> processWithExceptionHandling(term, context))
                    .toList();

            Set<Expression> seen = new HashSet<>();
            List<Expression> newTerms = new ArrayList<>();
            for (Expression term : terms) {
                if (term instanceof Constant(Type type, Boolean value)) {
                    if (node.operator() == AND && !value) {
                        return FALSE;
                    }

                    if (node.operator() == OR && value) {
                        return TRUE;
                    }
                }
                else if (!seen.contains(term)) {
                    newTerms.add(term);

                    if (isDeterministic(term)) {
                        seen.add(term);
                    }
                }
            }

            if (newTerms.isEmpty()) {
                return switch (node.operator()) {
                    case AND -> TRUE; // terms are true
                    case OR -> FALSE; // all terms are false
                };
            }

            if (newTerms.size() == 1) {
                return newTerms.getFirst();
            }

            return new Logical(node.operator(), newTerms);
        }

        @Override
        protected Expression visitCall(Call node, SymbolResolver context)
        {
            ResolvedFunction function = node.function();
            if (function.name().getFunctionName().equals(mangleOperatorName(NEGATION))) {
                return processNegation(node, context);
            }

            List<Expression> arguments = node.arguments().stream()
                    .map(argument -> processWithExceptionHandling(argument, context))
                    .toList();

            FunctionNullability nullability = function.functionNullability();
            for (int i = 0; i < arguments.size(); i++) {
                Expression argument = arguments.get(i);
                if (isConstantNull(argument) && !nullability.isArgumentNullable(i)) {
                    return new Constant(node.type(), null);
                }
            }

            if ((evaluate ||
                    function.deterministic() && // constant fold non-deterministic functions only in evaluation mode
                            !isDynamicFilter(node) &&
                            !function.name().equals(FAIL_NAME) &&
                            arguments.stream().allMatch(e -> e instanceof Constant || e instanceof Lambda && isDeterministic(e)))) {
                List<Object> argumentValues = arguments.stream()
                        .map(argument -> switch (argument) {
                            case Constant constant -> constant.value();
                            case Lambda lambda -> makeLambdaInvoker(lambda);
                            default -> throw new IllegalArgumentException("Expected lambda or constant, found: " + argument);
                        })
                        .collect(toList());

                try {
                    return new Constant(node.type(), functionInvoker.invoke(function, connectorSession, argumentValues));
                }
                catch (TrinoException e) {
                    if (evaluate) {
                        throw e;
                    }

                    return new Call(
                            node.function(),
                            node.arguments().stream()
                                    .map(argument -> processWithExceptionHandling(argument, context))
                                    .toList());
                }
            }

            return new Call(function, arguments);
        }

        private Object evaluate(Expression body, Map<Symbol, Integer> mappings, Object... arguments)
        {
            Constant result = (Constant) new Visitor(true).process(
                    body,
                    symbol -> {
                        Integer index = mappings.get(symbol);
                        if (index == null) {
                            return Optional.empty();
                        }
                        return Optional.of(new Constant(symbol.type(), arguments[index]));
                    });

            return result.value();
        }

        private MethodHandle makeLambdaInvoker(Lambda lambda)
        {
            Map<Symbol, Integer> mappings = new HashMap<>();
            for (int i = 0; i < lambda.arguments().size(); i++) {
                mappings.put(lambda.arguments().get(i), i);
            }

            return LAMBDA_EVALUATOR.bindTo(this)
                    .bindTo(lambda.body())
                    .bindTo(mappings)
                    .asVarargsCollector(new Object[0].getClass());
        }

        private Expression processNegation(Call negation, SymbolResolver context)
        {
            Expression value = processWithExceptionHandling(negation.arguments().getFirst(), context);

            return switch (value) {
                case Constant constant -> new Constant(negation.type(), functionInvoker.invoke(negation.function(), connectorSession, ImmutableList.of(constant.value())));
                case Call inner when inner.function().name().equals(builtinFunctionName(NEGATION)) -> inner.arguments().getFirst(); // double negation
                case Expression inner -> new Call(negation.function(), ImmutableList.of(inner));
            };
        }

        @Override
        protected Expression visitLambda(Lambda node, SymbolResolver context)
        {
            if (evaluate) {
                return node;
            }

            return new Lambda(
                    node.arguments(),
                    processWithExceptionHandling(node.body(), context));
        }

        @Override
        protected Expression visitBind(Bind node, SymbolResolver context)
        {
            List<Expression> values = node.values().stream()
                    .map(value -> processWithExceptionHandling(value, context))
                    .toList();

            Map<Symbol, Constant> bindings = new HashMap<>();

            List<Symbol> newArguments = new ArrayList<>();
            List<Expression> newBindings = new ArrayList<>();
            for (int i = 0; i < values.size(); i++) {
                Symbol argument = node.function().arguments().get(i);
                Expression value = values.get(i);

                if (value instanceof Constant constant) {
                    bindings.put(argument, constant);
                }
                else {
                    newArguments.add(argument);
                    newBindings.add(value);
                }
            }
            for (int i = values.size(); i < node.function().arguments().size(); i++) {
                newArguments.add(node.function().arguments().get(i));
            }

            Expression body = new Visitor(false).process(node.function().body(), symbol -> {
                Constant result = bindings.get(symbol);
                if (result != null) {
                    return Optional.of(result);
                }
                return context.getValue(symbol);
            });

            if (newBindings.isEmpty()) {
                return new Lambda(newArguments, body);
            }

            return new Bind(newBindings, new Lambda(newArguments, body));
        }

        @Override
        public Expression visitCast(Cast node, SymbolResolver context)
        {
            Expression value = processWithExceptionHandling(node.expression(), context);

            return switch (value) {
                case Call call when call.function().name().equals(builtinFunctionName("json_parse")) -> processJsonParseWithinCast(node, call);
                case Expression expression when expression.type().equals(node.type()) -> expression;
                case Constant constant when constant.value() == null -> new Constant(node.type(), null);
                case Constant constant -> {
                    try {
                        yield new Constant(node.type(), functionInvoker.invoke(metadata.getCoercion(constant.type(), node.type()), connectorSession, ImmutableList.of(constant.value())));
                    }
                    catch (TrinoException e) {
                        if (node.safe()) {
                            yield new Constant(node.type(), null);
                        }

                        if (evaluate) {
                            throw e;
                        }

                        yield new Cast(constant, node.type());
                    }
                }
                default -> new Cast(value, node.type(), node.safe());
            };
        }

        // Optimization for CAST(JSON_PARSE(...) AS ARRAY/MAP/ROW)
        private Expression processJsonParseWithinCast(Cast cast, Call jsonParse)
        {
            Expression string = jsonParse.arguments().getFirst();
            return switch (cast.type()) {
                case ArrayType arrayType -> new Call(metadata.getCoercion(builtinFunctionName(JSON_STRING_TO_ARRAY_NAME), string.type(), arrayType), jsonParse.arguments());
                case MapType mapType -> new Call(metadata.getCoercion(builtinFunctionName(JSON_STRING_TO_MAP_NAME), string.type(), mapType), jsonParse.arguments());
                case RowType rowType -> new Call(metadata.getCoercion(builtinFunctionName(JSON_STRING_TO_ROW_NAME), string.type(), rowType), jsonParse.arguments());
                default -> cast;
            };
        }

        @Override
        protected Expression visitArray(Array node, SymbolResolver context)
        {
            List<Expression> elements = node.elements().stream()
                    .map(field -> processWithExceptionHandling(field, context))
                    .toList();

            if (elements.stream().allMatch(Constant.class::isInstance)) {
                BlockBuilder builder = node.elementType().createBlockBuilder(null, node.elements().size());
                for (Expression element : elements) {
                    writeNativeValue(node.elementType(), builder, ((Constant) element).value());
                }
                return new Constant(node.type(), builder.build());
            }

            return new Array(node.elementType(), elements);
        }

        @Override
        protected Expression visitRow(Row node, SymbolResolver context)
        {
            List<Expression> fields = node.items().stream()
                    .map(field -> processWithExceptionHandling(field, context))
                    .toList();

            if (fields.stream().allMatch(Constant.class::isInstance)) {
                RowType rowType = (RowType) node.type();
                return new Constant(
                        rowType,
                        buildRowValue(rowType, builders -> {
                            for (int i = 0; i < fields.size(); ++i) {
                                writeNativeValue(fields.get(i).type(), builders.get(i), ((Constant) fields.get(i)).value());
                            }
                        }));
            }

            return new Row(fields);
        }

        @Override
        protected Expression visitFieldReference(FieldReference node, SymbolResolver context)
        {
            Expression base = processWithExceptionHandling(node.base(), context);

            return switch (base) {
                case Constant(RowType type, SqlRow row) -> {
                    Type fieldType = type.getFields().get(node.field()).getType();
                    yield new Constant(
                            fieldType,
                            readNativeValue(fieldType, row.getRawFieldBlock(node.field()), row.getRawIndex()));
                }
                case Constant(RowType type, Object value) -> new Constant(type.getFields().get(node.field()).getType(), null);
                case Row row -> row.items().get(node.field());
                default -> new FieldReference(base, node.field());
            };
        }

        @Override
        protected Expression visitExpression(Expression node, SymbolResolver context)
        {
            throw new TrinoException(NOT_SUPPORTED, "not yet implemented: " + node.getClass().getName());
        }

        private Object invokeOperator(OperatorType operatorType, List<? extends Type> argumentTypes, List<Object> argumentValues)
        {
            ResolvedFunction operator = metadata.resolveOperator(operatorType, argumentTypes);
            return functionInvoker.invoke(operator, connectorSession, argumentValues);
        }
    }

    private static boolean isConstantNull(Expression operand)
    {
        return operand instanceof Constant constant && constant.value() == null;
    }
}
