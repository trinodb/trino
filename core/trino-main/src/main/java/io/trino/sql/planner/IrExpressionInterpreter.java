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
import com.google.common.primitives.Primitives;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.TrinoException;
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
import io.trino.type.FunctionType;
import io.trino.type.TypeCoercion;
import io.trino.util.FastutilSetHelper;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.metadata.OperatorNameUtil.mangleOperatorName;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.NEGATION;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.sql.DynamicFilters.isDynamicFilter;
import static io.trino.sql.gen.VarArgsToMapAdapterGenerator.generateVarArgsToMapAdapter;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class IrExpressionInterpreter
{
    private static final CatalogSchemaFunctionName FAIL_NAME = builtinFunctionName("fail");

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
        Object result = new Visitor(false).processWithExceptionHandling(expression, null);
        verify(!(result instanceof Expression), "Expression interpreter returned an unresolved expression");
        return result;
    }

    public Expression optimize(SymbolResolver inputs)
    {
        Object result = new Visitor(true).processWithExceptionHandling(expression, inputs);

        if (result instanceof Expression expression) {
            return expression;
        }

        return new Constant(expression.type(), result);
    }

    public Expression optimize()
    {
        return optimize(NoOpSymbolResolver.INSTANCE);
    }

    private class Visitor
            extends IrVisitor<Object, SymbolResolver>
    {
        private final boolean optimize;

        private Visitor(boolean optimize)
        {
            this.optimize = optimize;
        }

        private Object processWithExceptionHandling(Expression expression, SymbolResolver context)
        {
            if (expression == null) {
                return null;
            }

            try {
                return process(expression, context);
            }
            catch (TrinoException e) {
                if (optimize) {
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
        protected Object visitReference(Reference node, SymbolResolver context)
        {
            Optional<Constant> binding = context.getValue(Symbol.from(node));
            if (binding.isPresent()) {
                return binding.get().value();
            }

            return node;
        }

        @Override
        protected Object visitConstant(Constant node, SymbolResolver context)
        {
            return node.value();
        }

        @Override
        protected Object visitIsNull(IsNull node, SymbolResolver context)
        {
            Object value = processWithExceptionHandling(node.value(), context);

            if (value instanceof Expression) {
                return new IsNull(toExpression(value, node.value().type()));
            }

            return value == null;
        }

        @Override
        protected Object visitCase(Case node, SymbolResolver context)
        {
            Object newDefault = null;
            boolean foundNewDefault = false;

            List<WhenClause> whenClauses = new ArrayList<>();
            for (WhenClause whenClause : node.whenClauses()) {
                Object whenOperand = processWithExceptionHandling(whenClause.getOperand(), context);

                if (whenOperand instanceof Expression) {
                    // cannot fully evaluate, add updated whenClause
                    whenClauses.add(new WhenClause(
                            toExpression(whenOperand, whenClause.getOperand().type()),
                            toExpression(processWithExceptionHandling(whenClause.getResult(), context), whenClause.getResult().type())));
                }
                else if (Boolean.TRUE.equals(whenOperand)) {
                    // condition is true, use this as default
                    foundNewDefault = true;
                    newDefault = processWithExceptionHandling(whenClause.getResult(), context);
                    break;
                }
            }

            Object defaultResult;
            if (foundNewDefault) {
                defaultResult = newDefault;
            }
            else {
                defaultResult = processWithExceptionHandling(node.defaultValue(), context);
            }

            if (whenClauses.isEmpty()) {
                return defaultResult;
            }

            return new Case(whenClauses, toExpression(defaultResult, ((Expression) node).type()));
        }

        @Override
        protected Object visitSwitch(Switch node, SymbolResolver context)
        {
            Object operand = processWithExceptionHandling(node.operand(), context);
            Type operandType = node.operand().type();

            // if operand is null, return defaultValue
            if (operand == null) {
                return processWithExceptionHandling(node.defaultValue(), context);
            }

            Object newDefault = null;
            boolean foundNewDefault = false;

            List<WhenClause> whenClauses = new ArrayList<>();
            for (WhenClause whenClause : node.whenClauses()) {
                Object whenOperand = processWithExceptionHandling(whenClause.getOperand(), context);

                if (whenOperand instanceof Expression || operand instanceof Expression) {
                    // cannot fully evaluate, add updated whenClause
                    whenClauses.add(new WhenClause(
                            toExpression(whenOperand, whenClause.getOperand().type()),
                            toExpression(processWithExceptionHandling(whenClause.getResult(), context), whenClause.getResult().type())));
                }
                else {
                    if (whenOperand != null && isEqual(operand, operandType, whenOperand, whenClause.getOperand().type())) {
                        // condition is true, use this as default
                        foundNewDefault = true;
                        newDefault = processWithExceptionHandling(whenClause.getResult(), context);
                        break;
                    }
                }
            }

            Object defaultResult;
            if (foundNewDefault) {
                defaultResult = newDefault;
            }
            else {
                defaultResult = processWithExceptionHandling(node.defaultValue(), context);
            }

            if (whenClauses.isEmpty()) {
                return defaultResult;
            }

            Expression defaultExpression = toExpression(defaultResult, ((Expression) node).type());
            return new Switch(toExpression(operand, node.operand().type()), whenClauses, defaultExpression);
        }

        private boolean isEqual(Object operand1, Type type1, Object operand2, Type type2)
        {
            return Boolean.TRUE.equals(invokeOperator(OperatorType.EQUAL, ImmutableList.of(type1, type2), ImmutableList.of(operand1, operand2)));
        }

        @Override
        protected Object visitCoalesce(Coalesce node, SymbolResolver context)
        {
            List<Object> newOperands = processOperands(node, context);
            if (newOperands.isEmpty()) {
                return null;
            }
            if (newOperands.size() == 1) {
                return getOnlyElement(newOperands);
            }
            return new Coalesce(newOperands.stream()
                    .map(value -> toExpression(value, ((Expression) node).type()))
                    .collect(toImmutableList()));
        }

        private List<Object> processOperands(Coalesce node, SymbolResolver context)
        {
            List<Object> newOperands = new ArrayList<>();
            Set<Expression> uniqueNewOperands = new HashSet<>();
            for (Expression operand : node.operands()) {
                Object value = processWithExceptionHandling(operand, context);
                if (value instanceof Coalesce) {
                    // The nested CoalesceExpression was recursively processed. It does not contain null.
                    for (Expression nestedOperand : ((Coalesce) value).operands()) {
                        // Skip duplicates unless they are non-deterministic.
                        if (!isDeterministic(nestedOperand) || uniqueNewOperands.add(nestedOperand)) {
                            newOperands.add(nestedOperand);
                        }
                        // This operand can be evaluated to a non-null value. Remaining operands can be skipped.
                        if (nestedOperand instanceof Constant constant && constant.value() != null) {
                            return newOperands;
                        }
                    }
                }
                else if (value instanceof Expression expression) {
                    // Skip duplicates unless they are non-deterministic.
                    if (!isDeterministic(expression) || uniqueNewOperands.add(expression)) {
                        newOperands.add(expression);
                    }
                }
                else if (value != null) {
                    // This operand can be evaluated to a non-null value. Remaining operands can be skipped.
                    newOperands.add(value);
                    return newOperands;
                }
            }
            return newOperands;
        }

        @Override
        protected Object visitIn(In node, SymbolResolver context)
        {
            Object value = processWithExceptionHandling(node.value(), context);

            List<Expression> valueList = node.valueList();
            // `NULL IN ()` would be false, but InListExpression cannot be empty by construction
            if (value == null) {
                return null;
            }

            Type type = node.value().type();
            if (!(value instanceof Expression) &&
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
                        Set<Object> objectSet = valueList.stream().map(expression -> processWithExceptionHandling(expression, context)).collect(Collectors.toSet());
                        set = FastutilSetHelper.toFastutilHashSet(
                                objectSet,
                                type,
                                plannerContext.getFunctionManager().getScalarFunctionImplementation(metadata.resolveOperator(HASH_CODE, ImmutableList.of(type)), simpleConvention(FAIL_ON_NULL, NEVER_NULL)).getMethodHandle(),
                                plannerContext.getFunctionManager().getScalarFunctionImplementation(metadata.resolveOperator(OperatorType.EQUAL, ImmutableList.of(type, type)), simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL)).getMethodHandle());
                    }
                    inListCache.put(valueList, set);
                }

                if (set != null) {
                    return set.contains(value);
                }
            }

            boolean hasUnresolvedValue = value instanceof Expression;
            boolean hasNullValue = false;
            boolean found = false;
            List<Object> values = new ArrayList<>(valueList.size());
            List<Type> types = new ArrayList<>(valueList.size());

            ResolvedFunction equalsOperator = metadata.resolveOperator(OperatorType.EQUAL, types(node.value(), node.value()));
            for (Expression expression : valueList) {
                if (value instanceof Expression && expression instanceof Constant) {
                    // skip interpreting of literal IN term since it cannot be compared
                    // with unresolved "value" and it cannot be simplified further
                    values.add(expression);
                    types.add(expression.type());
                    continue;
                }

                // Use process() instead of processWithExceptionHandling() for processing in-list items.
                // Do not handle exceptions thrown while processing a single in-list expression,
                // but fail the whole in-predicate evaluation.
                // According to in-predicate semantics, all in-list items must be successfully evaluated
                // before a check for the match is performed.
                Object inValue = process(expression, context);
                if (value instanceof Expression || inValue instanceof Expression) {
                    hasUnresolvedValue = true;
                    values.add(inValue);
                    types.add(expression.type());
                    continue;
                }

                if (inValue == null) {
                    hasNullValue = true;
                }
                else {
                    Boolean result = (Boolean) functionInvoker.invoke(equalsOperator, connectorSession, ImmutableList.of(value, inValue));
                    if (result == null) {
                        hasNullValue = true;
                    }
                    else if (!found && result) {
                        // in does not short-circuit so we must evaluate all value in the list
                        found = true;
                    }
                }
            }
            if (found) {
                return true;
            }

            if (hasUnresolvedValue) {
                List<Expression> expressionValues = toExpressions(values, types);
                List<Expression> simplifiedExpressionValues = Stream.concat(
                                expressionValues.stream()
                                        .filter(expression -> isDeterministic(expression))
                                        .distinct(),
                                expressionValues.stream()
                                        .filter(expression -> !isDeterministic(expression)))
                        .collect(toImmutableList());

                if (simplifiedExpressionValues.size() == 1) {
                    return new Comparison(Operator.EQUAL, toExpression(value, type), simplifiedExpressionValues.get(0));
                }

                return new In(toExpression(value, type), simplifiedExpressionValues);
            }
            if (hasNullValue) {
                return null;
            }
            return false;
        }

        @Override
        protected Object visitComparison(Comparison node, SymbolResolver context)
        {
            Operator operator = node.operator();
            Expression left = node.left();
            Expression right = node.right();

            if (operator == Operator.IS_DISTINCT_FROM) {
                return processIsDistinctFrom(context, left, right);
            }
            // Execution engine does not have not equal and greater than operators, so interpret with
            // equal or less than, but do not flip operator in result, as many optimizers depend on
            // operators not flipping
            if (node.operator() == Operator.NOT_EQUAL) {
                Object result = visitComparison(flipComparison(node), context);
                if (result == null) {
                    return null;
                }
                if (result instanceof Comparison) {
                    return flipComparison((Comparison) result);
                }
                return !(Boolean) result;
            }
            if (node.operator() == Operator.GREATER_THAN || node.operator() == Operator.GREATER_THAN_OR_EQUAL) {
                Object result = visitComparison(flipComparison(node), context);
                if (result instanceof Comparison) {
                    return flipComparison((Comparison) result);
                }
                return result;
            }

            return processComparisonExpression(context, operator, left, right);
        }

        private Object processIsDistinctFrom(SymbolResolver context, Expression leftExpression, Expression rightExpression)
        {
            Object left = processWithExceptionHandling(leftExpression, context);
            Object right = processWithExceptionHandling(rightExpression, context);

            if (left == null && right instanceof Expression) {
                return new Not(new IsNull(((Expression) right)));
            }

            if (right == null && left instanceof Expression) {
                return new Not(new IsNull(((Expression) left)));
            }

            if (left instanceof Expression || right instanceof Expression) {
                return new Comparison(Operator.IS_DISTINCT_FROM, toExpression(left, leftExpression.type()), toExpression(right, rightExpression.type()));
            }

            return invokeOperator(OperatorType.valueOf(Operator.IS_DISTINCT_FROM.name()), types(leftExpression, rightExpression), Arrays.asList(left, right));
        }

        private Object processComparisonExpression(SymbolResolver context, Operator operator, Expression leftExpression, Expression rightExpression)
        {
            Object left = processWithExceptionHandling(leftExpression, context);
            if (left == null) {
                return null;
            }

            Object right = processWithExceptionHandling(rightExpression, context);
            if (right == null) {
                return null;
            }

            if (left instanceof Expression || right instanceof Expression) {
                return new Comparison(operator, toExpression(left, leftExpression.type()), toExpression(right, rightExpression.type()));
            }

            return invokeOperator(OperatorType.valueOf(operator.name()), types(leftExpression, rightExpression), ImmutableList.of(left, right));
        }

        // TODO define method contract or split into separate methods, as flip(EQUAL) is a negation, while flip(LESS_THAN) is just flipping sides
        private Comparison flipComparison(Comparison comparison)
        {
            return switch (comparison.operator()) {
                case EQUAL -> new Comparison(Operator.NOT_EQUAL, comparison.left(), comparison.right());
                case NOT_EQUAL -> new Comparison(Operator.EQUAL, comparison.left(), comparison.right());
                case LESS_THAN -> new Comparison(Operator.GREATER_THAN, comparison.right(), comparison.left());
                case LESS_THAN_OR_EQUAL -> new Comparison(Operator.GREATER_THAN_OR_EQUAL, comparison.right(), comparison.left());
                case GREATER_THAN -> new Comparison(Operator.LESS_THAN, comparison.right(), comparison.left());
                case GREATER_THAN_OR_EQUAL -> new Comparison(Operator.LESS_THAN_OR_EQUAL, comparison.right(), comparison.left());
                default -> throw new IllegalStateException("Unexpected value: " + comparison.operator());
            };
        }

        @Override
        protected Object visitBetween(Between node, SymbolResolver context)
        {
            Object value = processWithExceptionHandling(node.value(), context);
            if (value == null) {
                return null;
            }
            Object min = processWithExceptionHandling(node.min(), context);
            Object max = processWithExceptionHandling(node.max(), context);

            if (value instanceof Expression || min instanceof Expression || max instanceof Expression) {
                return new Between(
                        toExpression(value, node.value().type()),
                        toExpression(min, node.min().type()),
                        toExpression(max, node.max().type()));
            }

            Boolean greaterOrEqualToMin = null;
            if (min != null) {
                greaterOrEqualToMin = (Boolean) invokeOperator(OperatorType.LESS_THAN_OR_EQUAL, types(node.min(), node.value()), ImmutableList.of(min, value));
            }
            Boolean lessThanOrEqualToMax = null;
            if (max != null) {
                lessThanOrEqualToMax = (Boolean) invokeOperator(OperatorType.LESS_THAN_OR_EQUAL, types(node.value(), node.max()), ImmutableList.of(value, max));
            }

            if (greaterOrEqualToMin == null) {
                return Objects.equals(lessThanOrEqualToMax, Boolean.FALSE) ? false : null;
            }
            if (lessThanOrEqualToMax == null) {
                return Objects.equals(greaterOrEqualToMin, Boolean.FALSE) ? false : null;
            }
            return greaterOrEqualToMin && lessThanOrEqualToMax;
        }

        @Override
        protected Object visitNullIf(NullIf node, SymbolResolver context)
        {
            Object first = processWithExceptionHandling(node.first(), context);
            if (first == null) {
                return null;
            }
            Object second = processWithExceptionHandling(node.second(), context);
            if (second == null) {
                return first;
            }

            Type firstType = node.first().type();
            Type secondType = node.second().type();

            if (hasUnresolvedValue(first, second)) {
                return new NullIf(toExpression(first, firstType), toExpression(second, secondType));
            }

            Type commonType = typeCoercion.getCommonSuperType(firstType, secondType).get();

            ResolvedFunction firstCast = metadata.getCoercion(firstType, commonType);
            ResolvedFunction secondCast = metadata.getCoercion(secondType, commonType);

            // cast(first as <common type>) == cast(second as <common type>)
            boolean equal = Boolean.TRUE.equals(invokeOperator(
                    OperatorType.EQUAL,
                    ImmutableList.of(commonType, commonType),
                    ImmutableList.of(
                            functionInvoker.invoke(firstCast, connectorSession, ImmutableList.of(first)),
                            functionInvoker.invoke(secondCast, connectorSession, ImmutableList.of(second)))));

            if (equal) {
                return null;
            }
            return first;
        }

        @Override
        protected Object visitNot(Not node, SymbolResolver context)
        {
            Object value = processWithExceptionHandling(node.value(), context);
            if (value == null) {
                return null;
            }

            if (value instanceof Expression) {
                return new Not(toExpression(value, node.value().type()));
            }

            return !(Boolean) value;
        }

        @Override
        protected Object visitLogical(Logical node, SymbolResolver context)
        {
            List<Object> terms = new ArrayList<>();
            List<Type> types = new ArrayList<>();

            for (Expression term : node.terms()) {
                Object processed = processWithExceptionHandling(term, context);

                switch (node.operator()) {
                    case AND -> {
                        if (Boolean.FALSE.equals(processed)) {
                            return false;
                        }
                        if (!Boolean.TRUE.equals(processed)) {
                            terms.add(processed);
                            types.add(term.type());
                        }
                    }
                    case OR -> {
                        if (Boolean.TRUE.equals(processed)) {
                            return true;
                        }
                        if (!Boolean.FALSE.equals(processed)) {
                            terms.add(processed);
                            types.add(term.type());
                        }
                    }
                }
            }

            if (terms.isEmpty()) {
                return switch (node.operator()) {
                    case AND -> true; // terms are true
                    case OR -> false; // all terms are false
                };
            }

            if (terms.size() == 1) {
                return terms.get(0);
            }

            if (terms.stream().allMatch(Objects::isNull)) {
                return null;
            }

            ImmutableList.Builder<Expression> expressions = ImmutableList.builder();
            for (int i = 0; i < terms.size(); i++) {
                expressions.add(toExpression(terms.get(i), types.get(i)));
            }
            return new Logical(node.operator(), expressions.build());
        }

        @Override
        protected Object visitCall(Call node, SymbolResolver context)
        {
            if (node.function().getName().getFunctionName().equals(mangleOperatorName(NEGATION))) {
                return processNegation(node, context);
            }

            List<Type> argumentTypes = new ArrayList<>();
            List<Object> argumentValues = new ArrayList<>();
            for (Expression expression : node.arguments()) {
                Object value = processWithExceptionHandling(expression, context);
                Type type = expression.type();
                argumentValues.add(value);
                argumentTypes.add(type);
            }

            ResolvedFunction resolvedFunction = node.function();
            FunctionNullability functionNullability = resolvedFunction.getFunctionNullability();
            for (int i = 0; i < argumentValues.size(); i++) {
                Object value = argumentValues.get(i);
                if (value == null && !functionNullability.isArgumentNullable(i)) {
                    return null;
                }
            }

            // do not optimize non-deterministic functions
            if (optimize && (!resolvedFunction.isDeterministic() ||
                    hasUnresolvedValue(argumentValues) ||
                    isDynamicFilter(node) ||
                    resolvedFunction.getSignature().getName().equals(FAIL_NAME))) {
                return ResolvedFunctionCallBuilder.builder(resolvedFunction)
                        .setArguments(toExpressions(argumentValues, argumentTypes))
                        .build();
            }
            return functionInvoker.invoke(resolvedFunction, connectorSession, argumentValues);
        }

        private Object processNegation(Call negation, SymbolResolver context)
        {
            Object value = processWithExceptionHandling(negation.arguments().getFirst(), context);

            return switch (value) {
                case Call inner when inner.function().getName().getFunctionName().equals(mangleOperatorName(NEGATION)) -> inner.arguments().getFirst(); // double negation
                case Expression inner -> new Call(negation.function(), ImmutableList.of(inner));
                case null -> null;
                default -> functionInvoker.invoke(negation.function(), connectorSession, ImmutableList.of(value));
            };
        }

        @Override
        protected Object visitLambda(Lambda node, SymbolResolver context)
        {
            if (optimize) {
                // TODO: enable optimization related to lambda expression
                // A mechanism to convert function type back into lambda expression need to exist to enable optimization
                Object value = processWithExceptionHandling(node.body(), context);
                Expression optimizedBody;

                // value may be null, converted to an expression by toExpression(value, type)
                if (value instanceof Expression) {
                    optimizedBody = (Expression) value;
                }
                else {
                    Type type = node.body().type();
                    optimizedBody = toExpression(value, type);
                }
                return new Lambda(node.arguments(), optimizedBody);
            }

            Expression body = node.body();
            List<String> argumentNames = node.arguments().stream()
                    .map(Symbol::getName)
                    .toList();
            FunctionType functionType = (FunctionType) node.type();
            checkArgument(argumentNames.size() == functionType.getArgumentTypes().size());

            return generateVarArgsToMapAdapter(
                    Primitives.wrap(functionType.getReturnType().getJavaType()),
                    functionType.getArgumentTypes().stream()
                            .map(Type::getJavaType)
                            .map(Primitives::wrap)
                            .collect(toImmutableList()),
                    argumentNames,
                    map -> processWithExceptionHandling(body, new LambdaSymbolResolver(map)));
        }

        @Override
        protected Object visitBind(Bind node, SymbolResolver context)
        {
            List<Object> values = node.values().stream()
                    .map(value -> processWithExceptionHandling(value, context))
                    .collect(toList()); // values are nullable
            Object function = processWithExceptionHandling(node.function(), context);

            if (hasUnresolvedValue(values) || hasUnresolvedValue(function)) {
                ImmutableList.Builder<Expression> builder = ImmutableList.builder();
                for (int i = 0; i < values.size(); i++) {
                    builder.add(toExpression(values.get(i), node.values().get(i).type()));
                }

                return new Bind(builder.build(), (Lambda) function);
            }

            return MethodHandles.insertArguments((MethodHandle) function, 0, values.toArray());
        }

        @Override
        public Object visitCast(Cast node, SymbolResolver context)
        {
            Object value = processWithExceptionHandling(node.expression(), context);
            Type targetType = node.type();
            Type sourceType = node.expression().type();
            if (value instanceof Expression) {
                if (targetType.equals(sourceType)) {
                    return value;
                }

                return new Cast((Expression) value, node.type(), node.safe());
            }

            if (value == null) {
                return null;
            }

            ResolvedFunction operator = metadata.getCoercion(sourceType, targetType);

            try {
                return functionInvoker.invoke(operator, connectorSession, ImmutableList.of(value));
            }
            catch (RuntimeException e) {
                if (node.safe()) {
                    return null;
                }
                throw e;
            }
        }

        @Override
        protected Object visitRow(Row node, SymbolResolver context)
        {
            RowType rowType = (RowType) ((Expression) node).type();
            List<Type> parameterTypes = rowType.getTypeParameters();
            List<Expression> arguments = node.items();

            int cardinality = arguments.size();
            List<Object> values = new ArrayList<>(cardinality);
            for (Expression argument : arguments) {
                values.add(processWithExceptionHandling(argument, context));
            }
            if (hasUnresolvedValue(values)) {
                return new Row(toExpressions(values, parameterTypes));
            }
            return buildRowValue(rowType, fields -> {
                for (int i = 0; i < cardinality; ++i) {
                    writeNativeValue(parameterTypes.get(i), fields.get(i), values.get(i));
                }
            });
        }

        @Override
        protected Object visitFieldReference(FieldReference node, SymbolResolver context)
        {
            Object base = processWithExceptionHandling(node.base(), context);
            if (base == null) {
                return null;
            }

            if (hasUnresolvedValue(base)) {
                return new FieldReference(toExpression(base, node.base().type()), node.field());
            }

            SqlRow row = (SqlRow) base;
            Type returnType = node.base().type().getTypeParameters().get(node.field());
            return readNativeValue(returnType, row.getRawFieldBlock(node.field()), row.getRawIndex());
        }

        @Override
        protected Object visitExpression(Expression node, SymbolResolver context)
        {
            throw new TrinoException(NOT_SUPPORTED, "not yet implemented: " + node.getClass().getName());
        }

        private List<Type> types(Expression... expressions)
        {
            return Stream.of(expressions)
                    .map(Expression::type)
                    .collect(toImmutableList());
        }

        private boolean hasUnresolvedValue(Object... values)
        {
            return hasUnresolvedValue(ImmutableList.copyOf(values));
        }

        private boolean hasUnresolvedValue(List<Object> values)
        {
            return values.stream().anyMatch(instanceOf(Expression.class));
        }

        private Object invokeOperator(OperatorType operatorType, List<? extends Type> argumentTypes, List<Object> argumentValues)
        {
            ResolvedFunction operator = metadata.resolveOperator(operatorType, argumentTypes);
            return functionInvoker.invoke(operator, connectorSession, argumentValues);
        }

        private Expression toExpression(Object base, Type type)
        {
            if (base instanceof Expression expression) {
                return expression;
            }

            return new Constant(type, base);
        }

        private List<Expression> toExpressions(List<Object> values, List<Type> types)
        {
            checkArgument(values.size() == types.size(), "values and types do not have the same size");

            ImmutableList.Builder<Expression> expressions = ImmutableList.builder();
            for (int i = 0; i < values.size(); i++) {
                Object object = values.get(i);
                expressions.add(object instanceof Expression expression ?
                        expression :
                        new Constant(types.get(i), object));
            }
            return expressions.build();
        }
    }

    private static boolean isArray(Type type)
    {
        return type instanceof ArrayType;
    }

    private static class LambdaSymbolResolver
            implements SymbolResolver
    {
        private final Map<String, Object> values;

        public LambdaSymbolResolver(Map<String, Object> values)
        {
            this.values = requireNonNull(values, "values is null");
        }

        @Override
        public Optional<Constant> getValue(Symbol symbol)
        {
            checkState(values.containsKey(symbol.getName()), "values does not contain %s", symbol);
            return Optional.of(new Constant(symbol.getType(), values.get(symbol.getName())));
        }
    }
}
