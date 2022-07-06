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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Primitives;
import io.trino.Session;
import io.trino.metadata.FunctionNullability;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.ExpressionInterpreter.LambdaSymbolResolver;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.relational.DeterminismEvaluator;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.LambdaDefinitionExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.RowExpressionVisitor;
import io.trino.sql.relational.SpecialForm;
import io.trino.sql.relational.StandardFunctionResolution;
import io.trino.sql.relational.VariableReferenceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.type.FunctionType;
import io.trino.type.TypeCoercion;
import io.trino.type.UnknownType;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.sql.DynamicFilters.isDynamicFilter;
import static io.trino.sql.gen.VarArgsToMapAdapterGenerator.generateVarArgsToMapAdapter;
import static io.trino.sql.relational.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.Expressions.constantNull;
import static io.trino.sql.relational.RowExpressionUtil.isEffectivelyLiteral;
import static io.trino.sql.relational.SpecialForm.Form.AND;
import static io.trino.sql.relational.SpecialForm.Form.BIND;
import static io.trino.sql.relational.SpecialForm.Form.COALESCE;
import static io.trino.sql.relational.SpecialForm.Form.DEREFERENCE;
import static io.trino.sql.relational.SpecialForm.Form.IF;
import static io.trino.sql.relational.SpecialForm.Form.IN;
import static io.trino.sql.relational.SpecialForm.Form.IS_NULL;
import static io.trino.sql.relational.SpecialForm.Form.NULL_IF;
import static io.trino.sql.relational.SpecialForm.Form.OR;
import static io.trino.sql.relational.SpecialForm.Form.ROW_CONSTRUCTOR;
import static io.trino.sql.relational.SpecialForm.Form.SWITCH;
import static io.trino.sql.relational.SpecialForm.Form.WHEN;
import static io.trino.sql.relational.StandardFunctionResolution.isCastFunction;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class RowExpressionInterpreter
{
    private final RowExpression expression;

    private final PlannerContext plannerContext;
    private final Metadata metadata;

    private final LiteralEncoder literalEncoder;

    private final Session session;

    private final ConnectorSession connectorSession;

    private final InterpretedFunctionInvoker functionInvoker;

    private final TypeCoercion typeCoercion;
    private final StandardFunctionResolution resolution;

    public static Object evaluateConstantRowExpression(RowExpression expression, PlannerContext plannerContext, Session session)
    {
        // evaluate the expression
        Object result = new RowExpressionInterpreter(expression, plannerContext, session).evaluate();
        verify(!(result instanceof RowExpression), "RowExpression interpreter returned an unresolved expression");
        return result;
    }

    public static RowExpressionInterpreter rowExpressionInterpreter(RowExpression expression, PlannerContext plannerContext, Session session)
    {
        return new RowExpressionInterpreter(expression, plannerContext, session);
    }

    public RowExpressionInterpreter(RowExpression expression, PlannerContext plannerContext, Session session)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.plannerContext = plannerContext;
        this.metadata = plannerContext.getMetadata();
        this.literalEncoder = new LiteralEncoder(plannerContext);
        this.session = requireNonNull(session, "session is null");
        this.connectorSession = session.toConnectorSession();
        this.functionInvoker = new InterpretedFunctionInvoker(plannerContext.getFunctionManager());
        this.resolution = new StandardFunctionResolution(session, metadata);
        this.typeCoercion = new TypeCoercion(plannerContext.getTypeManager()::getType);
    }

    public Type getType()
    {
        return expression.getType();
    }

    public Object evaluate()
    {
        Object result = new Visitor(false).processWithExceptionHandling(expression, null);
        verify(!(result instanceof RowExpression), "RowExpression interpreter returned an unresolved RowExpression");
        return result;
    }

    public Object evaluate(SymbolResolver inputs)
    {
        Object result = new Visitor(false).processWithExceptionHandling(expression, inputs);
        verify(!(result instanceof RowExpression), "RowExpression interpreter returned an unresolved RowExpression");
        return result;
    }

    @VisibleForTesting
    public Object optimize(SymbolResolver inputs)
    {
        return new Visitor(true).processWithExceptionHandling(expression, inputs);
    }

    private class Visitor
            implements RowExpressionVisitor<Object, Object>
    {
        private final boolean optimize;

        private Visitor(boolean optimize)
        {
            this.optimize = optimize;
        }

        @Override
        public Object visitInputReference(InputReferenceExpression node, Object context)
        {
            return node;
        }

        @Override
        public Object visitConstant(ConstantExpression node, Object context)
        {
            return node.getValue();
        }

        @Override
        public Object visitVariableReference(VariableReferenceExpression node, Object context)
        {
            if (context instanceof SymbolResolver) {
                return ((SymbolResolver) context).getValue(new Symbol(node.getName()));
            }
            return node;
        }

        @Override
        public Object visitCall(CallExpression node, Object context)
        {
            List<Type> argumentTypes = new ArrayList<>();
            List<Object> argumentValues = new ArrayList<>();
            for (RowExpression expression : node.getArguments()) {
                Object value = processWithExceptionHandling(expression, context);
                Type type = expression.getType();
                argumentValues.add(value);
                argumentTypes.add(type);
            }

            ResolvedFunction resolvedFunction = node.getResolvedFunction();
            FunctionNullability functionNullability = resolvedFunction.getFunctionNullability();
            for (int i = 0; i < argumentValues.size(); i++) {
                Object value = argumentValues.get(i);
                if (value == null && !functionNullability.isArgumentNullable(i)) {
                    return null;
                }
            }

            // do not optimize non-deterministic functions
            if (optimize && (!metadata.getFunctionMetadata(session, resolvedFunction).isDeterministic() ||
                    hasUnresolvedValue(argumentValues) ||
                    isDynamicFilter(node) ||
                    resolvedFunction.getSignature().getName().equals("fail"))) {
                return call(resolvedFunction, toRowExpressions(argumentValues, argumentTypes));
            }
            return functionInvoker.invoke(resolvedFunction, connectorSession, argumentValues);
        }

        @Override
        public Object visitLambda(LambdaDefinitionExpression node, Object context)
        {
            if (optimize) {
                // TODO: enable optimization related to lambda expression
                // A mechanism to convert function type back into lambda expression need to exist to enable optimization
                return node;
            }

            RowExpression body = node.getBody();

            List<String> argumentNames = node.getArguments();
            FunctionType functionType = (FunctionType) node.getType();
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
        public Object visitSpecialForm(SpecialForm node, Object context)
        {
            switch (node.getForm()) {
                case IF: {
                    checkArgument(node.getArguments().size() == 3);
                    Object condition = processWithExceptionHandling(node.getArguments().get(0), context);
                    Object trueValue = processWithExceptionHandling(node.getArguments().get(1), context);
                    Object falseValue = processWithExceptionHandling(node.getArguments().get(2), context);

                    if (condition instanceof RowExpression) {
                        return new SpecialForm(
                                IF,
                                node.getType(),
                                toRowExpression(condition, node.getArguments().get(0).getType()),
                                toRowExpression(trueValue, node.getArguments().get(1).getType()),
                                toRowExpression(falseValue, node.getArguments().get(2).getType()));
                    }
                    else if (Boolean.TRUE.equals(condition)) {
                        return trueValue;
                    }
                    return falseValue;
                }
                case NULL_IF: {
                    checkArgument(node.getArguments().size() == 2);
                    Object left = processWithExceptionHandling(node.getArguments().get(0), context);
                    if (left == null) {
                        return null;
                    }

                    Object right = processWithExceptionHandling(node.getArguments().get(1), context);
                    if (right == null) {
                        return left;
                    }

                    if (hasUnresolvedValue(left, right)) {
                        return new SpecialForm(
                                NULL_IF,
                                node.getType(),
                                toRowExpression(left, node.getArguments().get(0).getType()),
                                toRowExpression(right, node.getArguments().get(1).getType()));
                    }

                    Type firstType = node.getArguments().get(0).getType();
                    Type secondType = node.getArguments().get(1).getType();
                    Type commonType = typeCoercion.getCommonSuperType(firstType, secondType).get();
                    ResolvedFunction firstCast = metadata.getCoercion(session, firstType, commonType);
                    ResolvedFunction secondCast = metadata.getCoercion(session, secondType, commonType);

                    // cast(first as <common type>) == cast(second as <common type>)
                    boolean equal = Boolean.TRUE.equals(invokeOperator(
                            OperatorType.EQUAL,
                            ImmutableList.of(commonType, commonType),
                            ImmutableList.of(
                                    functionInvoker.invoke(firstCast, connectorSession, left),
                                    functionInvoker.invoke(secondCast, connectorSession, right))));

                    if (equal) {
                        return null;
                    }
                    return left;
                }
                case IS_NULL: {
                    checkArgument(node.getArguments().size() == 1);
                    Object value = processWithExceptionHandling(node.getArguments().get(0), context);
                    if (value instanceof RowExpression) {
                        return new SpecialForm(
                                IS_NULL,
                                node.getType(),
                                toRowExpression(value, node.getArguments().get(0).getType()));
                    }
                    return value == null;
                }
                case AND: {
                    Object left = node.getArguments().get(0).accept(this, context);
                    Object right;

                    if (Boolean.FALSE.equals(left)) {
                        return false;
                    }

                    right = node.getArguments().get(1).accept(this, context);

                    if (Boolean.TRUE.equals(right)) {
                        return left;
                    }

                    if (Boolean.FALSE.equals(right) || Boolean.TRUE.equals(left)) {
                        return right;
                    }

                    if (left == null && right == null) {
                        return null;
                    }
                    return new SpecialForm(
                            AND,
                            node.getType(),
                            toRowExpressions(
                                    asList(left, right),
                                    ImmutableList.of(node.getArguments().get(0).getType(), node.getArguments().get(1).getType())));
                }
                case OR: {
                    Object left = node.getArguments().get(0).accept(this, context);
                    Object right;

                    if (Boolean.TRUE.equals(left)) {
                        return true;
                    }

                    right = node.getArguments().get(1).accept(this, context);

                    if (Boolean.FALSE.equals(right)) {
                        return left;
                    }

                    if (Boolean.TRUE.equals(right) || Boolean.FALSE.equals(left)) {
                        return right;
                    }

                    if (left == null && right == null) {
                        return null;
                    }
                    return new SpecialForm(
                            OR,
                            node.getType(),
                            toRowExpressions(
                                    asList(left, right),
                                    ImmutableList.of(node.getArguments().get(0).getType(), node.getArguments().get(1).getType())));
                }
                case ROW_CONSTRUCTOR: {
                    RowType rowType = (RowType) node.getType();
                    List<Type> parameterTypes = rowType.getTypeParameters();
                    List<RowExpression> arguments = node.getArguments();

                    int cardinality = arguments.size();
                    List<Object> values = new ArrayList<>(cardinality);
                    arguments.forEach(argument -> values.add(processWithExceptionHandling(argument, context)));
                    if (hasUnresolvedValue(values)) {
                        return new SpecialForm(ROW_CONSTRUCTOR, node.getType(), toRowExpressions(values, parameterTypes));
                    }
                    else {
                        BlockBuilder blockBuilder = new RowBlockBuilder(parameterTypes, null, 1);
                        BlockBuilder singleRowBlockWriter = blockBuilder.beginBlockEntry();
                        for (int i = 0; i < cardinality; ++i) {
                            writeNativeValue(parameterTypes.get(i), singleRowBlockWriter, values.get(i));
                        }
                        blockBuilder.closeEntry();
                        return rowType.getObject(blockBuilder, 0);
                    }
                }
                case COALESCE: {
                    List<Object> newOperands = processOperands(node, context);
                    if (newOperands.isEmpty()) {
                        return null;
                    }
                    if (newOperands.size() == 1) {
                        return getOnlyElement(newOperands);
                    }
                    return new SpecialForm(COALESCE, node.getType(), newOperands.stream().map(value -> toRowExpression(value, node.getType())).collect(toImmutableList()));
                }
                case IN: {
                    checkArgument(node.getArguments().size() >= 2, "values must not be empty");

                    Object target = processWithExceptionHandling(node.getArguments().get(0), context);

                    List<RowExpression> valueList = node.getArguments().subList(1, node.getArguments().size());
//                    List<Object> values = valueList.stream().map(value -> value.accept(this, context)).collect(toList());
//                    List<Type> valuesTypes = valueList.stream().map(RowExpression::getType).collect(toImmutableList());

                    if (target == null) {
                        return null;
                    }

                    boolean hasUnresolvedValue = target instanceof RowExpression;
                    boolean hasNullValue = false;
                    boolean found = false;
                    List<Object> values = new ArrayList<>(valueList.size());
                    List<Type> types = new ArrayList<>(valueList.size());

                    for (RowExpression expression : valueList) {
                        if (target instanceof RowExpression && expression instanceof ConstantExpression) {
                            // skip interpreting of literal IN term since it cannot be compared
                            // with unresolved "value" and it cannot be simplified further
                            values.add(expression);
                            types.add(expression.getType());
                            continue;
                        }

                        // Use process() instead of processWithExceptionHandling() for processing in-list items.
                        // Do not handle exceptions thrown while processing a single in-list expression,
                        // but fail the whole in-predicate evaluation.
                        // According to in-predicate semantics, all in-list items must be successfully evaluated
                        // before a check for the match is performed.
                        Object inValue = expression.accept(this, context);
                        if (target instanceof RowExpression || inValue instanceof RowExpression) {
                            hasUnresolvedValue = true;
                            values.add(inValue);
                            types.add(expression.getType());
                            continue;
                        }

                        if (inValue == null) {
                            hasNullValue = true;
                        }
                        else {
                            ResolvedFunction equalsOperator = metadata.resolveOperator(session, OperatorType.EQUAL, ImmutableList.of(node.getArguments().get(0).getType(), expression.getType()));
                            Boolean result = (Boolean) functionInvoker.invoke(equalsOperator, connectorSession, ImmutableList.of(target, inValue));
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
                        Type type = node.getType();
                        List<RowExpression> expressionValues = toRowExpressions(values, types);
                        List<RowExpression> simplifiedExpressionValues = Stream.concat(
                                        expressionValues.stream()
                                                .filter(DeterminismEvaluator::isDeterministic)
                                                .distinct(),
                                        expressionValues.stream()
                                                .filter(DeterminismEvaluator::isDeterministic))
                                .collect(toImmutableList());

                        if (simplifiedExpressionValues.size() == 1) {
                            return new CallExpression(resolution.comparisonFunction(ComparisonExpression.Operator.EQUAL, type, simplifiedExpressionValues.get(0).getType()), ImmutableList.of(toRowExpression(target, type), simplifiedExpressionValues.get(0)));
                        }
                        return new SpecialForm(IN, node.getType(), simplifiedExpressionValues);
                    }
                    if (hasNullValue) {
                        return null;
                    }
                    return false;
                }
                case DEREFERENCE: {
                    checkArgument(node.getArguments().size() == 2);

                    Object base = node.getArguments().get(0).accept(this, context);
                    int index = ((Number) node.getArguments().get(1).accept(this, context)).intValue();

                    // if the base part is evaluated to be null, the dereference expression should also be null
                    if (base == null) {
                        return null;
                    }

                    if (hasUnresolvedValue(base)) {
                        return new SpecialForm(
                                DEREFERENCE,
                                node.getType(),
                                toRowExpression(base, node.getArguments().get(0).getType()),
                                toRowExpression((long) index, node.getArguments().get(1).getType()));
                    }
                    return readNativeValue(node.getType(), (Block) base, index);
                }
                case BIND: {
                    checkArgument(node.getArguments().size() >= 2);
                    List<RowExpression> values = node.getArguments().subList(0, node.getArguments().size() - 1);

                    List<Object> valuesProcessed = node.getArguments().subList(0, node.getArguments().size() - 1).stream()
                            .map(value -> processWithExceptionHandling(value, context))
                            .collect(toList()); // values are nullable
                    Object function = processWithExceptionHandling(node.getArguments().get(node.getArguments().size() - 1), context);

                    if (hasUnresolvedValue(values) || hasUnresolvedValue(function)) {
                        ImmutableList.Builder<RowExpression> builder = ImmutableList.builder();
                        for (int i = 0; i < values.size(); i++) {
                            builder.add(toRowExpression(valuesProcessed.get(i), values.get(i).getType()));
                        }

                        return new SpecialForm(BIND, node.getType(), builder.build());
                    }

                    return MethodHandles.insertArguments((MethodHandle) function, 0, valuesProcessed.toArray());
                }
                case SWITCH: {
                    // arguments: operand: whenClauseList: defaultValue
                    int argumentSize = node.getArguments().size();
                    RowExpression operand = node.getArguments().get(0);
                    RowExpression defaultValue = node.getArguments().get(node.getArguments().size() - 1);
                    List<RowExpression> whenClauses = node.getArguments().subList(1, argumentSize - 1);

                    Object operandProcessed = processWithExceptionHandling(node.getArguments().get(0), context);
                    Type operandType = node.getArguments().get(0).getType();

                    // if operand is null, return defaultValue

                    if (operandProcessed == null) {
                        return processWithExceptionHandling(defaultValue, context);
                    }

                    Object newDefault = null;
                    boolean foundNewDefault = false;

                    List<RowExpression> simplifiedWhenClauses = new ArrayList<>();
                    for (RowExpression whenClause : whenClauses) {
                        checkArgument(whenClause instanceof SpecialForm && ((SpecialForm) whenClause).getForm().equals(WHEN));
                        Object whenOperandProcessed = processWithExceptionHandling(((SpecialForm) whenClause).getArguments().get(0), context);

                        if (whenOperandProcessed instanceof RowExpression || operandProcessed instanceof RowExpression) {
                            // cannot fully evaluate, add updated whenClause
                            simplifiedWhenClauses.add(new SpecialForm(WHEN, whenClause.getType(),
                                    toRowExpression(whenOperandProcessed, ((SpecialForm) whenClause).getArguments().get(0).getType()),
                                    toRowExpression(processWithExceptionHandling(((SpecialForm) whenClause).getArguments().get(1), context), ((SpecialForm) whenClause).getArguments().get(1).getType())));
                        }
                        else if (whenOperandProcessed != null && isEqual(operandProcessed, operandType, whenOperandProcessed, ((SpecialForm) whenClause).getArguments().get(0).getType())) {
                            // condition is true, use this as default
                            foundNewDefault = true;
                            newDefault = processWithExceptionHandling(((SpecialForm) whenClause).getArguments().get(1), context);
                            break;
                        }
                    }

                    Object defaultResult;
                    if (foundNewDefault) {
                        defaultResult = newDefault;
                    }
                    else {
                        defaultResult = processWithExceptionHandling(defaultValue, context);
                    }

                    if (simplifiedWhenClauses.isEmpty()) {
                        return defaultResult;
                    }
                    RowExpression defaultExpression = (defaultResult == null) ? constantNull(node.getType()) : toRowExpression(defaultResult, node.getType());

                    ImmutableList.Builder<RowExpression> argumentsBuilder = ImmutableList.builder();
                    argumentsBuilder.add(toRowExpression(operandProcessed, operand.getType()))
                            .addAll(simplifiedWhenClauses)
                            .add(defaultExpression);

                    return new SpecialForm(SWITCH, node.getType(), argumentsBuilder.build());
                }
                default:
                    throw new IllegalStateException("Can not compile special form: " + node.getForm());
            }
        }

        private boolean isEqual(Object operand1, Type type1, Object operand2, Type type2)
        {
            return Boolean.TRUE.equals(invokeOperator(OperatorType.EQUAL, ImmutableList.of(type1, type2), ImmutableList.of(operand1, operand2)));
        }

        private List<Object> processOperands(SpecialForm node, Object context)
        {
            checkArgument(node.getForm() == COALESCE);
            List<Object> newOperands = new ArrayList<>();
            Set<RowExpression> uniqueNewOperands = new HashSet<>();
            for (RowExpression operand : node.getArguments()) {
                Object value = processWithExceptionHandling(operand, context);
                if (value instanceof SpecialForm && ((SpecialForm) value).getForm() == COALESCE) {
                    // The nested CoalesceExpression was recursively processed. It does not contain null.
                    for (RowExpression nestedOperand : ((SpecialForm) value).getArguments()) {
                        // Skip duplicates unless they are non-deterministic.
                        if (!isDeterministic(nestedOperand) || uniqueNewOperands.add(nestedOperand)) {
                            newOperands.add(nestedOperand);
                        }
                        // This operand can be evaluated to a non-null value. Remaining operands can be skipped.
                        if (isEffectivelyLiteral(plannerContext, session, nestedOperand)) {
                            verify(
                                    !(nestedOperand.getType() == UnknownType.UNKNOWN) && !notCastOnNullLiteral(nestedOperand),
                                    "Null operand should have been removed by recursive coalesce processing");
                            return newOperands;
                        }
                    }
                }
                else if (value instanceof RowExpression) {
                    verify(!((value instanceof ConstantExpression) && (((RowExpression) value).getType() == UnknownType.UNKNOWN)), "Null value is expected to be represented as null, not NullLiteral");
                    // Skip duplicates unless they are non-deterministic.
                    RowExpression expression = (RowExpression) value;
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

        private boolean notCastOnNullLiteral(RowExpression expression)
        {
            if (!(expression instanceof CallExpression)) {
                return true;
            }
            CallExpression callExpression = (CallExpression) expression;
            ResolvedFunction function = callExpression.getResolvedFunction();
            if (!isCastFunction(function)) {
                return true;
            }
            RowExpression nestedOperand = Iterables.getOnlyElement(callExpression.getArguments());
            return (nestedOperand instanceof ConstantExpression) && (nestedOperand.getType() != UnknownType.UNKNOWN);
        }

        private Object processWithExceptionHandling(RowExpression expression, Object context)
        {
            if (expression == null) {
                return null;
            }
            try {
                return expression.accept(this, context);
            }
            catch (RuntimeException e) {
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

        private boolean hasUnresolvedValue(Object... values)
        {
            return hasUnresolvedValue(ImmutableList.copyOf(values));
        }

        private boolean hasUnresolvedValue(List<Object> values)
        {
            return values.stream().anyMatch(instanceOf(RowExpression.class));
        }

        private Object invokeOperator(OperatorType operatorType, List<? extends Type> argumentTypes, List<Object> argumentValues)
        {
            ResolvedFunction operator = metadata.resolveOperator(session, operatorType, argumentTypes);
            return functionInvoker.invoke(operator, connectorSession, argumentValues);
        }

        private RowExpression toRowExpression(Object base, Type type)
        {
            return literalEncoder.toRowExpression(base, type);
        }

        private List<RowExpression> toRowExpressions(List<Object> values, List<Type> types)
        {
            return literalEncoder.toRowExpressions(values, types);
        }
    }
}
