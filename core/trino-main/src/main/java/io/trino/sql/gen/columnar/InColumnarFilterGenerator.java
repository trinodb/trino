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
package io.trino.sql.gen.columnar;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Primitives;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.ForLoop;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.control.SwitchStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.bytecode.instruction.LabelNode;
import io.airlift.slice.Slice;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.project.InputChannels;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.Binding;
import io.trino.sql.gen.CallSiteBinder;
import io.trino.sql.gen.InCodeGenerator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.util.FastutilSetHelper;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.control.SwitchStatement.switchBuilder;
import static io.airlift.bytecode.expression.BytecodeExpressions.add;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantTrue;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.lessThan;
import static io.airlift.bytecode.instruction.JumpInstruction.jump;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.sql.gen.BytecodeUtils.loadConstant;
import static io.trino.sql.gen.SqlTypeBytecodeExpression.constantType;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.createClassInstance;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.declareBlockVariables;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.generateBlockMayHaveNull;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.generateBlockPositionNotNull;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.generateGetInputChannels;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.updateOutputPositions;
import static io.trino.util.CompilerUtils.makeClassName;
import static io.trino.util.FastutilSetHelper.toFastutilHashSet;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class InColumnarFilterGenerator
{
    private final Reference valueReference;
    private final Map<Symbol, Integer> layout;
    private final int valueChannel;
    private final boolean useSwitchCase;
    private final Set<Object> constantValues;

    private final MethodHandle equalsMethodHandle;
    private final MethodHandle hashCodeMethodHandle;

    public InColumnarFilterGenerator(In in, Map<Symbol, Integer> layout, Metadata metadata, FunctionManager functionManager)
    {
        checkArgument(!in.valueList().isEmpty(), "At least one value is required in IN list");
        if (!(in.value() instanceof Reference)) {
            throw new UnsupportedOperationException("IN clause columnar evaluation is supported only on input references");
        }
        valueReference = (Reference) in.value();
        this.layout = requireNonNull(layout, "layout is null");
        Integer channel = layout.get(Symbol.from(valueReference));
        checkState(channel != null, "Reference not in layout: %s", valueReference.name());
        valueChannel = channel;
        List<Expression> expressions = in.valueList();
        expressions.forEach(expression -> {
            if (!(expression instanceof Constant)) {
                throw new UnsupportedOperationException("IN clause columnar evaluation is supported only on input reference against constants");
            }
        });
        List<Constant> testExpressions = expressions.stream()
                .map(Constant.class::cast)
                .collect(toImmutableList());

        Type valueType = valueReference.type();
        ResolvedFunction resolvedEqualsFunction = metadata.resolveOperator(EQUAL, ImmutableList.of(valueType, valueType));
        ResolvedFunction resolvedHashCodeFunction = metadata.resolveOperator(HASH_CODE, ImmutableList.of(valueType));
        ResolvedFunction resolvedIsIndeterminate = metadata.resolveOperator(INDETERMINATE, ImmutableList.of(valueType));
        equalsMethodHandle = functionManager.getScalarFunctionImplementation(resolvedEqualsFunction, simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL)).getMethodHandle();
        hashCodeMethodHandle = functionManager.getScalarFunctionImplementation(resolvedHashCodeFunction, simpleConvention(FAIL_ON_NULL, NEVER_NULL)).getMethodHandle();
        MethodHandle indeterminateMethodHandle = functionManager.getScalarFunctionImplementation(resolvedIsIndeterminate, simpleConvention(FAIL_ON_NULL, NEVER_NULL)).getMethodHandle();

        ImmutableSet.Builder<Object> constantValuesBuilder = ImmutableSet.builder();
        for (Constant testValue : testExpressions) {
            if (isDeterminateConstant(testValue, indeterminateMethodHandle)) {
                constantValuesBuilder.add(testValue.value());
            }
        }
        constantValues = constantValuesBuilder.build();
        useSwitchCase = useSwitchCaseGeneration(valueType, expressions);
    }

    public Class<? extends ColumnarFilter> generateColumnarFilter()
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(ColumnarFilter.class.getSimpleName() + "_in", Optional.empty()),
                type(Object.class),
                type(ColumnarFilter.class));
        CallSiteBinder callSiteBinder = new CallSiteBinder();

        FieldDefinition inputChannelsField = generateGetInputChannels(classDefinition);
        generateConstructor(classDefinition, inputChannelsField);

        Type valueType = valueReference.type();
        Set<?> constantValuesSet = toFastutilHashSet(constantValues, valueType, hashCodeMethodHandle, equalsMethodHandle);
        Binding constant = callSiteBinder.bind(constantValuesSet, constantValuesSet.getClass());

        generateFilterRangeMethod(callSiteBinder, classDefinition, constantValuesSet, constant);
        generateFilterListMethod(callSiteBinder, classDefinition, constantValuesSet, constant);

        return createClassInstance(callSiteBinder, classDefinition);
    }

    private static void generateConstructor(ClassDefinition classDefinition, FieldDefinition inputChannelsField)
    {
        Parameter inputChannelsParam = arg("inputChannels", InputChannels.class);
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC), inputChannelsParam);

        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        body.append(thisVariable.setField(inputChannelsField, inputChannelsParam));
        body.ret();
    }

    private void generateFilterRangeMethod(CallSiteBinder binder, ClassDefinition classDefinition, Set<?> constantValuesSet, Binding constant)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter outputPositions = arg("outputPositions", int[].class);
        Parameter offset = arg("offset", int.class);
        Parameter size = arg("size", int.class);
        Parameter page = arg("page", SourcePage.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filterPositionsRange",
                type(int.class),
                ImmutableList.of(session, outputPositions, offset, size, page));
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        declareBlockVariables(ImmutableList.of(valueReference), layout, page, scope, body);

        Variable outputPositionsCount = scope.declareVariable("outputPositionsCount", body, constantInt(0));
        Variable position = scope.declareVariable(int.class, "position");
        Variable result = scope.declareVariable(boolean.class, "result");

        IfStatement ifStatement = new IfStatement()
                .condition(generateBlockMayHaveNull(ImmutableList.of(valueReference), layout, scope));
        body.append(ifStatement);

        ifStatement.ifTrue(new ForLoop("nullable range based loop")
                .initialize(position.set(offset))
                .condition(lessThan(position, add(offset, size)))
                .update(position.increment())
                .body(new IfStatement()
                        .condition(generateBlockPositionNotNull(ImmutableList.of(valueReference), layout, scope, position))
                        .ifTrue(new BytecodeBlock()
                                .append(generateSetContainsCall(binder, scope, constantValuesSet, constant, position, result))
                                .append(updateOutputPositions(result, position, outputPositions, outputPositionsCount)))));

        ifStatement.ifFalse(new ForLoop("non-nullable range based loop")
                .initialize(position.set(offset))
                .condition(lessThan(position, add(offset, size)))
                .update(position.increment())
                .body(new BytecodeBlock()
                        .append(generateSetContainsCall(binder, scope, constantValuesSet, constant, position, result))
                        .append(updateOutputPositions(result, position, outputPositions, outputPositionsCount))));

        body.append(outputPositionsCount.ret());
    }

    private void generateFilterListMethod(CallSiteBinder binder, ClassDefinition classDefinition, Set<?> constantValuesSet, Binding constant)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter outputPositions = arg("outputPositions", int[].class);
        Parameter activePositions = arg("activePositions", int[].class);
        Parameter offset = arg("offset", int.class);
        Parameter size = arg("size", int.class);
        Parameter page = arg("page", SourcePage.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filterPositionsList",
                type(int.class),
                ImmutableList.of(session, outputPositions, activePositions, offset, size, page));
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        declareBlockVariables(ImmutableList.of(valueReference), layout, page, scope, body);

        Variable outputPositionsCount = scope.declareVariable("outputPositionsCount", body, constantInt(0));
        Variable index = scope.declareVariable(int.class, "index");
        Variable position = scope.declareVariable(int.class, "position");
        Variable result = scope.declareVariable(boolean.class, "result");

        IfStatement ifStatement = new IfStatement()
                .condition(generateBlockMayHaveNull(ImmutableList.of(valueReference), layout, scope));
        body.append(ifStatement);

        ifStatement.ifTrue(new ForLoop("nullable positions loop")
                .initialize(index.set(offset))
                .condition(lessThan(index, add(offset, size)))
                .update(index.increment())
                .body(new BytecodeBlock()
                        .append(position.set(activePositions.getElement(index)))
                        .append(new IfStatement()
                                .condition(generateBlockPositionNotNull(ImmutableList.of(valueReference), layout, scope, position))
                                .ifTrue(new BytecodeBlock()
                                        .append(generateSetContainsCall(binder, scope, constantValuesSet, constant, position, result))
                                        .append(updateOutputPositions(result, position, outputPositions, outputPositionsCount))))));

        ifStatement.ifFalse(new ForLoop("non-nullable positions loop")
                .initialize(index.set(offset))
                .condition(lessThan(index, add(offset, size)))
                .update(index.increment())
                .body(new BytecodeBlock()
                        .append(position.set(activePositions.getElement(index)))
                        .append(generateSetContainsCall(binder, scope, constantValuesSet, constant, position, result))
                        .append(updateOutputPositions(result, position, outputPositions, outputPositionsCount))));

        body.append(outputPositionsCount.ret());
    }

    private BytecodeBlock generateSetContainsCall(CallSiteBinder binder, Scope scope, Set<?> constantValuesSet, Binding constant, BytecodeExpression position, Variable result)
    {
        Type valueType = valueReference.type();
        Class<?> javaType = valueType.getJavaType();

        Class<?> callType = javaType;
        if (!callType.isPrimitive() && callType != Slice.class) {
            callType = Object.class;
        }
        String methodName = "get" + Primitives.wrap(callType).getSimpleName();
        BytecodeExpression value = constantType(binder, valueType)
                .invoke(methodName, callType, scope.getVariable("block_" + valueChannel), position);
        if (callType != javaType) {
            value = value.cast(javaType);
        }

        if (useSwitchCase) {
            // A white-list is used to select types eligible for DIRECT_SWITCH.
            // For these types, it's safe to not use Trino HASH_CODE and EQUAL operator.
            LabelNode end = new LabelNode("end");
            LabelNode match = new LabelNode("match");
            LabelNode defaultLabel = new LabelNode("default");

            SwitchStatement.SwitchBuilder switchBuilder = switchBuilder();
            BytecodeBlock matchBlock = new BytecodeBlock()
                    .setDescription("match")
                    .visitLabel(match)
                    .append(result.set(constantTrue()))
                    .gotoLabel(end);
            BytecodeBlock defaultCaseBlock = new BytecodeBlock()
                    .setDescription("default")
                    .visitLabel(defaultLabel)
                    .append(result.set(constantFalse()))
                    .gotoLabel(end);
            for (Object constantValue : constantValues) {
                switchBuilder.addCase(toIntExact((Long) constantValue), jump(match));
            }
            switchBuilder.defaultCase(jump(defaultLabel));

            Variable expression = scope.createTempVariable(javaType);
            return new BytecodeBlock()
                    .comment("lookupSwitch(<stackValue>))")
                    .append(expression.set(value))
                    .append(new IfStatement()
                            .condition(invokeStatic(InCodeGenerator.class, "isInteger", boolean.class, expression))
                            .ifFalse(new BytecodeBlock()
                                    .gotoLabel(defaultLabel)))
                    .append(switchBuilder.expression(expression.cast(int.class)).build())
                    .append(matchBlock)
                    .append(defaultCaseBlock)
                    .visitLabel(end);
        }
        return new BytecodeBlock()
                .comment("inListSet.contains(<stackValue>)")
                .append(new BytecodeBlock()
                        .comment("value")
                        .append(value)
                        .comment("set")
                        .append(loadConstant(constant))
                        .invokeStatic(FastutilSetHelper.class, "in", boolean.class, javaType.isPrimitive() ? javaType : Object.class, constantValuesSet.getClass())
                        .putVariable(result));
    }

    private static boolean isDeterminateConstant(Expression expression, MethodHandle isIndeterminateFunction)
    {
        if (!(expression instanceof Constant constantExpression)) {
            return false;
        }
        Object value = constantExpression.value();
        // NULL constants are skipped as they do not satisfy IN filter
        // NULL positions will need to be handled differently to allow IN filters to be composed (e.g. NOT IN)
        if (value == null) {
            return false;
        }
        try {
            return !(boolean) isIndeterminateFunction.invoke(value);
        }
        catch (Throwable t) {
            throwIfUnchecked(t);
            throw new RuntimeException(t);
        }
    }

    static boolean useSwitchCaseGeneration(Type type, List<Expression> values)
    {
        // FastutilSetHelper#in does not work correctly for indeterminate values stored in structural types
        // https://github.com/trinodb/trino/issues/17213
        // Until we support HASH_SWITCH strategy for code generation here, we treat structural type as an unsupported case
        // and fall back to existing expression evaluator for small lists
        if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
            throw new UnsupportedOperationException("Structural type not supported");
        }
        if (values.size() >= 8) {
            // Lookup in Set is generally faster than switch case for not super tiny IN lists.
            // Tipping point is between 5 and 10 (using round 8)
            return false;
        }

        if (type.getJavaType() != long.class) {
            return false;
        }
        for (Expression expression : values) {
            if (!(expression instanceof Constant constantExpression)) {
                throw new UnsupportedOperationException("IN clause columnar evaluation is supported only on input reference against constants");
            }
            Object constant = constantExpression.value();
            // NULL constants are skipped as they do not satisfy IN filter
            // NULL positions will need to be handled differently to allow IN filters to be composed (e.g. NOT IN)
            if (constant == null) {
                continue;
            }
            long longConstant = ((Number) constant).longValue();
            if (longConstant < Integer.MIN_VALUE || longConstant > Integer.MAX_VALUE) {
                return false;
            }
        }
        return true;
    }
}
