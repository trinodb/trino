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
package io.trino.sql.gen;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.control.SwitchStatement.SwitchBuilder;
import io.airlift.bytecode.instruction.LabelNode;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.type.Type;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;
import io.trino.util.FastutilSetHelper;

import java.lang.invoke.MethodHandle;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantTrue;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.airlift.bytecode.instruction.JumpInstruction.jump;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.sql.gen.BytecodeUtils.ifWasNullPopAndGoto;
import static io.trino.sql.gen.BytecodeUtils.invoke;
import static io.trino.sql.gen.BytecodeUtils.loadConstant;
import static io.trino.util.FastutilSetHelper.toFastutilHashSet;
import static java.lang.Math.toIntExact;

public class InCodeGenerator
        implements BytecodeGenerator
{
    private final RowExpression valueExpression;
    private final List<RowExpression> testExpressions;

    private final ResolvedFunction resolvedEqualsFunction;
    private final ResolvedFunction resolvedHashCodeFunction;
    private final ResolvedFunction resolvedIsIndeterminate;

    public InCodeGenerator(SpecialForm specialForm)
    {
        checkArgument(specialForm.arguments().size() >= 2, "At least two arguments are required");
        valueExpression = specialForm.arguments().get(0);
        testExpressions = specialForm.arguments().subList(1, specialForm.arguments().size());

        checkArgument(specialForm.functionDependencies().size() == 3);
        resolvedEqualsFunction = specialForm.getOperatorDependency(EQUAL);
        resolvedHashCodeFunction = specialForm.getOperatorDependency(HASH_CODE);
        resolvedIsIndeterminate = specialForm.getOperatorDependency(INDETERMINATE);
    }

    enum SwitchGenerationCase
    {
        DIRECT_SWITCH,
        HASH_SWITCH,
        SET_CONTAINS
    }

    @VisibleForTesting
    static SwitchGenerationCase checkSwitchGenerationCase(Type type, List<RowExpression> values)
    {
        if (values.size() >= 8) {
            // SET_CONTAINS is generally faster for not super tiny IN lists.
            // Tipping point is between 5 and 10 (using round 8)
            return SwitchGenerationCase.SET_CONTAINS;
        }

        if (type.getJavaType() != long.class) {
            return SwitchGenerationCase.HASH_SWITCH;
        }
        for (RowExpression expression : values) {
            // For non-constant expressions, they will be added to the default case in the generated switch code. They do not affect any of
            // the cases other than the default one. Therefore, it's okay to skip them when choosing between DIRECT_SWITCH and HASH_SWITCH.
            // Same argument applies for nulls.
            if (!(expression instanceof ConstantExpression constantExpression)) {
                continue;
            }
            Object constant = constantExpression.value();
            if (constant == null) {
                continue;
            }
            long longConstant = ((Number) constant).longValue();
            if (longConstant < Integer.MIN_VALUE || longConstant > Integer.MAX_VALUE) {
                return SwitchGenerationCase.HASH_SWITCH;
            }
        }
        return SwitchGenerationCase.DIRECT_SWITCH;
    }

    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext generatorContext)
    {
        Type type = valueExpression.type();
        Class<?> javaType = type.getJavaType();

        SwitchGenerationCase switchGenerationCase = checkSwitchGenerationCase(type, testExpressions);

        MethodHandle equalsMethodHandle = generatorContext.getScalarFunctionImplementation(resolvedEqualsFunction, simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL)).getMethodHandle();
        MethodHandle hashCodeMethodHandle = generatorContext.getScalarFunctionImplementation(resolvedHashCodeFunction, simpleConvention(FAIL_ON_NULL, NEVER_NULL)).getMethodHandle();
        MethodHandle indeterminateMethodHandle = generatorContext.getScalarFunctionImplementation(resolvedIsIndeterminate, simpleConvention(FAIL_ON_NULL, NEVER_NULL)).getMethodHandle();

        ImmutableListMultimap.Builder<Integer, BytecodeNode> hashBucketsBuilder = ImmutableListMultimap.builder();
        ImmutableList.Builder<BytecodeNode> defaultBucket = ImmutableList.builder();
        ImmutableSet.Builder<Object> constantValuesBuilder = ImmutableSet.builder();

        for (RowExpression testValue : testExpressions) {
            BytecodeNode testBytecode = generatorContext.generate(testValue);

            if (isDeterminateConstant(testValue, indeterminateMethodHandle)) {
                ConstantExpression constant = (ConstantExpression) testValue;
                Object object = constant.value();
                switch (switchGenerationCase) {
                    case DIRECT_SWITCH:
                    case SET_CONTAINS:
                        constantValuesBuilder.add(object);
                        break;
                    case HASH_SWITCH:
                        try {
                            int hashCode = Long.hashCode((Long) hashCodeMethodHandle.invoke(object));
                            hashBucketsBuilder.put(hashCode, testBytecode);
                        }
                        catch (Throwable throwable) {
                            throw new IllegalArgumentException("Error processing IN statement: error calculating hash code for " + object, throwable);
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Not supported switch generation case: " + switchGenerationCase);
                }
            }
            else {
                defaultBucket.add(testBytecode);
            }
        }
        ImmutableListMultimap<Integer, BytecodeNode> hashBuckets = hashBucketsBuilder.build();
        Set<Object> constantValues = constantValuesBuilder.build();

        LabelNode end = new LabelNode("end");
        LabelNode match = new LabelNode("match");
        LabelNode noMatch = new LabelNode("noMatch");

        LabelNode defaultLabel = new LabelNode("default");

        Scope scope = generatorContext.getScope();
        Variable value = scope.getOrCreateTempVariable(javaType);

        BytecodeNode switchBlock;
        Variable expression = scope.getOrCreateTempVariable(int.class);
        SwitchBuilder switchBuilder = new SwitchBuilder().expression(expression);

        switch (switchGenerationCase) {
            case DIRECT_SWITCH:
                // A white-list is used to select types eligible for DIRECT_SWITCH.
                // For these types, it's safe to not use Trino HASH_CODE and EQUAL operator.
                for (Object constantValue : constantValues) {
                    switchBuilder.addCase(toIntExact((Long) constantValue), jump(match));
                }
                switchBuilder.defaultCase(jump(defaultLabel));
                switchBlock = new BytecodeBlock()
                        .comment("lookupSwitch(<stackValue>))")
                        .append(new IfStatement()
                                .condition(invokeStatic(InCodeGenerator.class, "isInteger", boolean.class, value))
                                .ifFalse(new BytecodeBlock()
                                        .gotoLabel(defaultLabel)))
                        .append(expression.set(value.cast(int.class)))
                        .append(switchBuilder.build());
                break;
            case HASH_SWITCH:
                for (Map.Entry<Integer, Collection<BytecodeNode>> bucket : hashBuckets.asMap().entrySet()) {
                    Collection<BytecodeNode> testValues = bucket.getValue();
                    BytecodeBlock caseBlock = buildInCase(
                            generatorContext,
                            scope,
                            resolvedEqualsFunction,
                            match,
                            defaultLabel,
                            value,
                            testValues,
                            false,
                            resolvedIsIndeterminate);
                    switchBuilder.addCase(bucket.getKey(), caseBlock);
                }
                switchBuilder.defaultCase(jump(defaultLabel));
                Binding hashCodeBinding = generatorContext
                        .getCallSiteBinder()
                        .bind(hashCodeMethodHandle);
                switchBlock = new BytecodeBlock()
                        .comment("lookupSwitch(hashCode(<stackValue>))")
                        .getVariable(value)
                        .append(invoke(hashCodeBinding, resolvedHashCodeFunction.signature()))
                        .invokeStatic(Long.class, "hashCode", int.class, long.class)
                        .putVariable(expression)
                        .append(switchBuilder.build());
                break;
            case SET_CONTAINS:
                Set<?> constantValuesSet = toFastutilHashSet(constantValues, type, hashCodeMethodHandle, equalsMethodHandle);
                Binding constant = generatorContext.getCallSiteBinder().bind(constantValuesSet, constantValuesSet.getClass());

                switchBlock = new BytecodeBlock()
                        .comment("inListSet.contains(<stackValue>)")
                        .append(new IfStatement()
                                .condition(new BytecodeBlock()
                                        .comment("value")
                                        .getVariable(value)
                                        .comment("set")
                                        .append(loadConstant(constant))
                                        // TODO: use invokeVirtual on the set instead. This requires swapping the two elements in the stack
                                        .invokeStatic(FastutilSetHelper.class, "in", boolean.class, javaType.isPrimitive() ? javaType : Object.class, constantValuesSet.getClass()))
                                .ifTrue(jump(match)));
                break;
            default:
                throw new IllegalArgumentException("Not supported switch generation case: " + switchGenerationCase);
        }

        BytecodeBlock defaultCaseBlock = buildInCase(
                generatorContext,
                scope,
                resolvedEqualsFunction,
                match,
                noMatch,
                value,
                defaultBucket.build(),
                true,
                resolvedIsIndeterminate)
                .setDescription("default");

        BytecodeBlock block = new BytecodeBlock()
                .comment("IN")
                .append(generatorContext.generate(valueExpression))
                .append(ifWasNullPopAndGoto(scope, end, boolean.class, javaType))
                .putVariable(value)
                .append(switchBlock)
                .visitLabel(defaultLabel)
                .append(defaultCaseBlock);

        BytecodeBlock matchBlock = new BytecodeBlock()
                .setDescription("match")
                .visitLabel(match)
                .append(generatorContext.wasNull().set(constantFalse()))
                .push(true)
                .gotoLabel(end);
        block.append(matchBlock);

        BytecodeBlock noMatchBlock = new BytecodeBlock()
                .setDescription("noMatch")
                .visitLabel(noMatch)
                .push(false)
                .gotoLabel(end);
        block.append(noMatchBlock);

        block.visitLabel(end);

        scope.releaseTempVariableForReuse(expression);
        scope.releaseTempVariableForReuse(value);

        return block;
    }

    public static boolean isInteger(long value)
    {
        return value == (int) value;
    }

    private static BytecodeBlock buildInCase(
            BytecodeGeneratorContext generatorContext,
            Scope scope,
            ResolvedFunction equals,
            LabelNode matchLabel,
            LabelNode noMatchLabel,
            Variable value,
            Collection<BytecodeNode> testValues,
            boolean checkForNulls,
            ResolvedFunction isIndeterminateFunction)
    {
        Variable caseWasNull = null; // caseWasNull is set to true the first time a null in `testValues` is encountered
        if (checkForNulls) {
            caseWasNull = scope.getOrCreateTempVariable(boolean.class);
        }

        BytecodeBlock caseBlock = new BytecodeBlock();

        if (checkForNulls) {
            caseBlock.putVariable(caseWasNull, false);
        }

        LabelNode elseLabel = new LabelNode("else");
        BytecodeBlock elseBlock = new BytecodeBlock()
                .visitLabel(elseLabel);

        Variable wasNull = generatorContext.wasNull();
        if (checkForNulls) {
            // Consider following expression: "ARRAY[null] IN (ARRAY[1], ARRAY[2], ARRAY[3]) => NULL"
            // All lookup values will go to the SET_CONTAINS, since neither of them is indeterminate.
            // As ARRAY[null] is not among them, the code will fall through to the defaultCaseBlock.
            // Since there is no values in the defaultCaseBlock, the defaultCaseBlock will return FALSE.
            // That is incorrect. Doing an explicit check for indeterminate is required to correctly return NULL.
            if (testValues.isEmpty()) {
                elseBlock.append(new BytecodeBlock()
                        .append(generatorContext.generateCall(isIndeterminateFunction, ImmutableList.of(value)))
                        .putVariable(wasNull));
            }
            else {
                elseBlock.append(wasNull.set(caseWasNull));
            }
        }

        elseBlock.gotoLabel(noMatchLabel);

        BytecodeNode elseNode = elseBlock;
        for (BytecodeNode testNode : testValues) {
            LabelNode testLabel = new LabelNode("test");
            IfStatement test = new IfStatement();

            BytecodeNode equalsCall = generatorContext.generateCall(equals, ImmutableList.of(value, testNode));

            test.condition()
                    .visitLabel(testLabel)
                    .append(equalsCall);

            if (checkForNulls) {
                IfStatement wasNullCheck = new IfStatement("if wasNull, set caseWasNull to true, clear wasNull, pop boolean, and goto next test value");
                wasNullCheck.condition(wasNull);
                wasNullCheck.ifTrue(new BytecodeBlock()
                        .append(caseWasNull.set(constantTrue()))
                        .append(wasNull.set(constantFalse()))
                        .pop(boolean.class)
                        .gotoLabel(elseLabel));
                test.condition().append(wasNullCheck);
            }

            test.ifTrue().gotoLabel(matchLabel);
            test.ifFalse(elseNode);

            elseNode = test;
            elseLabel = testLabel;
        }
        caseBlock.append(elseNode);

        if (checkForNulls) {
            scope.releaseTempVariableForReuse(caseWasNull);
        }
        return caseBlock;
    }

    private static boolean isDeterminateConstant(RowExpression expression, MethodHandle isIndeterminateFunction)
    {
        if (!(expression instanceof ConstantExpression constantExpression)) {
            return false;
        }
        Object value = constantExpression.value();
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
}
