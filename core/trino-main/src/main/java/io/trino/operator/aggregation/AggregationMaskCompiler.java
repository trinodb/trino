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
package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableMap;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.ForLoop;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.add;
import static io.airlift.bytecode.expression.BytecodeExpressions.and;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.equal;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.isNotNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.isNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.lessThan;
import static io.airlift.bytecode.expression.BytecodeExpressions.newArray;
import static io.airlift.bytecode.expression.BytecodeExpressions.not;
import static io.airlift.bytecode.expression.BytecodeExpressions.notEqual;
import static io.airlift.bytecode.expression.BytecodeExpressions.or;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;

public final class AggregationMaskCompiler
{
    private AggregationMaskCompiler() {}

    public static Constructor<? extends AggregationMaskBuilder> generateAggregationMaskBuilder(int... nonNullArgumentChannels)
    {
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(AggregationMaskBuilder.class.getSimpleName()),
                type(Object.class),
                type(AggregationMaskBuilder.class));

        FieldDefinition selectedPositionsField = definition.declareField(a(PRIVATE), "selectedPositions", int[].class);

        MethodDefinition constructor = definition.declareConstructor(a(PUBLIC));
        constructor.getBody().comment("super();")
                .append(constructor.getThis())
                .invokeConstructor(Object.class)
                .append(constructor.getThis().setField(selectedPositionsField, newArray(type(int[].class), 0)))
                .ret();

        Parameter argumentsParameter = arg("arguments", type(Page.class));
        Parameter maskBlockParameter = arg("optionalMaskBlock", type(Optional.class, Block.class));
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC),
                "buildAggregationMask",
                type(AggregationMask.class),
                argumentsParameter,
                maskBlockParameter);

        BytecodeBlock body = method.getBody();
        Scope scope = method.getScope();
        Variable positionCount = scope.declareVariable("positionCount", body, argumentsParameter.invoke("getPositionCount", int.class));

        // if page is empty, return select none
        body.append(new IfStatement()
                .condition(equal(positionCount, constantInt(0)))
                .ifTrue(invokeStatic(AggregationMask.class, "createSelectNone", AggregationMask.class, positionCount).ret()));

        Variable maskBlock = scope.declareVariable("maskBlock", body, maskBlockParameter.invoke("orElse", Object.class, constantNull(Object.class)).cast(Block.class));
        Variable hasMaskBlock = scope.declareVariable("hasMaskBlock", body, isNotNull(maskBlock));
        Variable maskBlockMayHaveNull = scope.declareVariable(
                "maskBlockMayHaveNull",
                body,
                and(hasMaskBlock, maskBlock.invoke("mayHaveNull", boolean.class)));

        // if mask is RLE it will be, either all allowed, or all denied
        Variable rleValue = scope.declareVariable(ByteArrayBlock.class, "rleValue");
        body.append(new IfStatement()
                .condition(maskBlock.instanceOf(RunLengthEncodedBlock.class))
                .ifTrue(new BytecodeBlock()
                        .append(rleValue.set(maskBlock.cast(RunLengthEncodedBlock.class).invoke("getValue", ValueBlock.class).cast(ByteArrayBlock.class)))
                        .append(new IfStatement()
                                .condition(not(testMaskBlock(
                                        rleValue,
                                        maskBlockMayHaveNull,
                                        constantInt(0))))
                                .ifTrue(invokeStatic(AggregationMask.class, "createSelectNone", AggregationMask.class, positionCount).ret()))
                        .append(maskBlock.set(constantNull(Block.class)))
                        .append(hasMaskBlock.set(constantFalse()))
                        .append(maskBlockMayHaveNull.set(constantFalse()))));

        List<Variable> nonNullArgs = new ArrayList<>(nonNullArgumentChannels.length);
        List<Variable> nonNullArgMayHaveNulls = new ArrayList<>(nonNullArgumentChannels.length);
        for (int channel : nonNullArgumentChannels) {
            Variable arg = scope.declareVariable("arg" + channel, body, argumentsParameter.invoke("getBlock", Block.class, constantInt(channel)));
            body.append(new IfStatement()
                    .condition(invokeStatic(AggregationMaskCompiler.class, "isAlwaysNull", boolean.class, arg))
                    .ifTrue(invokeStatic(AggregationMask.class, "createSelectNone", AggregationMask.class, positionCount).ret()));
            Variable mayHaveNull = scope.declareVariable("arg" + channel + "MayHaveNull", body, arg.invoke("mayHaveNull", boolean.class));
            nonNullArgs.add(arg);
            nonNullArgMayHaveNulls.add(mayHaveNull);
        }

        // if there is no mask block, and all non-null arguments do not have nulls, return selectAll
        BytecodeExpression isSelectAll = not(hasMaskBlock);
        for (Variable mayHaveNull : nonNullArgMayHaveNulls) {
            isSelectAll = and(isSelectAll, not(mayHaveNull));
        }
        body.append(new IfStatement()
                .condition(isSelectAll)
                .ifTrue(invokeStatic(AggregationMask.class, "createSelectAll", AggregationMask.class, positionCount).ret()));

        // grow the selection array if necessary
        Variable selectedPositions = scope.declareVariable("selectedPositions", body, method.getThis().getField(selectedPositionsField));
        body.append(new IfStatement()
                .condition(lessThan(selectedPositions.length(), positionCount))
                .ifTrue(new BytecodeBlock()
                        .append(selectedPositions.set(newArray(type(int[].class), positionCount)))
                        .append(method.getThis().setField(selectedPositionsField, selectedPositions))));

        // create expression to test if a position is selected
        Variable maskValueBlock = scope.declareVariable(ByteArrayBlock.class, "maskValueBlock");
        Variable maskValueBlockPosition = scope.declareVariable("maskValueBlockPosition", body, constantInt(0));
        BytecodeExpression isPositionSelected = testMaskBlock(maskValueBlock, maskBlockMayHaveNull, maskValueBlockPosition);

        Variable pagePosition = scope.declareVariable("pagePosition", body, constantInt(0));
        for (int i = 0; i < nonNullArgs.size(); i++) {
            Variable arg = nonNullArgs.get(i);
            Variable mayHaveNull = nonNullArgMayHaveNulls.get(i);
            isPositionSelected = and(isPositionSelected, testPositionIsNotNull(arg, mayHaveNull, pagePosition));
        }

        // add all positions that pass the tests
        // at this point the mask block can only be a DictionaryBlock, ByteArrayBlock, or null
        Variable selectedPositionsIndex = scope.declareVariable("selectedPositionsIndex", body, constantInt(0));
        Variable rawIds = scope.declareVariable(int[].class, "rawIds");
        Variable rawIdsOffset = scope.declareVariable(int.class, "rawIdsOffset");
        body.append(new IfStatement()
                .condition(maskBlock.instanceOf(DictionaryBlock.class))
                        .ifTrue(new BytecodeBlock()
                                .append(maskValueBlock.set(maskBlock.cast(DictionaryBlock.class).invoke("getDictionary", ValueBlock.class).cast(ByteArrayBlock.class)))
                                .append(rawIds.set(maskBlock.cast(DictionaryBlock.class).invoke("getRawIds", int[].class)))
                                .append(rawIdsOffset.set(maskBlock.cast(DictionaryBlock.class).invoke("getRawIdsOffset", int.class)))
                                .append(new ForLoop()
                                        .initialize(pagePosition.set(constantInt(0)))
                                        .condition(lessThan(pagePosition, positionCount))
                                        .update(pagePosition.increment())
                                        .body(new BytecodeBlock()
                                                .append(maskValueBlockPosition.set(rawIds.getElement(add(rawIdsOffset, pagePosition))))
                                                .append(new IfStatement()
                                                        .condition(isPositionSelected)
                                                        .ifTrue(new BytecodeBlock()
                                                                .append(selectedPositions.setElement(selectedPositionsIndex, pagePosition))
                                                                .append(selectedPositionsIndex.increment()))))))
                        .ifFalse(new BytecodeBlock()
                                .append(maskValueBlock.set(maskBlock.cast(ByteArrayBlock.class)))
                                .append(new ForLoop()
                                        .initialize(pagePosition.set(constantInt(0)))
                                        .condition(lessThan(pagePosition, positionCount))
                                        .update(pagePosition.increment())
                                        .body(new BytecodeBlock()
                                                .append(maskValueBlockPosition.set(pagePosition))
                                                .append(new IfStatement()
                                                        .condition(isPositionSelected)
                                                        .ifTrue(new BytecodeBlock()
                                                                .append(selectedPositions.setElement(selectedPositionsIndex, pagePosition))
                                                                .append(selectedPositionsIndex.increment())))))));

        body.append(invokeStatic(
                AggregationMask.class,
                "createSelectedPositions",
                AggregationMask.class,
                positionCount,
                selectedPositions,
                selectedPositionsIndex)
                .ret());

        Class<? extends AggregationMaskBuilder> builderClass = defineClass(
                definition,
                AggregationMaskBuilder.class,
                ImmutableMap.of(),
                AggregationMaskCompiler.class.getClassLoader());

        try {
            return builderClass.getConstructor();
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static BytecodeExpression testPositionIsNotNull(BytecodeExpression block, BytecodeExpression mayHaveNulls, BytecodeExpression position)
    {
        return or(not(mayHaveNulls), not(block.invoke("isNull", boolean.class, position)));
    }

    private static BytecodeExpression testMaskBlock(BytecodeExpression block, BytecodeExpression mayHaveNulls, BytecodeExpression position)
    {
        verify(block.getType().equals(type(ByteArrayBlock.class)));
        return or(
                isNull(block),
                and(
                        testPositionIsNotNull(block, mayHaveNulls, position),
                        notEqual(block.invoke("getByte", byte.class, position).cast(int.class), constantInt(0))));
    }

    @UsedByGeneratedCode
    public static boolean isAlwaysNull(Block block)
    {
        if (block instanceof RunLengthEncodedBlock rle) {
            return rle.getValue().isNull(0);
        }
        return false;
    }
}
