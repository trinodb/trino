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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.ForLoop;
import io.airlift.bytecode.control.IfStatement;
import io.trino.operator.scalar.CombineHashFunction;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.CallSiteBinder;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.add;
import static io.airlift.bytecode.expression.BytecodeExpressions.and;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantBoolean;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantLong;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantTrue;
import static io.airlift.bytecode.expression.BytecodeExpressions.equal;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.lessThan;
import static io.airlift.bytecode.expression.BytecodeExpressions.not;
import static io.airlift.bytecode.expression.BytecodeExpressions.notEqual;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.BLOCK_BUILDER;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.TypeUtils.NULL_HASH_CODE;
import static io.trino.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static io.trino.sql.gen.BytecodeUtils.loadConstant;
import static io.trino.sql.gen.SqlTypeBytecodeExpression.constantType;
import static io.trino.sql.planner.optimizations.HashGenerationOptimizer.INITIAL_HASH_VALUE;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;

public final class FlatHashStrategyCompiler
{
    private FlatHashStrategyCompiler() {}

    public static FlatHashStrategy compileFlatHashStrategy(List<Type> types, TypeOperators typeOperators)
    {
        boolean anyVariableWidth = (int) types.stream().filter(Type::isFlatVariableWidth).count() > 0;

        List<KeyField> keyFields = new ArrayList<>();
        int fixedOffset = 0;
        for (int i = 0; i < types.size(); i++) {
            Type type = types.get(i);
            keyFields.add(new KeyField(
                    i,
                    type,
                    fixedOffset,
                    fixedOffset + 1,
                    typeOperators.getReadValueOperator(type, simpleConvention(BLOCK_BUILDER, FLAT)),
                    typeOperators.getReadValueOperator(type, simpleConvention(FLAT_RETURN, BLOCK_POSITION_NOT_NULL)),
                    typeOperators.getDistinctFromOperator(type, simpleConvention(FAIL_ON_NULL, FLAT, BLOCK_POSITION_NOT_NULL)),
                    typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, FLAT)),
                    typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL))));
            fixedOffset += 1 + type.getFlatFixedSize();
        }

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("FlatHashStrategy"),
                type(Object.class),
                type(FlatHashStrategy.class));

        // the 'types' field is not used, but it makes debugging easier
        // this is an instance field because a static field doesn't seem to show up in the IntelliJ debugger
        FieldDefinition typesField = definition.declareField(a(PRIVATE, FINAL), "types", type(List.class, Type.class));
        MethodDefinition constructor = definition.declareConstructor(a(PUBLIC));
        constructor
                .getBody()
                .append(constructor.getThis())
                .invokeConstructor(Object.class)
                .append(constructor.getThis().setField(typesField, loadConstant(callSiteBinder, ImmutableList.copyOf(types), List.class)))
                .ret();

        definition.declareMethod(a(PUBLIC), "isAnyVariableWidth", type(boolean.class)).getBody()
                .append(constantBoolean(anyVariableWidth).ret());

        definition.declareMethod(a(PUBLIC), "getTotalFlatFixedLength", type(int.class)).getBody()
                .append(constantInt(fixedOffset).ret());

        generateGetTotalVariableWidth(definition, keyFields, callSiteBinder);

        generateReadFlat(definition, keyFields, callSiteBinder);
        generateWriteFlat(definition, keyFields, callSiteBinder);
        generateNotDistinctFromMethod(definition, keyFields, callSiteBinder);
        generateHashBlock(definition, keyFields, callSiteBinder);
        generateHashFlat(definition, keyFields, callSiteBinder);
        generateHashBlocksBatched(definition, keyFields, callSiteBinder);

        try {
            return defineClass(definition, FlatHashStrategy.class, callSiteBinder.getBindings(), FlatHashStrategyCompiler.class.getClassLoader())
                    .getConstructor()
                    .newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static void generateGetTotalVariableWidth(ClassDefinition definition, List<KeyField> keyFields, CallSiteBinder callSiteBinder)
    {
        Parameter blocks = arg("blocks", type(Block[].class));
        Parameter position = arg("position", type(int.class));
        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC),
                "getTotalVariableWidth",
                type(int.class),
                blocks,
                position);
        BytecodeBlock body = methodDefinition.getBody();

        Scope scope = methodDefinition.getScope();
        Variable variableWidth = scope.declareVariable("variableWidth", body, constantLong(0));

        for (KeyField keyField : keyFields) {
            Type type = keyField.type();
            if (type.isFlatVariableWidth()) {
                body.append(new IfStatement()
                        .condition(not(blocks.getElement(keyField.index()).invoke("isNull", boolean.class, position)))
                        .ifTrue(variableWidth.set(add(
                                variableWidth,
                                constantType(callSiteBinder, type).invoke("getFlatVariableWidthSize", int.class, blocks.getElement(keyField.index()), position).cast(long.class)))));
            }
        }
        body.append(invokeStatic(Math.class, "toIntExact", int.class, variableWidth).ret());
    }

    private static void generateReadFlat(ClassDefinition definition, List<KeyField> keyFields, CallSiteBinder callSiteBinder)
    {
        Parameter fixedChunk = arg("fixedChunk", type(byte[].class));
        Parameter fixedOffset = arg("fixedOffset", type(int.class));
        Parameter variableChunk = arg("variableChunk", type(byte[].class));
        Parameter blockBuilders = arg("blockBuilders", type(BlockBuilder[].class));
        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC),
                "readFlat",
                type(void.class),
                fixedChunk,
                fixedOffset,
                variableChunk,
                blockBuilders);
        BytecodeBlock body = methodDefinition.getBody();

        for (KeyField keyField : keyFields) {
            body.append(new IfStatement()
                    .condition(notEqual(fixedChunk.getElement(add(fixedOffset, constantInt(keyField.fieldIsNullOffset()))).cast(int.class), constantInt(0)))
                    .ifTrue(blockBuilders.getElement(keyField.index()).invoke("appendNull", BlockBuilder.class).pop())
                    .ifFalse(new BytecodeBlock()
                            .append(invokeDynamic(
                                    BOOTSTRAP_METHOD,
                                    ImmutableList.of(callSiteBinder.bind(keyField.readFlatMethod()).getBindingId()),
                                    "readFlat",
                                    void.class,
                                    fixedChunk,
                                    add(fixedOffset, constantInt(keyField.fieldFixedOffset())),
                                    variableChunk,
                                    blockBuilders.getElement(keyField.index())))));
        }
        body.ret();
    }

    private static void generateWriteFlat(ClassDefinition definition, List<KeyField> keyFields, CallSiteBinder callSiteBinder)
    {
        Parameter blocks = arg("blocks", type(Block[].class));
        Parameter position = arg("position", type(int.class));
        Parameter fixedChunk = arg("fixedChunk", type(byte[].class));
        Parameter fixedOffset = arg("fixedOffset", type(int.class));
        Parameter variableChunk = arg("variableChunk", type(byte[].class));
        Parameter variableOffset = arg("variableOffset", type(int.class));
        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC),
                "writeFlat",
                type(void.class),
                blocks,
                position,
                fixedChunk,
                fixedOffset,
                variableChunk,
                variableOffset);
        BytecodeBlock body = methodDefinition.getBody();
        for (KeyField keyField : keyFields) {
            BytecodeBlock writeNonNullFlat = new BytecodeBlock()
                    .append(invokeDynamic(
                            BOOTSTRAP_METHOD,
                            ImmutableList.of(callSiteBinder.bind(keyField.writeFlatMethod()).getBindingId()),
                            "writeFlat",
                            void.class,
                            blocks.getElement(keyField.index()),
                            position,
                            fixedChunk,
                            add(fixedOffset, constantInt(keyField.fieldFixedOffset())),
                            variableChunk,
                            variableOffset));
            if (keyField.type().isFlatVariableWidth()) {
                // variableOffset += type.getFlatVariableWidthSize(blocks[i], position);
                writeNonNullFlat.append(variableOffset.set(add(variableOffset, constantType(callSiteBinder, keyField.type()).invoke(
                        "getFlatVariableWidthSize",
                        int.class,
                        blocks.getElement(keyField.index()),
                        position))));
            }
            body.append(new IfStatement()
                    .condition(blocks.getElement(keyField.index()).invoke("isNull", boolean.class, position))
                    .ifTrue(fixedChunk.setElement(add(fixedOffset, constantInt(keyField.fieldIsNullOffset())), constantInt(1).cast(byte.class)))
                    .ifFalse(writeNonNullFlat));
        }
        body.ret();
    }

    private static void generateNotDistinctFromMethod(ClassDefinition definition, List<KeyField> keyFields, CallSiteBinder callSiteBinder)
    {
        Parameter leftFixedChunk = arg("leftFixedChunk", type(byte[].class));
        Parameter leftFixedOffset = arg("leftFixedOffset", type(int.class));
        Parameter leftVariableChunk = arg("leftVariableChunk", type(byte[].class));
        Parameter rightBlocks = arg("rightBlocks", type(Block[].class));
        Parameter rightPosition = arg("rightPosition", type(int.class));
        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC),
                "valueNotDistinctFrom",
                type(boolean.class),
                leftFixedChunk,
                leftFixedOffset,
                leftVariableChunk,
                rightBlocks,
                rightPosition);
        BytecodeBlock body = methodDefinition.getBody();

        for (KeyField keyField : keyFields) {
            MethodDefinition distinctFromMethod = generateDistinctFromMethod(definition, keyField, callSiteBinder);
            body.append(new IfStatement()
                    .condition(invokeStatic(distinctFromMethod, leftFixedChunk, leftFixedOffset, leftVariableChunk, rightBlocks.getElement(keyField.index()), rightPosition))
                    .ifTrue(constantFalse().ret()));
        }
        body.append(constantTrue().ret());
    }

    private static MethodDefinition generateDistinctFromMethod(ClassDefinition definition, KeyField keyField, CallSiteBinder callSiteBinder)
    {
        Parameter leftFixedChunk = arg("leftFixedChunk", type(byte[].class));
        Parameter leftFixedOffset = arg("leftFixedOffset", type(int.class));
        Parameter leftVariableChunk = arg("leftVariableChunk", type(byte[].class));
        Parameter rightBlock = arg("rightBlock", type(Block.class));
        Parameter rightPosition = arg("rightPosition", type(int.class));
        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC, STATIC),
                "valueDistinctFrom" + keyField.index(),
                type(boolean.class),
                leftFixedChunk,
                leftFixedOffset,
                leftVariableChunk,
                rightBlock,
                rightPosition);
        BytecodeBlock body = methodDefinition.getBody();
        Scope scope = methodDefinition.getScope();

        Variable leftIsNull = scope.declareVariable("leftIsNull", body, notEqual(leftFixedChunk.getElement(add(leftFixedOffset, constantInt(keyField.fieldIsNullOffset()))).cast(int.class), constantInt(0)));
        Variable rightIsNull = scope.declareVariable("rightIsNull", body, rightBlock.invoke("isNull", boolean.class, rightPosition));

        // if (leftIsNull) {
        //     return !rightIsNull;
        // }
        body.append(new IfStatement()
                .condition(leftIsNull)
                .ifTrue(not(rightIsNull).ret()));

        // if (rightIsNull) {
        //     return true;
        // }
        body.append(new IfStatement()
                .condition(rightIsNull)
                .ifTrue(constantTrue().ret()));

        body.append(invokeDynamic(
                BOOTSTRAP_METHOD,
                ImmutableList.of(callSiteBinder.bind(keyField.distinctFlatBlockMethod()).getBindingId()),
                "distinctFrom",
                boolean.class,
                leftFixedChunk,
                add(leftFixedOffset, constantInt(keyField.fieldFixedOffset())),
                leftVariableChunk,
                rightBlock,
                rightPosition)
                .ret());
        return methodDefinition;
    }

    private static void generateHashBlock(ClassDefinition definition, List<KeyField> keyFields, CallSiteBinder callSiteBinder)
    {
        Parameter blocks = arg("blocks", type(Block[].class));
        Parameter position = arg("position", type(int.class));
        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC),
                "hash",
                type(long.class),
                blocks,
                position);
        BytecodeBlock body = methodDefinition.getBody();

        Scope scope = methodDefinition.getScope();
        Variable result = scope.declareVariable("result", body, constantLong(INITIAL_HASH_VALUE));
        Variable hash = scope.declareVariable(long.class, "hash");
        Variable block = scope.declareVariable(Block.class, "block");

        for (KeyField keyField : keyFields) {
            body.append(block.set(blocks.getElement(keyField.index())));
            body.append(new IfStatement()
                    .condition(block.invoke("isNull", boolean.class, position))
                    .ifTrue(hash.set(constantLong(NULL_HASH_CODE)))
                    .ifFalse(hash.set(invokeDynamic(
                            BOOTSTRAP_METHOD,
                            ImmutableList.of(callSiteBinder.bind(keyField.hashBlockMethod()).getBindingId()),
                            "hash",
                            long.class,
                            block,
                            position))));
            body.append(result.set(invokeStatic(CombineHashFunction.class, "getHash", long.class, result, hash)));
        }
        body.append(result.ret());
    }

    private static void generateHashBlocksBatched(ClassDefinition definition, List<KeyField> keyFields, CallSiteBinder callSiteBinder)
    {
        Parameter blocks = arg("blocks", type(Block[].class));
        Parameter hashes = arg("hashes", type(long[].class));
        Parameter offset = arg("offset", type(int.class));
        Parameter length = arg("length", type(int.class));

        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC),
                "hashBlocksBatched",
                type(void.class),
                blocks,
                hashes,
                offset,
                length);

        BytecodeBlock body = methodDefinition.getBody();
        body.append(invokeStatic(Objects.class, "checkFromIndexSize", int.class, constantInt(0), length, hashes.length()).pop());

        BytecodeBlock nonEmptyLength = new BytecodeBlock();

        Map<Type, MethodDefinition> typeMethods = new HashMap<>();
        for (KeyField keyField : keyFields) {
            MethodDefinition method;
            // First hash method implementation does not combine hashes, so it can't be reused
            if (keyField.index() == 0) {
                method = generateHashBlockVectorized(definition, keyField, callSiteBinder);
            }
            else {
                // Columns of the same type can reuse the same static method implementation
                method = typeMethods.get(keyField.type());
                if (method == null) {
                    method = generateHashBlockVectorized(definition, keyField, callSiteBinder);
                    typeMethods.put(keyField.type(), method);
                }
            }
            nonEmptyLength.append(invokeStatic(method, blocks.getElement(keyField.index()), hashes, offset, length));
        }

        body.append(new IfStatement("if (length != 0)")
                .condition(equal(length, constantInt(0)))
                .ifFalse(nonEmptyLength))
                .ret();
    }

    private static MethodDefinition generateHashBlockVectorized(ClassDefinition definition, KeyField field, CallSiteBinder callSiteBinder)
    {
        Parameter block = arg("block", type(Block.class));
        Parameter hashes = arg("hashes", type(long[].class));
        Parameter offset = arg("offset", type(int.class));
        Parameter length = arg("length", type(int.class));

        MethodDefinition methodDefinition = definition.declareMethod(
                a(PRIVATE, STATIC),
                "hashBlockVectorized_" + field.index(),
                type(void.class),
                block,
                hashes,
                offset,
                length);

        Scope scope = methodDefinition.getScope();
        BytecodeBlock body = methodDefinition.getBody();

        Variable index = scope.declareVariable(int.class, "index");
        Variable position = scope.declareVariable(int.class, "position");
        Variable mayHaveNull = scope.declareVariable(boolean.class, "mayHaveNull");
        Variable hash = scope.declareVariable(long.class, "hash");

        body.append(mayHaveNull.set(block.invoke("mayHaveNull", boolean.class)));
        body.append(position.set(invokeStatic(Objects.class, "checkFromToIndex", int.class, offset, add(offset, length), block.invoke("getPositionCount", int.class))));
        body.append(invokeStatic(Objects.class, "checkFromIndexSize", int.class, constantInt(0), length, hashes.length()).pop());

        BytecodeBlock loopBody = new BytecodeBlock().append(new IfStatement("if (mayHaveNull && block.isNull(position))")
                .condition(and(mayHaveNull, block.invoke("isNull", boolean.class, position)))
                .ifTrue(hash.set(constantLong(NULL_HASH_CODE)))
                .ifFalse(hash.set(invokeDynamic(
                        BOOTSTRAP_METHOD,
                        ImmutableList.of(callSiteBinder.bind(field.hashBlockMethod()).getBindingId()),
                        "hash",
                        long.class,
                        block,
                        position))));
        if (field.index() == 0) {
            // hashes[index] = hash;
            loopBody.append(hashes.setElement(index, hash));
        }
        else {
            // hashes[index] = CombineHashFunction.getHash(hashes[index], hash);
            loopBody.append(hashes.setElement(index, invokeStatic(CombineHashFunction.class, "getHash", long.class, hashes.getElement(index), hash)));
        }
        loopBody.append(position.increment());

        body.append(new ForLoop("for (index = 0; index < length; index++)")
                .initialize(index.set(constantInt(0)))
                .condition(lessThan(index, length))
                .update(index.increment())
                .body(loopBody))
                .ret();

        return methodDefinition;
    }

    private static void generateHashFlat(ClassDefinition definition, List<KeyField> keyFields, CallSiteBinder callSiteBinder)
    {
        Parameter fixedChunk = arg("fixedChunk", type(byte[].class));
        Parameter fixedOffset = arg("fixedOffset", type(int.class));
        Parameter variableChunk = arg("variableChunk", type(byte[].class));
        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC),
                "hash",
                type(long.class),
                fixedChunk,
                fixedOffset,
                variableChunk);
        BytecodeBlock body = methodDefinition.getBody();

        Scope scope = methodDefinition.getScope();
        Variable result = scope.declareVariable("result", body, constantLong(INITIAL_HASH_VALUE));
        Variable hash = scope.declareVariable(long.class, "hash");

        for (KeyField keyField : keyFields) {
            body.append(new IfStatement()
                    .condition(notEqual(fixedChunk.getElement(add(fixedOffset, constantInt(keyField.fieldIsNullOffset()))).cast(int.class), constantInt(0)))
                    .ifTrue(hash.set(constantLong(NULL_HASH_CODE)))
                    .ifFalse(hash.set(invokeDynamic(
                            BOOTSTRAP_METHOD,
                            ImmutableList.of(callSiteBinder.bind(keyField.hashFlatMethod()).getBindingId()),
                            "hash",
                            long.class,
                            fixedChunk,
                            add(fixedOffset, constantInt(keyField.fieldFixedOffset())),
                            variableChunk))));
            body.append(result.set(invokeStatic(CombineHashFunction.class, "getHash", long.class, result, hash)));
        }
        body.append(result.ret());
    }

    private record KeyField(
            int index,
            Type type,
            int fieldIsNullOffset,
            int fieldFixedOffset,
            MethodHandle readFlatMethod,
            MethodHandle writeFlatMethod,
            MethodHandle distinctFlatBlockMethod,
            MethodHandle hashFlatMethod,
            MethodHandle hashBlockMethod) {}
}
