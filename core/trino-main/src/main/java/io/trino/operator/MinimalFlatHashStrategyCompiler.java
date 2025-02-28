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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.ForLoop;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.cache.CacheStatsMBean;
import io.trino.operator.scalar.CombineHashFunction;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.CallSiteBinder;
import org.assertj.core.util.VisibleForTesting;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
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
import static io.airlift.bytecode.expression.BytecodeExpressions.subtract;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.operator.HashGenerator.INITIAL_HASH_VALUE;
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
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;

public final class MinimalFlatHashStrategyCompiler
{
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    private final LoadingCache<List<Type>, MinimalFlatHashStrategy> flatHashStrategies;

    @Inject
    public MinimalFlatHashStrategyCompiler(TypeOperators typeOperators)
    {
        this.flatHashStrategies = buildNonEvictableCache(
                CacheBuilder.newBuilder()
                        .recordStats()
                        .maximumSize(1000),
                CacheLoader.from(key -> compileFlatHashStrategy(key, typeOperators)));
    }

    public MinimalFlatHashStrategy getFlatHashStrategy(List<Type> types)
    {
        return flatHashStrategies.getUnchecked(ImmutableList.copyOf(types));
    }

    @Managed
    @Nested
    public CacheStatsMBean getFlatHashStrategiesStats()
    {
        return new CacheStatsMBean(flatHashStrategies);
    }

    @VisibleForTesting
    public static MinimalFlatHashStrategy compileFlatHashStrategy(List<Type> types, TypeOperators typeOperators)
    {
        List<KeyField> keyFields = new ArrayList<>();
        int fixedOffset = 0;
        int previousVariableFixedOffset = -1;
        for (int i = 0; i < types.size(); i++) {
            Type type = types.get(i);
            int fieldFixedOffset = fixedOffset + 1;
            keyFields.add(new KeyField(
                    i,
                    type,
                    fixedOffset,
                    fieldFixedOffset,
                    previousVariableFixedOffset,
                    typeOperators.getReadValueOperator(type, simpleConvention(BLOCK_BUILDER, FLAT)),
                    typeOperators.getReadValueOperator(type, simpleConvention(FLAT_RETURN, BLOCK_POSITION_NOT_NULL)),
                    typeOperators.getIdenticalOperator(type, simpleConvention(FAIL_ON_NULL, FLAT, BLOCK_POSITION_NOT_NULL)),
                    typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, FLAT)),
                    typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL))));
            if (type.isFlatVariableWidth()) {
                // Each variable width field stores the ending variableChunkOffset in the fixed size region
                fixedOffset = fieldFixedOffset + Integer.BYTES;
                // Store the fixedFieldOffset that the next variable width field will use to compute its length (previous variable width ending offset is starting variable width offset)
                previousVariableFixedOffset = fieldFixedOffset;
            }
            else {
                fixedOffset = fieldFixedOffset + type.getFlatFixedSize();
            }
        }

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        List<ChunkClass> chunkClasses = new ArrayList<>();
        int chunkNumber = 0;
        // generate a separate class for each chunk of 500 types to avoid hitting the JVM method size and constant pool limits
        for (List<KeyField> chunk : Lists.partition(keyFields, 500)) {
            chunkClasses.add(compileFlatHashStrategyChunk(callSiteBinder, chunk, chunkNumber));
            chunkNumber++;
        }

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("MinimalFlatHashStrategy"),
                type(Object.class),
                type(MinimalFlatHashStrategy.class));

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

        boolean anyVariableWidth = (int) types.stream().filter(Type::isFlatVariableWidth).count() > 0;
        definition.declareMethod(a(PUBLIC), "isAnyVariableWidth", type(boolean.class)).getBody()
                .append(constantBoolean(anyVariableWidth).ret());

        definition.declareMethod(a(PUBLIC), "getTotalFlatFixedLength", type(int.class)).getBody()
                .append(constantInt(fixedOffset).ret());

        generateGetTotalVariableWidth(definition, chunkClasses);

        generateReadFlat(definition, chunkClasses);
        generateWriteFlat(definition, chunkClasses);
        generateIdenticalMethod(definition, chunkClasses);
        generateHashFlat(definition, chunkClasses);
        generateHashBlock(definition, chunkClasses);
        generateHashBlocksBatched(definition, chunkClasses);

        try {
            DynamicClassLoader classLoader = new DynamicClassLoader(FlatHashStrategyCompiler.class.getClassLoader(), callSiteBinder.getBindings());
            for (ChunkClass chunkClass : chunkClasses) {
                defineClass(chunkClass.definition(), Object.class, classLoader);
            }
            return defineClass(definition, MinimalFlatHashStrategy.class, classLoader)
                    .getConstructor()
                    .newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static ChunkClass compileFlatHashStrategyChunk(CallSiteBinder callSiteBinder, List<KeyField> keyFields, int chunkNumber)
    {
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("FlatHashStrategyChunk$" + chunkNumber),
                type(Object.class),
                type(FlatHashStrategy.class));

        definition.declareDefaultConstructor(a(PRIVATE));

        MethodDefinition getTotalVariableWidthChunk = generateGetTotalVariableWidthChunk(definition, keyFields, callSiteBinder);
        MethodDefinition readFlatChunk = generateReadFlatChunk(definition, keyFields, callSiteBinder);
        MethodDefinition writeFlatChunk = generateWriteFlatChunk(definition, keyFields, callSiteBinder);
        MethodDefinition identicalChunkMethod = generateIdenticalChunkMethod(definition, keyFields, callSiteBinder);
        MethodDefinition hashFlatChunkMethod = generateHashFlatChunk(definition, keyFields, callSiteBinder);
        MethodDefinition hashBlockChunk = generateHashBlockChunk(definition, keyFields, callSiteBinder);
        MethodDefinition hashBlocksBatchedChunk = generateHashBlocksBatchedChunk(definition, keyFields, callSiteBinder);

        return new ChunkClass(
                definition,
                getTotalVariableWidthChunk,
                readFlatChunk,
                writeFlatChunk,
                identicalChunkMethod,
                hashFlatChunkMethod,
                hashBlockChunk,
                hashBlocksBatchedChunk);
    }

    private static void generateGetTotalVariableWidth(ClassDefinition definition, List<ChunkClass> chunkClasses)
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
        for (ChunkClass chunkClass : chunkClasses) {
            body.append(variableWidth.set(add(variableWidth, invokeStatic(chunkClass.getTotalVariableWidth(), blocks, position))));
        }
        body.append(invokeStatic(Math.class, "toIntExact", int.class, variableWidth).ret());
    }

    private static MethodDefinition generateGetTotalVariableWidthChunk(ClassDefinition definition, List<KeyField> keyFields, CallSiteBinder callSiteBinder)
    {
        Parameter blocks = arg("blocks", type(Block[].class));
        Parameter position = arg("position", type(int.class));
        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC, STATIC),
                "getTotalVariableWidth",
                type(long.class),
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
                                constantType(callSiteBinder, type).invoke("getMinimalFlatVariableWidthSize", int.class, blocks.getElement(keyField.index()), position).cast(long.class)))));
            }
        }
        body.append(variableWidth.ret());
        return methodDefinition;
    }

    private static void generateReadFlat(ClassDefinition definition, List<ChunkClass> chunkClasses)
    {
        Parameter fixedChunk = arg("fixedChunk", type(byte[].class));
        Parameter fixedOffset = arg("fixedOffset", type(int.class));
        Parameter variableChunk = arg("variableChunk", type(byte[].class));
        Parameter variableOffset = arg("variableOffset", type(int.class));
        Parameter blockBuilders = arg("blockBuilders", type(BlockBuilder[].class));
        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC),
                "readFlat",
                type(void.class),
                fixedChunk,
                fixedOffset,
                variableChunk,
                variableOffset,
                blockBuilders);
        BytecodeBlock body = methodDefinition.getBody();
        for (ChunkClass chunkClass : chunkClasses) {
            body.append(variableOffset.set(invokeStatic(chunkClass.readFlatChunk(), fixedChunk, fixedOffset, variableChunk, variableOffset, blockBuilders)));
        }
        body.ret();
    }

    private static MethodDefinition generateReadFlatChunk(ClassDefinition definition, List<KeyField> keyFields, CallSiteBinder callSiteBinder)
    {
        Parameter fixedChunk = arg("fixedChunk", type(byte[].class));
        Parameter fixedOffset = arg("fixedOffset", type(int.class));
        Parameter variableChunk = arg("variableChunk", type(byte[].class));
        Parameter variableOffset = arg("variableOffset", type(int.class));
        Parameter blockBuilders = arg("blockBuilders", type(BlockBuilder[].class));
        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC, STATIC),
                "readFlat",
                type(int.class),
                fixedChunk,
                fixedOffset,
                variableChunk,
                variableOffset,
                blockBuilders);
        Variable variableEndOffset = methodDefinition.getScope().declareVariable(int.class, "variableEndOffset");
        BytecodeBlock body = methodDefinition.getBody();

        for (KeyField keyField : keyFields) {
            BytecodeBlock readNonNull = new BytecodeBlock();
            if (keyField.type().isFlatVariableWidth()) {
                // variableEndOffset = MinimalFlatHashStrategyCompiler.readVariableWidthEndingOffset(fixedChunk, fixedOffset + fieldOffset)
                readNonNull.append(variableEndOffset.set(invokeStatic(MinimalFlatHashStrategyCompiler.class, "readVariableWidthEndingOffset", int.class, fixedChunk, add(fixedOffset, constantInt(keyField.fieldFixedOffset())))));
                readNonNull.append(constantType(callSiteBinder, keyField.type()).invoke(
                        "readMinimalFlatVariableWidth",
                        void.class,
                        variableChunk,
                        variableOffset,
                        subtract(variableEndOffset, variableOffset),
                        blockBuilders.getElement(keyField.index())));
                // variableOffset = variableEndOffset;
                readNonNull.append(variableOffset.set(variableEndOffset));
            }
            else {
                readNonNull.append(invokeDynamic(
                        BOOTSTRAP_METHOD,
                        ImmutableList.of(callSiteBinder.bind(keyField.readFlatMethod()).getBindingId()),
                        "readFlat",
                        void.class,
                        fixedChunk,
                        add(fixedOffset, constantInt(keyField.fieldFixedOffset())),
                        variableChunk,
                        blockBuilders.getElement(keyField.index())));
            }
            body.append(new IfStatement()
                    .condition(notEqual(fixedChunk.getElement(add(fixedOffset, constantInt(keyField.fieldIsNullOffset()))).cast(int.class), constantInt(0)))
                    .ifTrue(blockBuilders.getElement(keyField.index()).invoke("appendNull", BlockBuilder.class).pop())
                    .ifFalse(readNonNull));
        }
        body.append(variableOffset.ret());
        return methodDefinition;
    }

    private static void generateWriteFlat(ClassDefinition definition, List<ChunkClass> chunkClasses)
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
        for (ChunkClass chunkClass : chunkClasses) {
            body.append(variableOffset.set(invokeStatic(chunkClass.writeFlatChunk(), blocks, position, fixedChunk, fixedOffset, variableChunk, variableOffset)));
        }
        body.ret();
    }

    private static MethodDefinition generateWriteFlatChunk(ClassDefinition definition, List<KeyField> keyFields, CallSiteBinder callSiteBinder)
    {
        Parameter blocks = arg("blocks", type(Block[].class));
        Parameter position = arg("position", type(int.class));
        Parameter fixedChunk = arg("fixedChunk", type(byte[].class));
        Parameter fixedOffset = arg("fixedOffset", type(int.class));
        Parameter variableChunk = arg("variableChunk", type(byte[].class));
        Parameter variableOffset = arg("variableOffset", type(int.class));
        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC, STATIC),
                "writeFlat",
                type(int.class),
                blocks,
                position,
                fixedChunk,
                fixedOffset,
                variableChunk,
                variableOffset);
        BytecodeBlock body = methodDefinition.getBody();
        for (KeyField keyField : keyFields) {
            BytecodeBlock writeNonNullFlat = new BytecodeBlock();
            if (keyField.type().isFlatVariableWidth()) {
                writeNonNullFlat.append(constantType(callSiteBinder, keyField.type()).invoke(
                        "writeMinimalFlatVariableWidth",
                                void.class,
                                blocks.getElement(keyField.index()),
                                position,
                                variableChunk,
                                variableOffset));
                // variableOffset += type.getMinimalFlatVariableWidthSize(blocks[i], position);
                writeNonNullFlat.append(
                        variableOffset.set(
                                add(variableOffset, constantType(callSiteBinder, keyField.type()).invoke("getMinimalFlatVariableWidthSize", int.class, blocks.getElement(keyField.index()), position))));
            }
            else {
                writeNonNullFlat.append(invokeDynamic(
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
            }
            body.append(new IfStatement()
                    .condition(blocks.getElement(keyField.index()).invoke("isNull", boolean.class, position))
                    .ifTrue(fixedChunk.setElement(add(fixedOffset, constantInt(keyField.fieldIsNullOffset())), constantInt(1).cast(byte.class)))
                    .ifFalse(writeNonNullFlat));
            // Store the ending offset, even when the field is null so that subsequent variable width fields can still compute their length
            if (keyField.type().isFlatVariableWidth()) {
                body.append(invokeStatic(MinimalFlatHashStrategyCompiler.class, "writeVariableWidthEndingOffset", void.class, fixedChunk, add(fixedOffset, constantInt(keyField.fieldFixedOffset())), variableOffset));
            }
        }
        body.append(variableOffset.ret());
        return methodDefinition;
    }

    private static void generateIdenticalMethod(ClassDefinition definition, List<ChunkClass> chunkClasses)
    {
        Parameter leftFixedChunk = arg("leftFixedChunk", type(byte[].class));
        Parameter leftFixedOffset = arg("leftFixedOffset", type(int.class));
        Parameter leftVariableChunk = arg("leftVariableChunk", type(byte[].class));
        Parameter leftVariableChunkOffset = arg("leftVariableChunkOffset", type(int.class));
        Parameter rightBlocks = arg("rightBlocks", type(Block[].class));
        Parameter rightPosition = arg("rightPosition", type(int.class));
        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC),
                "valueIdentical",
                type(boolean.class),
                leftFixedChunk,
                leftFixedOffset,
                leftVariableChunk,
                leftVariableChunkOffset,
                rightBlocks,
                rightPosition);
        BytecodeBlock body = methodDefinition.getBody();
        for (ChunkClass chunkClass : chunkClasses) {
            body.append(new IfStatement()
                    .condition(invokeStatic(chunkClass.identicalMethodChunk(), leftFixedChunk, leftFixedOffset, leftVariableChunk, leftVariableChunkOffset, rightBlocks, rightPosition))
                    .ifFalse(constantFalse().ret()));
        }
        body.append(constantTrue().ret());
    }

    private static MethodDefinition generateIdenticalChunkMethod(ClassDefinition definition, List<KeyField> keyFields, CallSiteBinder callSiteBinder)
    {
        Parameter leftFixedChunk = arg("leftFixedChunk", type(byte[].class));
        Parameter leftFixedOffset = arg("leftFixedOffset", type(int.class));
        Parameter leftVariableChunk = arg("leftVariableChunk", type(byte[].class));
        Parameter leftVariableChunkOffset = arg("leftVariableChunkOffset", type(int.class));
        Parameter rightBlocks = arg("rightBlocks", type(Block[].class));
        Parameter rightPosition = arg("rightPosition", type(int.class));
        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC, STATIC),
                "valueIdentical",
                type(boolean.class),
                leftFixedChunk,
                leftFixedOffset,
                leftVariableChunk,
                leftVariableChunkOffset,
                rightBlocks,
                rightPosition);
        BytecodeBlock body = methodDefinition.getBody();

        for (KeyField keyField : keyFields) {
            MethodDefinition identicalMethod = generateIdenticalMethod(definition, keyField, callSiteBinder);
            body.append(new IfStatement()
                    .condition(invokeStatic(identicalMethod, leftFixedChunk, leftFixedOffset, leftVariableChunk, leftVariableChunkOffset, rightBlocks.getElement(keyField.index()), rightPosition))
                    .ifFalse(constantFalse().ret()));
        }
        body.append(constantTrue().ret());
        return methodDefinition;
    }

    private static MethodDefinition generateIdenticalMethod(ClassDefinition definition, KeyField keyField, CallSiteBinder callSiteBinder)
    {
        Parameter leftFixedChunk = arg("leftFixedChunk", type(byte[].class));
        Parameter leftFixedOffset = arg("leftFixedOffset", type(int.class));
        Parameter leftVariableChunk = arg("leftVariableChunk", type(byte[].class));
        Parameter leftVariableChunkOffset = arg("leftVariableChunkOffset", type(int.class));
        Parameter rightBlock = arg("rightBlock", type(Block.class));
        Parameter rightPosition = arg("rightPosition", type(int.class));
        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC, STATIC),
                "valueIdentical" + keyField.index(),
                type(boolean.class),
                leftFixedChunk,
                leftFixedOffset,
                leftVariableChunk,
                leftVariableChunkOffset,
                rightBlock,
                rightPosition);
        BytecodeBlock body = methodDefinition.getBody();
        Scope scope = methodDefinition.getScope();

        Variable leftIsNull = scope.declareVariable("leftIsNull", body, notEqual(leftFixedChunk.getElement(add(leftFixedOffset, constantInt(keyField.fieldIsNullOffset()))).cast(int.class), constantInt(0)));
        Variable rightIsNull = scope.declareVariable("rightIsNull", body, rightBlock.invoke("isNull", boolean.class, rightPosition));

        // if (leftIsNull) {
        //     return rightIsNull;
        // }
        body.append(new IfStatement()
                .condition(leftIsNull)
                .ifTrue(rightIsNull.ret()));

        // if (rightIsNull) {
        //     return false;
        // }
        body.append(new IfStatement()
                .condition(rightIsNull)
                .ifTrue(constantFalse().ret()));

        if (keyField.type().isFlatVariableWidth()) {
            BytecodeExpression variableEndOffset = invokeStatic(MinimalFlatHashStrategyCompiler.class, "readVariableWidthEndingOffset", int.class, leftFixedChunk, add(leftFixedOffset, constantInt(keyField.fieldFixedOffset())));
            BytecodeExpression variableStartOffset;
            // read the previous variable width ending offset as starting offset, except for the first variable width field which takes
            // the starting offset directly from the parameter value
            if (keyField.previousVariableFieldFixedOffset() >= 0) {
                variableStartOffset = scope.declareVariable("leftVariableStartOffset", body, invokeStatic(MinimalFlatHashStrategyCompiler.class, "readVariableWidthEndingOffset", int.class, leftFixedChunk, add(leftFixedOffset, constantInt(keyField.previousVariableFieldFixedOffset()))));
            }
            else {
                variableStartOffset = leftVariableChunkOffset;
            }
            body.append(constantType(callSiteBinder, keyField.type()).invoke(
                    "minimalFlatVariableWidthIdentical",
                    boolean.class,
                    leftVariableChunk,
                    variableStartOffset,
                    subtract(variableEndOffset, variableStartOffset),
                    rightBlock,
                    rightPosition).ret());
        }
        else {
            body.append(invokeDynamic(
                    BOOTSTRAP_METHOD,
                    ImmutableList.of(callSiteBinder.bind(keyField.identicalFlatBlockMethod()).getBindingId()),
                    "identical",
                    boolean.class,
                    leftFixedChunk,
                    add(leftFixedOffset, constantInt(keyField.fieldFixedOffset())),
                    leftVariableChunk,
                    rightBlock,
                    rightPosition).ret());
        }
        return methodDefinition;
    }

    private static void generateHashBlock(ClassDefinition definition, List<ChunkClass> chunkClasses)
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
        for (ChunkClass chunkClass : chunkClasses) {
            body.append(result.set(invokeStatic(chunkClass.hashBlockChunk(), blocks, position, result)));
        }
        body.append(result.ret());
    }

    private static MethodDefinition generateHashBlockChunk(ClassDefinition definition, List<KeyField> keyFields, CallSiteBinder callSiteBinder)
    {
        Parameter blocks = arg("blocks", type(Block[].class));
        Parameter position = arg("position", type(int.class));
        Parameter seed = arg("seed", type(long.class));
        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC, STATIC),
                "hashBlocks",
                type(long.class),
                blocks,
                position,
                seed);
        BytecodeBlock body = methodDefinition.getBody();

        Scope scope = methodDefinition.getScope();
        Variable result = scope.declareVariable("result", body, seed);
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
        return methodDefinition;
    }

    private static void generateHashBlocksBatched(ClassDefinition definition, List<ChunkClass> chunkClasses)
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
        for (ChunkClass chunkClass : chunkClasses) {
            nonEmptyLength.append(invokeStatic(chunkClass.hashBlocksBatchedChunk(), blocks, hashes, offset, length));
        }

        body.append(new IfStatement("if (length != 0)")
                        .condition(equal(length, constantInt(0)))
                        .ifFalse(nonEmptyLength))
                .ret();
    }

    private static MethodDefinition generateHashBlocksBatchedChunk(ClassDefinition definition, List<KeyField> keyFields, CallSiteBinder callSiteBinder)
    {
        Parameter blocks = arg("blocks", type(Block[].class));
        Parameter hashes = arg("hashes", type(long[].class));
        Parameter offset = arg("offset", type(int.class));
        Parameter length = arg("length", type(int.class));

        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC, STATIC),
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
            // The first hash method implementation does not combine hashes, so it can't be reused
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

        return methodDefinition;
    }

    private static MethodDefinition generateHashBlockVectorized(ClassDefinition definition, KeyField field, CallSiteBinder callSiteBinder)
    {
        Parameter block = arg("block", type(Block.class));
        Parameter hashes = arg("hashes", type(long[].class));
        Parameter offset = arg("offset", type(int.class));
        Parameter length = arg("length", type(int.class));

        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC, STATIC),
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

        body.append(position.set(invokeStatic(Objects.class, "checkFromToIndex", int.class, offset, add(offset, length), block.invoke("getPositionCount", int.class))));
        body.append(invokeStatic(Objects.class, "checkFromIndexSize", int.class, constantInt(0), length, hashes.length()).pop());

        BytecodeExpression computeHashNonNull = invokeDynamic(
                BOOTSTRAP_METHOD,
                ImmutableList.of(callSiteBinder.bind(field.hashBlockMethod()).getBindingId()),
                "hash",
                long.class,
                block,
                position);

        BytecodeBlock rleHandling = new BytecodeBlock()
                .append(new IfStatement("hash = block.isNull(position) ? NULL_HASH_CODE : hash(block, position)")
                        .condition(block.invoke("isNull", boolean.class, position))
                        .ifTrue(hash.set(constantLong(NULL_HASH_CODE)))
                        .ifFalse(hash.set(computeHashNonNull)));
        if (field.index() == 0) {
            // Arrays.fill(hashes, 0, length, hash)
            rleHandling.append(invokeStatic(Arrays.class, "fill", void.class, hashes, constantInt(0), length, hash));
        }
        else {
            // CombineHashFunction.combineAllHashesWithConstant(hashes, 0, length, hash)
            rleHandling.append(invokeStatic(CombineHashFunction.class, "combineAllHashesWithConstant", void.class, hashes, constantInt(0), length, hash));
        }

        BytecodeExpression setHashExpression;
        if (field.index() == 0) {
            // hashes[index] = hash;
            setHashExpression = hashes.setElement(index, hash);
        }
        else {
            // hashes[index] = CombineHashFunction.getHash(hashes[index], hash);
            setHashExpression = hashes.setElement(index, invokeStatic(CombineHashFunction.class, "getHash", long.class, hashes.getElement(index), hash));
        }

        BytecodeBlock computeHashLoop = new BytecodeBlock()
                .append(mayHaveNull.set(block.invoke("mayHaveNull", boolean.class)))
                .append(new ForLoop("for (int index = 0; index < length; index++)")
                        .initialize(index.set(constantInt(0)))
                        .condition(lessThan(index, length))
                        .update(index.increment())
                        .body(new BytecodeBlock()
                                .append(new IfStatement("if (mayHaveNull && block.isNull(position))")
                                        .condition(and(mayHaveNull, block.invoke("isNull", boolean.class, position)))
                                        .ifTrue(hash.set(constantLong(NULL_HASH_CODE)))
                                        .ifFalse(hash.set(computeHashNonNull)))
                                .append(setHashExpression)
                                .append(position.increment())));

        body.append(new IfStatement("if (block instanceof RunLengthEncodedBlock)")
                        .condition(block.instanceOf(RunLengthEncodedBlock.class))
                        .ifTrue(rleHandling)
                        .ifFalse(computeHashLoop))
                .ret();

        return methodDefinition;
    }

    private static void generateHashFlat(ClassDefinition definition, List<ChunkClass> chunkClasses)
    {
        Parameter fixedChunk = arg("fixedChunk", type(byte[].class));
        Parameter fixedOffset = arg("fixedOffset", type(int.class));
        Parameter variableChunk = arg("variableChunk", type(byte[].class));
        Parameter variableChunkOffset = arg("variableChunkOffset", type(int.class));
        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC),
                "hash",
                type(long.class),
                fixedChunk,
                fixedOffset,
                variableChunk,
                variableChunkOffset);
        BytecodeBlock body = methodDefinition.getBody();

        Scope scope = methodDefinition.getScope();
        Variable result = scope.declareVariable("result", body, constantLong(INITIAL_HASH_VALUE));
        for (ChunkClass chunkClass : chunkClasses) {
            body.append(result.set(invokeStatic(chunkClass.hashFlatChunk(), fixedChunk, fixedOffset, variableChunk, variableChunkOffset, result)));
        }
        body.append(result.ret());
    }

    private static MethodDefinition generateHashFlatChunk(ClassDefinition definition, List<KeyField> keyFields, CallSiteBinder callSiteBinder)
    {
        Parameter fixedChunk = arg("fixedChunk", type(byte[].class));
        Parameter fixedOffset = arg("fixedOffset", type(int.class));
        Parameter variableChunk = arg("variableChunk", type(byte[].class));
        Parameter variableChunkOffset = arg("variableChunkOffset", type(int.class));
        Parameter seed = arg("seed", type(long.class));
        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC, STATIC),
                "hashFlat",
                type(long.class),
                fixedChunk,
                fixedOffset,
                variableChunk,
                variableChunkOffset,
                seed);
        BytecodeBlock body = methodDefinition.getBody();

        Scope scope = methodDefinition.getScope();
        Variable result = scope.declareVariable("result", body, seed);
        Variable hash = scope.declareVariable(long.class, "hash");
        // variable is only declared if a variable width type is encountered
        Variable variableEndOffset = null;

        for (KeyField keyField : keyFields) {
            BytecodeExpression hashNonNull;
            if (keyField.type().isFlatVariableWidth()) {
                // On the first variableWidth element encountered in each chunk, we need to initialize the starting variableWidthOffset from the previous ending offset
                // since variableWidthOffset is not updated between chunk calls. However, on the _first_ chunk- the variableWidthOffset value is taken directly from the
                // caller since this is the first variable width part altogether
                if (variableEndOffset == null) {
                    // declaring the variable indicates that at least one variable width field has been encountered already in the chunk and that
                    // we can use variableChunkOffset
                    variableEndOffset = scope.declareVariable(int.class, "variableEndOffset");
                    if (keyField.previousVariableFieldFixedOffset() >= 0) {
                        // variableChunkOffset = previousVariableWidthEndOffset
                        body.append(variableChunkOffset.set(invokeStatic(MinimalFlatHashStrategyCompiler.class, "readVariableWidthEndingOffset", int.class, fixedChunk, add(fixedOffset, constantInt(keyField.previousVariableFieldFixedOffset())))));
                    }
                }
                // variableEndOffset = MinimalFlatHashStrategyCompiler.readVariableEndingOffset(fixedChunk, fixedOffset)
                body.append(variableEndOffset.set(invokeStatic(MinimalFlatHashStrategyCompiler.class, "readVariableWidthEndingOffset", int.class, fixedChunk, add(fixedOffset, constantInt(keyField.fieldFixedOffset())))));

                hashNonNull = constantType(callSiteBinder, keyField.type()).invoke(
                        "hashMinimalFlatVariableWidth",
                        long.class,
                        variableChunk,
                        variableChunkOffset,
                        subtract(variableEndOffset, variableChunkOffset));
            }
            else {
                hashNonNull = invokeDynamic(
                        BOOTSTRAP_METHOD,
                        ImmutableList.of(callSiteBinder.bind(keyField.hashFlatMethod()).getBindingId()),
                        "hash",
                        long.class,
                        fixedChunk,
                        add(fixedOffset, constantInt(keyField.fieldFixedOffset())),
                        variableChunk);
            }

            body.append(new IfStatement()
                    .condition(notEqual(fixedChunk.getElement(add(fixedOffset, constantInt(keyField.fieldIsNullOffset()))).cast(int.class), constantInt(0)))
                    .ifTrue(hash.set(constantLong(NULL_HASH_CODE)))
                    .ifFalse(hash.set(hashNonNull)));
            body.append(result.set(invokeStatic(CombineHashFunction.class, "getHash", long.class, result, hash)));

            // Advance the variable start offset
            if (keyField.type().isFlatVariableWidth()) {
                // variableChunkOffset = variableEndOffset
                body.append(variableChunkOffset.set(variableEndOffset));
            }
        }
        body.append(result.ret());
        return methodDefinition;
    }

    @UsedByGeneratedCode
    public static int readVariableWidthEndingOffset(byte[] fixedBytes, int fixedBytesOffset)
    {
        return (int) INT_HANDLE.get(fixedBytes, fixedBytesOffset);
    }

    @UsedByGeneratedCode
    public static void writeVariableWidthEndingOffset(byte[] fixedBytes, int fixedBytesOffset, int endingVariableWidthOffset)
    {
        INT_HANDLE.set(fixedBytes, fixedBytesOffset, endingVariableWidthOffset);
    }

    private record KeyField(
            int index,
            Type type,
            int fieldIsNullOffset,
            int fieldFixedOffset,
            int previousVariableFieldFixedOffset,
            MethodHandle readFlatMethod,
            MethodHandle writeFlatMethod,
            MethodHandle identicalFlatBlockMethod,
            MethodHandle hashFlatMethod,
            MethodHandle hashBlockMethod) {}

    private record ChunkClass(
            ClassDefinition definition,
            MethodDefinition getTotalVariableWidth,
            MethodDefinition readFlatChunk,
            MethodDefinition writeFlatChunk,
            MethodDefinition identicalMethodChunk,
            MethodDefinition hashFlatChunk,
            MethodDefinition hashBlockChunk,
            MethodDefinition hashBlocksBatchedChunk) {}
}
