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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
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
import io.trino.cache.CacheStatsMBean;
import io.trino.operator.scalar.CombineHashFunction;
import io.trino.spi.BlocksHashFactory;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.Binding;
import io.trino.sql.gen.CallSiteBinder;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.add;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantBoolean;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantLong;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantTrue;
import static io.airlift.bytecode.expression.BytecodeExpressions.greaterThan;
import static io.airlift.bytecode.expression.BytecodeExpressions.inlineIf;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.lessThan;
import static io.airlift.bytecode.expression.BytecodeExpressions.newInstance;
import static io.airlift.bytecode.expression.BytecodeExpressions.not;
import static io.airlift.bytecode.expression.BytecodeExpressions.notEqual;
import static io.airlift.bytecode.expression.BytecodeExpressions.subtract;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.operator.HashGenerator.INITIAL_HASH_VALUE;
import static io.trino.operator.InterpretedHashGenerator.createPagePrefixHashGenerator;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.BLOCK_BUILDER;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.TypeUtils.NULL_HASH_CODE;
import static io.trino.sql.gen.BytecodeUtils.invoke;
import static io.trino.sql.gen.BytecodeUtils.loadConstant;
import static io.trino.sql.gen.SqlTypeBytecodeExpression.constantType;
import static io.trino.util.CompilerUtils.defineHiddenClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static java.util.Objects.requireNonNull;

public final class FlatHashStrategyCompiler
{
    @VisibleForTesting
    static final int COLUMNS_PER_CHUNK = 500;

    private final LoadingCache<List<Type>, FlatHashStrategy> flatHashStrategies;
    private final NullSafeHashCompiler nullSafeHashCompiler;

    @Inject
    public FlatHashStrategyCompiler(TypeOperators typeOperators, NullSafeHashCompiler nullSafeHashCompiler)
    {
        this.nullSafeHashCompiler = requireNonNull(nullSafeHashCompiler, "nullSafeHashCompiler is null");
        this.flatHashStrategies = buildNonEvictableCache(
                CacheBuilder.newBuilder()
                        .recordStats()
                        .maximumSize(1000),
                CacheLoader.from(key -> compileFlatHashStrategy(key, typeOperators)));
    }

    public FlatHashStrategy getFlatHashStrategy(List<Type> types)
    {
        return flatHashStrategies.getUnchecked(ImmutableList.copyOf(types));
    }

    public InterpretedHashGenerator getInterpretedHashGenerator(List<Type> types)
    {
        return createPagePrefixHashGenerator(types, nullSafeHashCompiler);
    }

    public BlocksHashFactory createBlocksHashFactory()
    {
        return (types, cacheHashValue, expectedSize) -> new FlatHash(getFlatHashStrategy(types), cacheHashValue, expectedSize, UpdateMemory.NOOP);
    }

    @Managed
    @Nested
    public CacheStatsMBean getFlatHashStrategiesStats()
    {
        return new CacheStatsMBean(flatHashStrategies);
    }

    @VisibleForTesting
    public static FlatHashStrategy compileFlatHashStrategy(List<Type> types, TypeOperators typeOperators)
    {
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
                    typeOperators.getIdenticalOperator(type, simpleConvention(FAIL_ON_NULL, FLAT, BLOCK_POSITION_NOT_NULL)),
                    typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, FLAT)),
                    typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL))));
            fixedOffset += 1 + type.getFlatFixedSize();
        }

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        List<ChunkClass> chunkClasses = new ArrayList<>();
        int chunkNumber = 0;
        // generate a separate class for each chunk of 500 types to avoid hitting the JVM method size and constant pool limits
        boolean singleChunkClass = keyFields.size() <= COLUMNS_PER_CHUNK;
        for (List<KeyField> chunk : Lists.partition(keyFields, COLUMNS_PER_CHUNK)) {
            chunkClasses.add(compileFlatHashStrategyChunk(callSiteBinder, chunk, chunkNumber, singleChunkClass));
            chunkNumber++;
        }

        // The chunk classes are hidden and cannot be referenced by name from the strategy
        // class, so their entry points are invoked through bound method handles. The chunk
        // classes only need the bindings created so far, so sharing the binder is safe.
        List<ChunkBindings> chunkBindings = new ArrayList<>();
        for (ChunkClass chunkClass : chunkClasses) {
            Class<?> definedChunk = defineHiddenClass(chunkClass.definition(), Object.class, callSiteBinder.getClassData());
            chunkBindings.add(new ChunkBindings(
                    bindChunkMethod(callSiteBinder, definedChunk, chunkClass.getTotalVariableWidth()),
                    bindChunkMethod(callSiteBinder, definedChunk, chunkClass.readFlatChunk()),
                    bindChunkMethod(callSiteBinder, definedChunk, chunkClass.writeFlatChunk()),
                    bindChunkMethod(callSiteBinder, definedChunk, chunkClass.identicalMethodChunk()),
                    bindChunkMethod(callSiteBinder, definedChunk, chunkClass.hashBlockChunk()),
                    bindChunkMethod(callSiteBinder, definedChunk, chunkClass.hashFlatChunk())));
        }

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

        boolean anyVariableWidth = (int) types.stream().filter(Type::isFlatVariableWidth).count() > 0;
        definition.declareMethod(a(PUBLIC), "isAnyVariableWidth", type(boolean.class)).getBody()
                .append(constantBoolean(anyVariableWidth).ret());

        definition.declareMethod(a(PUBLIC), "getTotalFlatFixedLength", type(int.class)).getBody()
                .append(constantInt(fixedOffset).ret());

        generateGetTotalVariableWidth(definition, chunkBindings);

        generateReadFlat(definition, chunkBindings);
        generateWriteFlat(definition, chunkBindings);
        generateIdenticalMethod(definition, chunkBindings);
        generateHashBlock(definition, chunkBindings);
        generateHashFlat(definition, chunkBindings, singleChunkClass);

        try {
            return defineHiddenClass(definition, FlatHashStrategy.class, callSiteBinder.getClassData())
                    .getConstructor()
                    .newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static ChunkClass compileFlatHashStrategyChunk(CallSiteBinder callSiteBinder, List<KeyField> keyFields, int chunkNumber, boolean singleChunkClass)
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
        MethodDefinition hashBlockChunk = generateHashBlockChunk(definition, keyFields, callSiteBinder);
        MethodDefinition hashFlatChunk;
        if (singleChunkClass) {
            hashFlatChunk = generateHashFlatSingleChunk(definition, keyFields, callSiteBinder);
        }
        else {
            hashFlatChunk = generateHashFlatMultiChunk(definition, keyFields, callSiteBinder);
        }

        return new ChunkClass(
                definition,
                getTotalVariableWidthChunk,
                readFlatChunk,
                writeFlatChunk,
                identicalChunkMethod,
                hashBlockChunk,
                hashFlatChunk);
    }

    private static void generateGetTotalVariableWidth(ClassDefinition definition, List<ChunkBindings> chunkBindings)
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
        for (ChunkBindings chunk : chunkBindings) {
            body.append(variableWidth.set(add(variableWidth, invoke(chunk.getTotalVariableWidth(), "getTotalVariableWidth", blocks, position))));
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

        for (FieldRun run : partitionIntoRuns(keyFields)) {
            Type type = run.first().type();
            if (!type.isFlatVariableWidth()) {
                continue;
            }
            if (run.size() < LOOP_THRESHOLD) {
                for (KeyField keyField : run.fields()) {
                    body.append(totalVariableWidthField(callSiteBinder, type, blocks.getElement(keyField.index()), position, variableWidth));
                }
            }
            else {
                Variable channel = scope.getOrCreateTempVariable(int.class);
                body.append(new ForLoop()
                        .initialize(channel.set(constantInt(run.first().index())))
                        .condition(lessThan(channel, constantInt(run.first().index() + run.size())))
                        .update(channel.increment())
                        .body(totalVariableWidthField(callSiteBinder, type, blocks.getElement(channel), position, variableWidth)));
                scope.releaseTempVariableForReuse(channel);
            }
        }
        body.append(variableWidth.ret());
        return methodDefinition;
    }

    private static BytecodeNode totalVariableWidthField(CallSiteBinder callSiteBinder, Type type, BytecodeExpression block, Parameter position, Variable variableWidth)
    {
        return new IfStatement()
                .condition(not(block.invoke("isNull", boolean.class, position)))
                .ifTrue(variableWidth.set(add(
                        variableWidth,
                        constantType(callSiteBinder, type).invoke("getFlatVariableWidthSize", int.class, block, position).cast(long.class))));
    }

    private static void generateReadFlat(ClassDefinition definition, List<ChunkBindings> chunkBindings)
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
        for (ChunkBindings chunk : chunkBindings) {
            body.append(variableOffset.set(invoke(chunk.readFlatChunk(), "readFlat", fixedChunk, fixedOffset, variableChunk, variableOffset, blockBuilders)));
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
        BytecodeBlock body = methodDefinition.getBody();

        Scope scope = methodDefinition.getScope();
        for (FieldRun run : partitionIntoRuns(keyFields)) {
            if (run.size() < LOOP_THRESHOLD) {
                for (KeyField keyField : run.fields()) {
                    body.append(readFlatField(
                            callSiteBinder,
                            keyField,
                            add(fixedOffset, constantInt(keyField.fieldFixedOffset())),
                            add(fixedOffset, constantInt(keyField.fieldIsNullOffset())),
                            blockBuilders.getElement(keyField.index()),
                            fixedChunk,
                            variableChunk,
                            variableOffset));
                }
            }
            else {
                Variable channel = scope.getOrCreateTempVariable(int.class);
                Variable fieldOffset = scope.getOrCreateTempVariable(int.class);
                body.append(new ForLoop()
                        .initialize(new BytecodeBlock()
                                .append(channel.set(constantInt(run.first().index())))
                                .append(fieldOffset.set(add(fixedOffset, constantInt(run.first().fieldFixedOffset())))))
                        .condition(lessThan(channel, constantInt(run.first().index() + run.size())))
                        .update(new BytecodeBlock()
                                .append(channel.increment())
                                .append(fieldOffset.set(add(fieldOffset, constantInt(run.fixedOffsetStride())))))
                        .body(readFlatField(
                                callSiteBinder,
                                run.first(),
                                fieldOffset,
                                add(fieldOffset, constantInt(run.isNullOffsetDelta())),
                                blockBuilders.getElement(channel),
                                fixedChunk,
                                variableChunk,
                                variableOffset)));
                scope.releaseTempVariableForReuse(fieldOffset);
                scope.releaseTempVariableForReuse(channel);
            }
        }
        body.append(variableOffset.ret());
        return methodDefinition;
    }

    private static BytecodeNode readFlatField(
            CallSiteBinder callSiteBinder,
            KeyField keyField,
            BytecodeExpression fieldFixedOffset,
            BytecodeExpression fieldIsNullOffset,
            BytecodeExpression blockBuilder,
            Parameter fixedChunk,
            Parameter variableChunk,
            Parameter variableOffset)
    {
        BytecodeBlock readNonNull = new BytecodeBlock()
                .append(invoke(
                        callSiteBinder.bind(keyField.readFlatMethod()),
                        "readFlat",
                        fixedChunk,
                        fieldFixedOffset,
                        variableChunk,
                        variableOffset,
                        blockBuilder));
        if (keyField.type().isFlatVariableWidth()) {
            // variableOffset += type.getFlatVariableWidthLength(fixedChunk, fieldFixedOffset);
            readNonNull.append(variableOffset.set(add(
                    variableOffset,
                    constantType(callSiteBinder, keyField.type()).invoke(
                            "getFlatVariableWidthLength",
                            int.class,
                            fixedChunk,
                            fieldFixedOffset))));
        }
        return new IfStatement()
                .condition(notEqual(fixedChunk.getElement(fieldIsNullOffset).cast(int.class), constantInt(0)))
                .ifTrue(blockBuilder.invoke("appendNull", BlockBuilder.class).pop())
                .ifFalse(readNonNull);
    }

    private static void generateWriteFlat(ClassDefinition definition, List<ChunkBindings> chunkBindings)
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
        for (ChunkBindings chunk : chunkBindings) {
            body.append(variableOffset.set(invoke(chunk.writeFlatChunk(), "writeFlat", blocks, position, fixedChunk, fixedOffset, variableChunk, variableOffset)));
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
        Scope scope = methodDefinition.getScope();
        for (FieldRun run : partitionIntoRuns(keyFields)) {
            if (run.size() < LOOP_THRESHOLD) {
                for (KeyField keyField : run.fields()) {
                    body.append(writeFlatField(
                            callSiteBinder,
                            keyField,
                            add(fixedOffset, constantInt(keyField.fieldFixedOffset())),
                            add(fixedOffset, constantInt(keyField.fieldIsNullOffset())),
                            blocks.getElement(keyField.index()),
                            position,
                            fixedChunk,
                            variableChunk,
                            variableOffset));
                }
            }
            else {
                Variable channel = scope.getOrCreateTempVariable(int.class);
                Variable fieldOffset = scope.getOrCreateTempVariable(int.class);
                body.append(new ForLoop()
                        .initialize(new BytecodeBlock()
                                .append(channel.set(constantInt(run.first().index())))
                                .append(fieldOffset.set(add(fixedOffset, constantInt(run.first().fieldFixedOffset())))))
                        .condition(lessThan(channel, constantInt(run.first().index() + run.size())))
                        .update(new BytecodeBlock()
                                .append(channel.increment())
                                .append(fieldOffset.set(add(fieldOffset, constantInt(run.fixedOffsetStride())))))
                        .body(writeFlatField(
                                callSiteBinder,
                                run.first(),
                                fieldOffset,
                                add(fieldOffset, constantInt(run.isNullOffsetDelta())),
                                blocks.getElement(channel),
                                position,
                                fixedChunk,
                                variableChunk,
                                variableOffset)));
                scope.releaseTempVariableForReuse(fieldOffset);
                scope.releaseTempVariableForReuse(channel);
            }
        }
        body.append(variableOffset.ret());
        return methodDefinition;
    }

    private static BytecodeNode writeFlatField(
            CallSiteBinder callSiteBinder,
            KeyField keyField,
            BytecodeExpression fieldFixedOffset,
            BytecodeExpression fieldIsNullOffset,
            BytecodeExpression block,
            Parameter position,
            Parameter fixedChunk,
            Parameter variableChunk,
            Parameter variableOffset)
    {
        BytecodeBlock writeNonNullFlat = new BytecodeBlock()
                .append(invoke(
                        callSiteBinder.bind(keyField.writeFlatMethod()),
                        "writeFlat",
                        block,
                        position,
                        fixedChunk,
                        fieldFixedOffset,
                        variableChunk,
                        variableOffset));
        if (keyField.type().isFlatVariableWidth()) {
            // variableOffset += type.getFlatVariableWidthLength(fixedChunk, fieldFixedOffset);
            writeNonNullFlat.append(variableOffset.set(add(variableOffset, constantType(callSiteBinder, keyField.type()).invoke(
                    "getFlatVariableWidthLength",
                    int.class,
                    fixedChunk,
                    fieldFixedOffset))));
        }
        return new IfStatement()
                .condition(block.invoke("isNull", boolean.class, position))
                .ifTrue(fixedChunk.setElement(fieldIsNullOffset, constantInt(1).cast(byte.class)))
                .ifFalse(writeNonNullFlat);
    }

    private static void generateIdenticalMethod(ClassDefinition definition, List<ChunkBindings> chunkBindings)
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
        // leftVariableChunkOffset = FlatHashStrategyCompiler.checkVariableWidthOffsetArgument(leftVariableChunkOffset)
        body.append(leftVariableChunkOffset.set(invokeStatic(FlatHashStrategyCompiler.class, "checkVariableWidthOffsetArgument", int.class, leftVariableChunkOffset)));
        for (ChunkBindings chunk : chunkBindings) {
            // leftVariableChunkOffset = Chunk.valueIdentical(leftFixedChunk, leftFixedOffset, leftVariableChunk, leftVariableChunkOffset, rightBlocks, rightPosition);
            body.append(leftVariableChunkOffset.set(invoke(chunk.identicalMethodChunk(), "valueIdentical", leftFixedChunk, leftFixedOffset, leftVariableChunk, leftVariableChunkOffset, rightBlocks, rightPosition)));
            // if (leftVariableChunkOffset < 0) {
            //    return false;
            // }
            body.append(new IfStatement()
                    .condition(lessThan(leftVariableChunkOffset, constantInt(0)))
                    .ifTrue(constantFalse().ret()));
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
                type(int.class),
                leftFixedChunk,
                leftFixedOffset,
                leftVariableChunk,
                leftVariableChunkOffset,
                rightBlocks,
                rightPosition);
        BytecodeBlock body = methodDefinition.getBody();
        // leftVariableChunkOffset = FlatHashStrategyCompiler.checkVariableWidthOffsetArgument(leftVariableChunkOffset)
        body.append(leftVariableChunkOffset.set(invokeStatic(FlatHashStrategyCompiler.class, "checkVariableWidthOffsetArgument", int.class, leftVariableChunkOffset)));

        // identical methods are generated once per type and take the field offsets as
        // arguments, so runs of same-typed fields share a single helper
        Map<Type, MethodDefinition> identicalMethods = new HashMap<>();
        Scope scope = methodDefinition.getScope();
        for (FieldRun run : partitionIntoRuns(keyFields)) {
            KeyField first = run.first();
            MethodDefinition identicalMethod = identicalMethods.computeIfAbsent(first.type(), _ -> {
                if (first.type().isFlatVariableWidth()) {
                    return generateVariableWidthIdenticalMethod(definition, first, callSiteBinder, identicalMethods.size());
                }
                return generateFixedWidthIdenticalMethod(definition, first, callSiteBinder, identicalMethods.size());
            });
            if (run.size() < LOOP_THRESHOLD) {
                for (KeyField keyField : run.fields()) {
                    body.append(identicalField(
                            keyField,
                            identicalMethod,
                            add(leftFixedOffset, constantInt(keyField.fieldFixedOffset())),
                            add(leftFixedOffset, constantInt(keyField.fieldIsNullOffset())),
                            rightBlocks.getElement(keyField.index()),
                            leftFixedChunk,
                            leftVariableChunk,
                            leftVariableChunkOffset,
                            rightPosition));
                }
            }
            else {
                Variable channel = scope.getOrCreateTempVariable(int.class);
                Variable fieldOffset = scope.getOrCreateTempVariable(int.class);
                body.append(new ForLoop()
                        .initialize(new BytecodeBlock()
                                .append(channel.set(constantInt(first.index())))
                                .append(fieldOffset.set(add(leftFixedOffset, constantInt(first.fieldFixedOffset())))))
                        .condition(lessThan(channel, constantInt(first.index() + run.size())))
                        .update(new BytecodeBlock()
                                .append(channel.increment())
                                .append(fieldOffset.set(add(fieldOffset, constantInt(run.fixedOffsetStride())))))
                        .body(identicalField(
                                first,
                                identicalMethod,
                                fieldOffset,
                                add(fieldOffset, constantInt(run.isNullOffsetDelta())),
                                rightBlocks.getElement(channel),
                                leftFixedChunk,
                                leftVariableChunk,
                                leftVariableChunkOffset,
                                rightPosition)));
                scope.releaseTempVariableForReuse(fieldOffset);
                scope.releaseTempVariableForReuse(channel);
            }
        }
        body.append(leftVariableChunkOffset.ret());
        return methodDefinition;
    }

    private static BytecodeNode identicalField(
            KeyField keyField,
            MethodDefinition identicalMethod,
            BytecodeExpression fieldFixedOffset,
            BytecodeExpression fieldIsNullOffset,
            BytecodeExpression rightBlock,
            Parameter leftFixedChunk,
            Parameter leftVariableChunk,
            Parameter leftVariableChunkOffset,
            Parameter rightPosition)
    {
        // variable width identical methods take leftVariableChunk and leftVariableChunkOffset arguments, while
        // fixed width types omit those arguments entirely. Variable width methods return -1 for false, otherwise
        // they return the current variableWidthOffset which will be >= 0
        if (keyField.type().isFlatVariableWidth()) {
            return new BytecodeBlock()
                    // leftVariableChunkOffset = identicalMethod(leftFixedChunk, fieldFixedOffset, fieldIsNullOffset, leftVariableChunk, leftVariableChunkOffset, rightBlock, rightPosition)
                    .append(leftVariableChunkOffset.set(invokeStatic(identicalMethod, leftFixedChunk, fieldFixedOffset, fieldIsNullOffset, leftVariableChunk, leftVariableChunkOffset, rightBlock, rightPosition)))
                    // if (leftVariableChunkOffset < 0) {
                    //    return -1;
                    // }
                    .append(new IfStatement()
                            .condition(lessThan(leftVariableChunkOffset, constantInt(0)))
                            .ifTrue(constantInt(-1).ret()));
        }
        // if (!identicalMethod(leftFixedChunk, fieldFixedOffset, fieldIsNullOffset, rightBlock, rightPosition)) {
        //   return -1;
        // }
        return new IfStatement()
                .condition(invokeStatic(identicalMethod, leftFixedChunk, fieldFixedOffset, fieldIsNullOffset, rightBlock, rightPosition))
                .ifFalse(constantInt(-1).ret());
    }

    private static MethodDefinition generateFixedWidthIdenticalMethod(ClassDefinition definition, KeyField keyField, CallSiteBinder callSiteBinder, int typeId)
    {
        checkArgument(!keyField.type().isFlatVariableWidth(), "type is not fixed width");

        Parameter leftFixedChunk = arg("leftFixedChunk", type(byte[].class));
        Parameter fieldFixedOffset = arg("fieldFixedOffset", type(int.class));
        Parameter fieldIsNullOffset = arg("fieldIsNullOffset", type(int.class));
        Parameter rightBlock = arg("rightBlock", type(Block.class));
        Parameter rightPosition = arg("rightPosition", type(int.class));
        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC, STATIC),
                "valueIdenticalType" + typeId,
                type(boolean.class),
                leftFixedChunk,
                fieldFixedOffset,
                fieldIsNullOffset,
                rightBlock,
                rightPosition);
        BytecodeBlock body = methodDefinition.getBody();
        Scope scope = methodDefinition.getScope();

        Variable leftIsNull = scope.declareVariable("leftIsNull", body, notEqual(leftFixedChunk.getElement(fieldIsNullOffset).cast(int.class), constantInt(0)));
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

        body.append(invoke(
                callSiteBinder.bind(keyField.identicalFlatBlockMethod()),
                "identical",
                leftFixedChunk,
                fieldFixedOffset,
                constantNull(byte[].class),
                constantInt(0),
                rightBlock,
                rightPosition)
                .ret());
        return methodDefinition;
    }

    private static MethodDefinition generateVariableWidthIdenticalMethod(ClassDefinition definition, KeyField keyField, CallSiteBinder callSiteBinder, int typeId)
    {
        checkArgument(keyField.type().isFlatVariableWidth(), "type is not variable width");

        Parameter leftFixedChunk = arg("leftFixedChunk", type(byte[].class));
        Parameter fieldFixedOffset = arg("fieldFixedOffset", type(int.class));
        Parameter fieldIsNullOffset = arg("fieldIsNullOffset", type(int.class));
        Parameter leftVariableChunk = arg("leftVariableChunk", type(byte[].class));
        Parameter leftVariableChunkOffset = arg("leftVariableChunkOffset", type(int.class));
        Parameter rightBlock = arg("rightBlock", type(Block.class));
        Parameter rightPosition = arg("rightPosition", type(int.class));
        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC, STATIC),
                "valueIdenticalType" + typeId,
                type(int.class),
                leftFixedChunk,
                fieldFixedOffset,
                fieldIsNullOffset,
                leftVariableChunk,
                leftVariableChunkOffset,
                rightBlock,
                rightPosition);
        BytecodeBlock body = methodDefinition.getBody();
        Scope scope = methodDefinition.getScope();

        Variable leftIsNull = scope.declareVariable("leftIsNull", body, notEqual(leftFixedChunk.getElement(fieldIsNullOffset).cast(int.class), constantInt(0)));
        Variable rightIsNull = scope.declareVariable("rightIsNull", body, rightBlock.invoke("isNull", boolean.class, rightPosition));

        // if (leftIsNull) {
        //    return rightIsNull ? leftVariableChunkOffset : -1;
        // }
        body.append(new IfStatement()
                .condition(leftIsNull)
                .ifTrue(inlineIf(rightIsNull, leftVariableChunkOffset, constantInt(-1)).ret()));

        // if (rightIsNull) {
        //     return -1;
        // }
        body.append(new IfStatement()
                .condition(rightIsNull)
                .ifTrue(constantInt(-1).ret()));

        // if (identical(leftFixedChunk, fieldFixedOffset, leftVariableChunk, leftVariableOffset, rightBlock, rightPosition)) {
        //   return leftVariableOffset + type.getFlatVariableWidthLength(leftFixedChunk, fieldFixedOffset);
        // }
        // else {
        //   return -1;
        // }
        body.append(new IfStatement().condition(invoke(
                        callSiteBinder.bind(keyField.identicalFlatBlockMethod()),
                        "identical",
                        leftFixedChunk,
                        fieldFixedOffset,
                        leftVariableChunk,
                        leftVariableChunkOffset,
                        rightBlock,
                        rightPosition))
                .ifTrue(add(leftVariableChunkOffset, constantType(callSiteBinder, keyField.type()).invoke(
                        "getFlatVariableWidthLength",
                        int.class,
                        leftFixedChunk,
                        fieldFixedOffset)).ret())
                .ifFalse(constantInt(-1).ret()));
        return methodDefinition;
    }

    private static void generateHashBlock(ClassDefinition definition, List<ChunkBindings> chunkBindings)
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
        for (ChunkBindings chunk : chunkBindings) {
            body.append(result.set(invoke(chunk.hashBlockChunk(), "hashBlocks", blocks, position, result)));
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

        for (FieldRun run : partitionIntoRuns(keyFields)) {
            if (run.size() < LOOP_THRESHOLD) {
                for (KeyField keyField : run.fields()) {
                    body.append(hashBlockField(callSiteBinder, keyField, blocks.getElement(keyField.index()), position, block, hash, result));
                }
            }
            else {
                Variable channel = scope.getOrCreateTempVariable(int.class);
                body.append(new ForLoop()
                        .initialize(channel.set(constantInt(run.first().index())))
                        .condition(lessThan(channel, constantInt(run.first().index() + run.size())))
                        .update(channel.increment())
                        .body(hashBlockField(callSiteBinder, run.first(), blocks.getElement(channel), position, block, hash, result)));
                scope.releaseTempVariableForReuse(channel);
            }
        }
        body.append(result.ret());
        return methodDefinition;
    }

    private static BytecodeNode hashBlockField(
            CallSiteBinder callSiteBinder,
            KeyField keyField,
            BytecodeExpression blockElement,
            Parameter position,
            Variable block,
            Variable hash,
            Variable result)
    {
        return new BytecodeBlock()
                .append(block.set(blockElement))
                .append(new IfStatement()
                        .condition(block.invoke("isNull", boolean.class, position))
                        .ifTrue(hash.set(constantLong(NULL_HASH_CODE)))
                        .ifFalse(hash.set(invoke(
                                callSiteBinder.bind(keyField.hashBlockMethod()),
                                "hash",
                                block,
                                position))))
                .append(result.set(invokeStatic(CombineHashFunction.class, "getHash", long.class, result, hash)));
    }

    private static void generateHashFlat(ClassDefinition definition, List<ChunkBindings> chunkBindings, boolean singleChunkClass)
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

        if (singleChunkClass) {
            ChunkBindings chunk = getOnlyElement(chunkBindings);
            // single chunk implementation takes variableChunkOffset directly
            body.append(invoke(chunk.hashFlatChunk(), "hashFlat", fixedChunk, fixedOffset, variableChunk, variableChunkOffset, constantLong(INITIAL_HASH_VALUE)).ret());
        }
        else {
            // multi chunk implementation must pass variableChunkOffset as a MutableVariableWidthOffset to propagate the
            // value between chunk class method calls
            Scope scope = methodDefinition.getScope();
            Variable result = scope.declareVariable("result", body, constantLong(INITIAL_HASH_VALUE));
            Variable mutableVariableWidthOffset = scope.declareVariable("mutableOffset", body, newInstance(MutableVariableWidthOffset.class, variableChunkOffset));
            for (ChunkBindings chunk : chunkBindings) {
                body.append(result.set(invoke(chunk.hashFlatChunk(), "hashFlat", fixedChunk, fixedOffset, variableChunk, mutableVariableWidthOffset, result)));
            }
            body.append(result.ret());
        }
    }

    private static MethodDefinition generateHashFlatSingleChunk(ClassDefinition definition, List<KeyField> keyFields, CallSiteBinder callSiteBinder)
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

        for (FieldRun run : partitionIntoRuns(keyFields)) {
            if (run.size() < LOOP_THRESHOLD) {
                for (KeyField keyField : run.fields()) {
                    body.append(hashFlatSingleField(
                            callSiteBinder,
                            keyField,
                            add(fixedOffset, constantInt(keyField.fieldFixedOffset())),
                            add(fixedOffset, constantInt(keyField.fieldIsNullOffset())),
                            fixedChunk,
                            variableChunk,
                            variableChunkOffset,
                            hash,
                            result));
                }
            }
            else {
                Variable fieldOffset = scope.getOrCreateTempVariable(int.class);
                Variable remaining = scope.getOrCreateTempVariable(int.class);
                body.append(new ForLoop()
                        .initialize(new BytecodeBlock()
                                .append(remaining.set(constantInt(run.size())))
                                .append(fieldOffset.set(add(fixedOffset, constantInt(run.first().fieldFixedOffset())))))
                        .condition(greaterThan(remaining, constantInt(0)))
                        .update(new BytecodeBlock()
                                .append(remaining.set(subtract(remaining, constantInt(1))))
                                .append(fieldOffset.set(add(fieldOffset, constantInt(run.fixedOffsetStride())))))
                        .body(hashFlatSingleField(
                                callSiteBinder,
                                run.first(),
                                fieldOffset,
                                add(fieldOffset, constantInt(run.isNullOffsetDelta())),
                                fixedChunk,
                                variableChunk,
                                variableChunkOffset,
                                hash,
                                result)));
                scope.releaseTempVariableForReuse(remaining);
                scope.releaseTempVariableForReuse(fieldOffset);
            }
        }
        body.append(result.ret());
        return methodDefinition;
    }

    private static BytecodeNode hashFlatSingleField(
            CallSiteBinder callSiteBinder,
            KeyField keyField,
            BytecodeExpression fieldFixedOffset,
            BytecodeExpression fieldIsNullOffset,
            Parameter fixedChunk,
            Parameter variableChunk,
            Parameter variableChunkOffset,
            Variable hash,
            Variable result)
    {
        BytecodeBlock hashNonNull = new BytecodeBlock().append(hash.set(invoke(
                callSiteBinder.bind(keyField.hashFlatMethod()),
                "hash",
                fixedChunk,
                fieldFixedOffset,
                variableChunk,
                variableChunkOffset)));
        if (keyField.type().isFlatVariableWidth()) {
            // variableChunkOffset += type.getFlatVariableWidthLength(fixedChunk, fieldFixedOffset);
            hashNonNull.append(
                    variableChunkOffset.set(add(variableChunkOffset, constantType(callSiteBinder, keyField.type()).invoke(
                            "getFlatVariableWidthLength",
                            int.class,
                            fixedChunk,
                            fieldFixedOffset))));
        }
        return new BytecodeBlock()
                .append(new IfStatement()
                        .condition(notEqual(fixedChunk.getElement(fieldIsNullOffset).cast(int.class), constantInt(0)))
                        .ifTrue(hash.set(constantLong(NULL_HASH_CODE)))
                        .ifFalse(hashNonNull))
                .append(result.set(invokeStatic(CombineHashFunction.class, "getHash", long.class, result, hash)));
    }

    private static MethodDefinition generateHashFlatMultiChunk(ClassDefinition definition, List<KeyField> keyFields, CallSiteBinder callSiteBinder)
    {
        Parameter fixedChunk = arg("fixedChunk", type(byte[].class));
        Parameter fixedOffset = arg("fixedOffset", type(int.class));
        Parameter variableChunk = arg("variableChunk", type(byte[].class));
        Parameter mutableVariableChunkOffset = arg("mutableVariableChunkOffset", type(MutableVariableWidthOffset.class));
        Parameter seed = arg("seed", type(long.class));
        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC, STATIC),
                "hashFlat",
                type(long.class),
                fixedChunk,
                fixedOffset,
                variableChunk,
                mutableVariableChunkOffset,
                seed);
        BytecodeBlock body = methodDefinition.getBody();

        Scope scope = methodDefinition.getScope();
        Variable result = scope.declareVariable("result", body, seed);
        Variable hash = scope.declareVariable(long.class, "hash");

        for (FieldRun run : partitionIntoRuns(keyFields)) {
            if (run.size() < LOOP_THRESHOLD) {
                for (KeyField keyField : run.fields()) {
                    body.append(hashFlatMultiField(
                            callSiteBinder,
                            keyField,
                            add(fixedOffset, constantInt(keyField.fieldFixedOffset())),
                            add(fixedOffset, constantInt(keyField.fieldIsNullOffset())),
                            fixedChunk,
                            variableChunk,
                            mutableVariableChunkOffset,
                            hash,
                            result));
                }
            }
            else {
                Variable fieldOffset = scope.getOrCreateTempVariable(int.class);
                Variable remaining = scope.getOrCreateTempVariable(int.class);
                body.append(new ForLoop()
                        .initialize(new BytecodeBlock()
                                .append(remaining.set(constantInt(run.size())))
                                .append(fieldOffset.set(add(fixedOffset, constantInt(run.first().fieldFixedOffset())))))
                        .condition(greaterThan(remaining, constantInt(0)))
                        .update(new BytecodeBlock()
                                .append(remaining.set(subtract(remaining, constantInt(1))))
                                .append(fieldOffset.set(add(fieldOffset, constantInt(run.fixedOffsetStride())))))
                        .body(hashFlatMultiField(
                                callSiteBinder,
                                run.first(),
                                fieldOffset,
                                add(fieldOffset, constantInt(run.isNullOffsetDelta())),
                                fixedChunk,
                                variableChunk,
                                mutableVariableChunkOffset,
                                hash,
                                result)));
                scope.releaseTempVariableForReuse(remaining);
                scope.releaseTempVariableForReuse(fieldOffset);
            }
        }
        body.append(result.ret());
        return methodDefinition;
    }

    private static BytecodeNode hashFlatMultiField(
            CallSiteBinder callSiteBinder,
            KeyField keyField,
            BytecodeExpression fieldFixedOffset,
            BytecodeExpression fieldIsNullOffset,
            Parameter fixedChunk,
            Parameter variableChunk,
            Parameter mutableVariableChunkOffset,
            Variable hash,
            Variable result)
    {
        BytecodeExpression variableWidthOffset;
        if (keyField.type().isFlatVariableWidth()) {
            // mutableVariableChunkOffset.getAndAdd(type.getFlatVariableWidthLength(fixedChunk, fieldFixedOffset))
            variableWidthOffset = mutableVariableChunkOffset.invoke(
                    "getAndAdd",
                    int.class,
                    constantType(callSiteBinder, keyField.type()).invoke(
                            "getFlatVariableWidthLength",
                            int.class,
                            fixedChunk,
                            fieldFixedOffset));
        }
        else {
            variableWidthOffset = constantInt(0);
        }
        return new BytecodeBlock()
                .append(new IfStatement()
                        .condition(notEqual(fixedChunk.getElement(fieldIsNullOffset).cast(int.class), constantInt(0)))
                        .ifTrue(hash.set(constantLong(NULL_HASH_CODE)))
                        .ifFalse(hash.set(invoke(
                                callSiteBinder.bind(keyField.hashFlatMethod()),
                                "hash",
                                fixedChunk,
                                fieldFixedOffset,
                                variableChunk,
                                variableWidthOffset))))
                .append(result.set(invokeStatic(CombineHashFunction.class, "getHash", long.class, result, hash)));
    }

    @UsedByGeneratedCode
    public static final class MutableVariableWidthOffset
    {
        private int offset;

        public MutableVariableWidthOffset(int offset)
        {
            this.offset = offset;
        }

        public int getAndAdd(int length)
        {
            int offset = this.offset;
            this.offset += length;
            return offset;
        }
    }

    @UsedByGeneratedCode
    public static int checkVariableWidthOffsetArgument(int variableWidthOffset)
    {
        if (variableWidthOffset < 0) {
            throw new IllegalStateException("variableWidthOffset must be >= 0, found: " + variableWidthOffset);
        }
        return variableWidthOffset;
    }

    /**
     * Consecutive fields with the same type, consecutive channels, and uniform offset
     * strides. Long runs are generated as loops over incremented offsets, so the chunk
     * bytecode scales with the number of runs instead of the number of fields.
     */
    private record FieldRun(List<KeyField> fields)
    {
        KeyField first()
        {
            return fields.getFirst();
        }

        int size()
        {
            return fields.size();
        }

        int fixedOffsetStride()
        {
            return fields.size() == 1 ? 0 : fields.get(1).fieldFixedOffset() - fields.getFirst().fieldFixedOffset();
        }

        int isNullOffsetDelta()
        {
            return first().fieldIsNullOffset() - first().fieldFixedOffset();
        }
    }

    // shorter runs are emitted field by field with constant offsets
    private static final int LOOP_THRESHOLD = 4;

    private static List<FieldRun> partitionIntoRuns(List<KeyField> keyFields)
    {
        List<FieldRun> runs = new ArrayList<>();
        int start = 0;
        while (start < keyFields.size()) {
            int end = start + 1;
            while (end < keyFields.size() && extendsRun(keyFields, start, end)) {
                end++;
            }
            runs.add(new FieldRun(keyFields.subList(start, end)));
            start = end;
        }
        return runs;
    }

    private static boolean extendsRun(List<KeyField> keyFields, int start, int candidate)
    {
        KeyField first = keyFields.get(start);
        KeyField previous = keyFields.get(candidate - 1);
        KeyField field = keyFields.get(candidate);
        if (!field.type().equals(first.type()) || field.index() != previous.index() + 1) {
            return false;
        }
        // both offsets must advance by the stride established by the first pair
        int stride = candidate - start == 1
                ? field.fieldFixedOffset() - first.fieldFixedOffset()
                : keyFields.get(start + 1).fieldFixedOffset() - first.fieldFixedOffset();
        return field.fieldFixedOffset() - previous.fieldFixedOffset() == stride &&
                field.fieldIsNullOffset() - previous.fieldIsNullOffset() == stride &&
                field.fieldIsNullOffset() - field.fieldFixedOffset() == first.fieldIsNullOffset() - first.fieldFixedOffset();
    }

    private record KeyField(
            int index,
            Type type,
            int fieldIsNullOffset,
            int fieldFixedOffset,
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
            MethodDefinition hashBlockChunk,
            MethodDefinition hashFlatChunk) {}

    private record ChunkBindings(
            Binding getTotalVariableWidth,
            Binding readFlatChunk,
            Binding writeFlatChunk,
            Binding identicalMethodChunk,
            Binding hashBlockChunk,
            Binding hashFlatChunk) {}

    private static Binding bindChunkMethod(CallSiteBinder callSiteBinder, Class<?> chunkClass, MethodDefinition method)
    {
        Method chunkMethod = Arrays.stream(chunkClass.getMethods())
                .filter(candidate -> Modifier.isStatic(candidate.getModifiers()))
                .filter(candidate -> candidate.getName().equals(method.getName()))
                .filter(candidate -> candidate.getParameterCount() == method.getParameterTypes().size())
                .collect(onlyElement());
        try {
            return callSiteBinder.bind(MethodHandles.lookup().unreflect(chunkMethod));
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
