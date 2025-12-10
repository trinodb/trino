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
import io.airlift.bytecode.control.IfStatement;
import io.trino.cache.CacheStatsMBean;
import io.trino.operator.HashStrategyCompilerUtils.ChunkClass;
import io.trino.operator.HashStrategyCompilerUtils.HashGeneratorKeyField;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.CallSiteBinder;
import jakarta.annotation.Nullable;
import org.assertj.core.util.VisibleForTesting;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantLong;
import static io.airlift.bytecode.expression.BytecodeExpressions.equal;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.operator.HashGenerator.INITIAL_HASH_VALUE;
import static io.trino.operator.HashStrategyCompilerUtils.generateHashBlock;
import static io.trino.operator.HashStrategyCompilerUtils.generateHashBlockChunk;
import static io.trino.operator.HashStrategyCompilerUtils.generateHashBlocksBatchedChunk;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.sql.gen.BytecodeUtils.loadConstant;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;

public final class PartitionHashGeneratorCompiler
{
    @VisibleForTesting
    static final int COLUMNS_PER_CHUNK = 500;

    private final LoadingCache<CacheKey, HashGenerator> partitionHashGeneratorLoadingCache;

    @Inject
    public PartitionHashGeneratorCompiler(TypeOperators typeOperators)
    {
        this.partitionHashGeneratorLoadingCache = buildNonEvictableCache(
                CacheBuilder.newBuilder()
                        .recordStats()
                        .maximumSize(1000),
                CacheLoader.from(key -> compilePartitionHashGenerator(key.getTypes(), key.getHashChannels(), typeOperators)));
    }

    public HashGenerator getPartitionHashGenerator(List<Type> types, @Nullable int[] hashChannels)
    {
        return partitionHashGeneratorLoadingCache.getUnchecked(new CacheKey(types, hashChannels));
    }

    @Managed
    @Nested
    public CacheStatsMBean getPartitionHashGeneratorStats()
    {
        return new CacheStatsMBean(partitionHashGeneratorLoadingCache);
    }

    @VisibleForTesting
    public static HashGenerator compilePartitionHashGenerator(List<Type> types, @Nullable int[] hashChannels, TypeOperators typeOperators)
    {
        List<HashGeneratorKeyField> keyFields = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            Type type = types.get(i);
            int hashChannelIndex = hashChannels == null ? i : hashChannels[i];
            keyFields.add(new HashGeneratorKeyField(
                    i,
                    type,
                    typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL)),
                    hashChannelIndex));
        }

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        List<ChunkClass> chunkClasses = new ArrayList<>();
        int chunkNumber = 0;
        for (List<HashGeneratorKeyField> chunk : Lists.partition(keyFields, COLUMNS_PER_CHUNK)) {
            chunkClasses.add(compilePartitionHashGeneratorChunk(callSiteBinder, chunk, chunkNumber));
            chunkNumber++;
        }

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("HashGenerator"),
                type(Object.class),
                type(HashGenerator.class));

        FieldDefinition typesField = definition.declareField(a(PRIVATE, FINAL), "types", type(List.class, Type.class));
        MethodDefinition constructor = definition.declareConstructor(a(PUBLIC));
        constructor
                .getBody()
                .append(constructor.getThis())
                .invokeConstructor(Object.class)
                .append(constructor.getThis().setField(typesField, loadConstant(callSiteBinder, ImmutableList.copyOf(types), List.class)))
                .ret();

        generateHashBlock(definition, chunkClasses);
        generateHashBlocksBatched(definition, chunkClasses);
        generateHashPosition(definition, chunkClasses);

        try {
            DynamicClassLoader classLoader = new DynamicClassLoader(PartitionHashGeneratorCompiler.class.getClassLoader(), callSiteBinder.getBindings());
            for (ChunkClass chunkClass : chunkClasses) {
                defineClass(chunkClass.definition(), Object.class, classLoader);
            }
            return defineClass(definition, HashGenerator.class, classLoader)
                    .getConstructor()
                    .newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static ChunkClass compilePartitionHashGeneratorChunk(CallSiteBinder callSiteBinder, List<HashGeneratorKeyField> keyFields, int chunkNumber)
    {
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("PartitionHashGeneratorChunk$" + chunkNumber),
                type(Object.class),
                type(HashGenerator.class));

        definition.declareDefaultConstructor(a(PRIVATE));

        MethodDefinition hashBlockChunk = generateHashBlockChunk(definition, keyFields, callSiteBinder);
        MethodDefinition hashBlocksBatchedChunk = generateHashBlocksBatchedChunk(definition, keyFields, callSiteBinder);

        return new ChunkClass(
                definition,
                null,
                null,
                null,
                null,
                hashBlockChunk,
                null,
                hashBlocksBatchedChunk);
    }

    private static void generateHashBlocksBatched(ClassDefinition definition, List<ChunkClass> chunkClasses)
    {
        Parameter page = arg("page", type(Page.class));
        Parameter hashes = arg("hashes", type(long[].class));
        Parameter offset = arg("offset", type(int.class));
        Parameter length = arg("length", type(int.class));

        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC),
                "hashBlocksBatched",
                type(void.class),
                page,
                hashes,
                offset,
                length);

        Scope scope = methodDefinition.getScope();
        BytecodeBlock body = methodDefinition.getBody();
        Variable blocks = scope.declareVariable(Block[].class, "blocks");
        body.append(invokeStatic(Objects.class, "checkFromIndexSize", int.class, constantInt(0), length, hashes.length()).pop());
        body.append(blocks.set(page.invoke("getBlocks", Block[].class)));

        BytecodeBlock nonEmptyLength = new BytecodeBlock();
        for (ChunkClass chunkClass : chunkClasses) {
            nonEmptyLength.append(invokeStatic(chunkClass.hashBlocksBatchedChunk(), blocks, hashes, offset, length));
        }

        body.append(new IfStatement("if (length != 0)")
                        .condition(equal(length, constantInt(0)))
                        .ifFalse(nonEmptyLength))
                .ret();
    }

    private static void generateHashPosition(ClassDefinition definition, List<ChunkClass> chunkClasses)
    {
        Parameter position = arg("position", type(int.class));
        Parameter page = arg("page", type(Page.class));
        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC),
                "hashPosition",
                type(long.class),
                position,
                page);

        Scope scope = methodDefinition.getScope();
        BytecodeBlock body = methodDefinition.getBody();
        Variable blocks = scope.declareVariable(Block[].class, "blocks");
        Variable result = scope.declareVariable("result", body, constantLong(INITIAL_HASH_VALUE));
        body.append(blocks.set(page.invoke("getBlocks", Block[].class)));
        for (ChunkClass chunkClass : chunkClasses) {
            body.append(result.set(invokeStatic(chunkClass.hashBlockChunk(), blocks, position, result)));
        }
        body.append(result.ret());
    }

    private record CacheKey(
            List<Type> types,
            int[] hashChannels)
    {
        private CacheKey(List<Type> types, @Nullable int[] hashChannels)
        {
            this.types = ImmutableList.copyOf(types);
            this.hashChannels = hashChannels;
        }

        private List<Type> getTypes()
        {
            return types;
        }

        private int[] getHashChannels()
        {
            return hashChannels;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(types, Arrays.hashCode(hashChannels));
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            CacheKey other = (CacheKey) obj;
            return Objects.equals(this.types, other.types) &&
                    Arrays.equals(this.hashChannels, other.hashChannels);
        }
    }
}
