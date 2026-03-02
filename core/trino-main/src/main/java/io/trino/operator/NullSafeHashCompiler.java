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
import com.google.inject.Inject;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.ForLoop;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.trino.operator.scalar.CombineHashFunction;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.CallSiteBinder;
import org.assertj.core.util.VisibleForTesting;

import java.lang.invoke.MethodHandle;
import java.util.Objects;

import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.add;
import static io.airlift.bytecode.expression.BytecodeExpressions.and;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantLong;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.lessThan;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.VALUE_BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.TypeUtils.NULL_HASH_CODE;
import static io.trino.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;

public final class NullSafeHashCompiler
{
    private final LoadingCache<Type, NullSafeHash> nullSafeHashes;

    @Inject
    public NullSafeHashCompiler(TypeOperators typeOperators)
    {
        this.nullSafeHashes = buildNonEvictableCache(
                CacheBuilder.newBuilder()
                        .maximumSize(1000),
                CacheLoader.from(key -> compileNullSafeHash(key, typeOperators)));
    }

    public NullSafeHash compileHash(Type type)
    {
        return nullSafeHashes.getUnchecked(type);
    }

    @VisibleForTesting
    public static NullSafeHash compileNullSafeHash(Type type, TypeOperators typeOperators)
    {
        CallSiteBinder callSiteBinder = new CallSiteBinder();

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("NullSafeHash$" + type.getTypeSignature().toString()),
                type(Object.class),
                type(NullSafeHash.class));

        MethodDefinition constructor = definition.declareConstructor(a(PUBLIC));
        constructor.getBody()
                .append(constructor.getThis())
                .invokeConstructor(Object.class)
                .ret();

        MethodHandle hashMethod = typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
        generateHashBlock(definition, callSiteBinder, hashMethod);
        generateHashBlocksBatched("hashBatched", definition, callSiteBinder, hashMethod, false);
        generateHashBlocksBatched("hashBatchedWithCombine", definition, callSiteBinder, hashMethod, true);
        generateHashBlocksDictionary("hashBatchedDictionary", definition, callSiteBinder, hashMethod, false);
        generateHashBlocksDictionary("hashBatchedDictionaryWithCombine", definition, callSiteBinder, hashMethod, true);

        try {
            DynamicClassLoader classLoader = new DynamicClassLoader(NullSafeHashCompiler.class.getClassLoader(), callSiteBinder.getBindings());
            return defineClass(definition, NullSafeHash.class, classLoader)
                    .getConstructor()
                    .newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static void generateHashBlock(ClassDefinition definition, CallSiteBinder callSiteBinder, MethodHandle hashMethod)
    {
        Parameter block = arg("block", type(ValueBlock.class));
        Parameter position = arg("position", type(int.class));
        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC),
                "hash",
                type(long.class),
                block,
                position);
        BytecodeBlock body = methodDefinition.getBody();

        Scope scope = methodDefinition.getScope();
        Variable hash = scope.declareVariable(long.class, "hash");
        body.append(new IfStatement()
                .condition(block.invoke("isNull", boolean.class, position))
                .ifTrue(hash.set(constantLong(NULL_HASH_CODE)))
                .ifFalse(hash.set(computeHashNonNull(callSiteBinder, block, position, hashMethod))));
        body.append(hash.ret());
    }

    private static void generateHashBlocksBatched(String methodName, ClassDefinition definition, CallSiteBinder callSiteBinder, MethodHandle hashMethod, boolean combineHash)
    {
        Parameter block = arg("block", type(ValueBlock.class));
        Parameter hashes = arg("hashes", type(long[].class));
        Parameter offset = arg("offset", type(int.class));
        Parameter length = arg("length", type(int.class));

        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC),
                methodName,
                type(void.class),
                block,
                hashes,
                offset,
                length);

        BytecodeBlock body = methodDefinition.getBody();
        Scope scope = methodDefinition.getScope();

        Variable index = scope.declareVariable(int.class, "index");
        Variable position = scope.declareVariable(int.class, "position");
        Variable mayHaveNull = scope.declareVariable(boolean.class, "mayHaveNull");
        Variable hash = scope.declareVariable(long.class, "hash");

        body.append(position.set(invokeStatic(Objects.class, "checkFromToIndex", int.class, offset, add(offset, length), block.invoke("getPositionCount", int.class))));
        body.append(invokeStatic(Objects.class, "checkFromIndexSize", int.class, constantInt(0), length, hashes.length()).pop());

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
                                        .ifFalse(hash.set(computeHashNonNull(callSiteBinder, block, position, hashMethod))))
                                .append(setHashExpression(hashes, index, hash, combineHash))
                                .append(position.increment())));

        body.append(computeHashLoop).ret();
    }

    private static void generateHashBlocksDictionary(String methodName, ClassDefinition definition, CallSiteBinder callSiteBinder, MethodHandle hashMethod, boolean combineHash)
    {
        Parameter dictionaryBlock = arg("dictionaryBlock", type(DictionaryBlock.class));
        Parameter hashes = arg("hashes", type(long[].class));
        Parameter offset = arg("offset", type(int.class));
        Parameter length = arg("length", type(int.class));

        MethodDefinition methodDefinition = definition.declareMethod(
                a(PUBLIC),
                methodName,
                type(void.class),
                dictionaryBlock,
                hashes,
                offset,
                length);

        BytecodeBlock body = methodDefinition.getBody();
        Scope scope = methodDefinition.getScope();
        Variable position = scope.declareVariable(int.class, "position");
        body.append(invokeStatic(Objects.class, "checkFromToIndex", int.class, offset, add(offset, length), dictionaryBlock.invoke("getPositionCount", int.class)));
        body.append(invokeStatic(Objects.class, "checkFromIndexSize", int.class, constantInt(0), length, hashes.length()).pop());

        Variable mayHaveNull = scope.declareVariable(boolean.class, "mayHaveNull");
        Variable hash = scope.declareVariable(long.class, "hash");
        Variable valueBlock = scope.declareVariable("valueBlock", body, dictionaryBlock.invoke("getUnderlyingValueBlock", ValueBlock.class));
        Variable index = scope.declareVariable(int.class, "index");
        Variable rawIds = scope.declareVariable("rawIds", body, dictionaryBlock.invoke("getRawIds", int[].class));
        Variable rawIdsOffset = scope.declareVariable("rawIdsOffset", body, dictionaryBlock.invoke("getRawIdsOffset", int.class));

        BytecodeBlock computeHashLoop = new BytecodeBlock()
                .append(mayHaveNull.set(valueBlock.invoke("mayHaveNull", boolean.class)))
                .append(new ForLoop("for (int index = 0; index < length; index++)")
                        .initialize(index.set(constantInt(0)))
                        .condition(lessThan(index, length))
                        .update(index.increment())
                        .body(new BytecodeBlock()
                                // position = rawIds[rawIdsOffset + offset + index]
                                .append(position.set(rawIds.getElement(add(rawIdsOffset, add(offset, index)))))
                                .append(new IfStatement("if (mayHaveNull && block.isNull(position))")
                                        .condition(and(mayHaveNull, valueBlock.invoke("isNull", boolean.class, position)))
                                        .ifTrue(hash.set(constantLong(NULL_HASH_CODE)))
                                        .ifFalse(hash.set(computeHashNonNull(callSiteBinder, valueBlock, position, hashMethod))))
                                .append(setHashExpression(hashes, index, hash, combineHash))));

        body.append(computeHashLoop).ret();
    }

    private static BytecodeExpression computeHashNonNull(CallSiteBinder callSiteBinder, Variable block, BytecodeExpression position, MethodHandle hashMethod)
    {
        return invokeDynamic(
                BOOTSTRAP_METHOD,
                ImmutableList.of(callSiteBinder.bind(hashMethod).getBindingId()),
                "hash",
                long.class,
                block,
                position);
    }

    private static BytecodeExpression setHashExpression(Parameter hashes, Variable index, Variable hash, boolean combineHash)
    {
        if (combineHash) {
            // hashes[index] = CombineHashFunction.getHash(hashes[index], hash);
            return hashes.setElement(index, invokeStatic(CombineHashFunction.class, "getHash", long.class, hashes.getElement(index), hash));
        }
        // hashes[index] = hash;
        return hashes.setElement(index, hash);
    }
}
