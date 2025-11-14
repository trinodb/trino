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
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.ForLoop;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.trino.operator.scalar.CombineHashFunction;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import io.trino.sql.gen.CallSiteBinder;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.add;
import static io.airlift.bytecode.expression.BytecodeExpressions.and;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantLong;
import static io.airlift.bytecode.expression.BytecodeExpressions.equal;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.lessThan;
import static io.trino.operator.HashGenerator.INITIAL_HASH_VALUE;
import static io.trino.spi.type.TypeUtils.NULL_HASH_CODE;
import static io.trino.sql.gen.Bootstrap.BOOTSTRAP_METHOD;

public final class HashStrategyCompilerUtils
{
    private HashStrategyCompilerUtils() {}

    public static void generateHashBlock(ClassDefinition definition, List<ChunkClass> chunkClasses)
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

    public static MethodDefinition generateHashBlockChunk(ClassDefinition definition, List<HashGeneratorKeyField> keyFields, CallSiteBinder callSiteBinder)
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

        for (HashGeneratorKeyField keyField : keyFields) {
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

    public static MethodDefinition generateHashBlocksBatchedChunk(ClassDefinition definition, List<HashGeneratorKeyField> keyFields, CallSiteBinder callSiteBinder)
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
        for (HashGeneratorKeyField keyField : keyFields) {
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
            nonEmptyLength.append(invokeStatic(method, blocks.getElement(keyField.hashChannelIndex()), hashes, offset, length));
        }

        body.append(new IfStatement("if (length != 0)")
                        .condition(equal(length, constantInt(0)))
                        .ifFalse(nonEmptyLength))
                .ret();

        return methodDefinition;
    }

    private static MethodDefinition generateHashBlockVectorized(ClassDefinition definition, HashGeneratorKeyField field, CallSiteBinder callSiteBinder)
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

    public record HashGeneratorKeyField(
            int index,
            Type type,
            MethodHandle hashBlockMethod,
            int hashChannelIndex) {}

    public record ChunkClass(
            ClassDefinition definition,
            MethodDefinition getTotalVariableWidth,
            MethodDefinition readFlatChunk,
            MethodDefinition writeFlatChunk,
            MethodDefinition identicalMethodChunk,
            MethodDefinition hashBlockChunk,
            MethodDefinition hashFlatChunk,
            MethodDefinition hashBlocksBatchedChunk) {}
}
