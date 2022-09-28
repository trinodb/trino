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
package io.trino.parquet;

import io.trino.spi.block.Block;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.StandardTypes.ARRAY;
import static io.trino.spi.type.StandardTypes.MAP;
import static io.trino.spi.type.StandardTypes.ROW;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

/**
 * Based on io.trino.rcfile.ValidationHash and io.trino.orc.ValidationHash
 * with minor differences in handling of timestamp and map types.
 */
class ValidationHash
{
    // This value is a large arbitrary prime
    private static final long NULL_HASH_CODE = 0x6e3efbd56c16a0cbL;

    private static final MethodHandle MAP_HASH;
    private static final MethodHandle ARRAY_HASH;
    private static final MethodHandle ROW_HASH;

    static {
        try {
            MAP_HASH = lookup().findStatic(
                    ValidationHash.class,
                    "mapHash",
                    MethodType.methodType(long.class, Type.class, ValidationHash.class, ValidationHash.class, Block.class, int.class));
            ARRAY_HASH = lookup().findStatic(
                    ValidationHash.class,
                    "arrayHash",
                    MethodType.methodType(long.class, Type.class, ValidationHash.class, Block.class, int.class));
            ROW_HASH = lookup().findStatic(
                    ValidationHash.class,
                    "rowHash",
                    MethodType.methodType(long.class, Type.class, ValidationHash[].class, Block.class, int.class));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // This should really come from the environment, but there is not good way to get a value here
    private static final TypeOperators VALIDATION_TYPE_OPERATORS_CACHE = new TypeOperators();

    public static ValidationHash createValidationHash(Type type)
    {
        requireNonNull(type, "type is null");
        if (type.getTypeSignature().getBase().equals(MAP)) {
            ValidationHash keyHash = createValidationHash(type.getTypeParameters().get(0));
            ValidationHash valueHash = createValidationHash(type.getTypeParameters().get(1));
            return new ValidationHash(MAP_HASH.bindTo(type).bindTo(keyHash).bindTo(valueHash));
        }

        if (type.getTypeSignature().getBase().equals(ARRAY)) {
            ValidationHash elementHash = createValidationHash(type.getTypeParameters().get(0));
            return new ValidationHash(ARRAY_HASH.bindTo(type).bindTo(elementHash));
        }

        if (type.getTypeSignature().getBase().equals(ROW)) {
            ValidationHash[] fieldHashes = type.getTypeParameters().stream()
                    .map(ValidationHash::createValidationHash)
                    .toArray(ValidationHash[]::new);
            return new ValidationHash(ROW_HASH.bindTo(type).bindTo(fieldHashes));
        }

        return new ValidationHash(VALIDATION_TYPE_OPERATORS_CACHE.getHashCodeOperator(type, InvocationConvention.simpleConvention(FAIL_ON_NULL, BLOCK_POSITION)));
    }

    private final MethodHandle hashCodeOperator;

    private ValidationHash(MethodHandle hashCodeOperator)
    {
        this.hashCodeOperator = requireNonNull(hashCodeOperator, "hashCodeOperator is null");
    }

    public long hash(Block block, int position)
    {
        if (block.isNull(position)) {
            return NULL_HASH_CODE;
        }
        try {
            return (long) hashCodeOperator.invokeExact(block, position);
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    private static long mapHash(Type type, ValidationHash keyHash, ValidationHash valueHash, Block block, int position)
    {
        Block mapBlock = (Block) type.getObject(block, position);
        long hash = 0;
        for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
            hash = 31 * hash + keyHash.hash(mapBlock, i);
            hash = 31 * hash + valueHash.hash(mapBlock, i + 1);
        }
        return hash;
    }

    private static long arrayHash(Type type, ValidationHash elementHash, Block block, int position)
    {
        Block array = (Block) type.getObject(block, position);
        long hash = 0;
        for (int i = 0; i < array.getPositionCount(); i++) {
            hash = 31 * hash + elementHash.hash(array, i);
        }
        return hash;
    }

    private static long rowHash(Type type, ValidationHash[] fieldHashes, Block block, int position)
    {
        Block row = (Block) type.getObject(block, position);
        long hash = 0;
        for (int i = 0; i < row.getPositionCount(); i++) {
            hash = 31 * hash + fieldHashes[i].hash(row, i);
        }
        return hash;
    }
}
