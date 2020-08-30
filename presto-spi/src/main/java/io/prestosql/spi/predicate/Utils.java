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
package io.prestosql.spi.predicate;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeOperators;

import static io.prestosql.spi.type.TypeUtils.readNativeValue;
import static io.prestosql.spi.type.TypeUtils.writeNativeValue;
import static java.lang.String.format;

public final class Utils
{
    private Utils() {}

    // Tuple domain accesses equal and hash code operators from static contexts which
    // are too numerous to inject a type operator cache. Instead, we uses a static cache
    // just for this use case.
    static final TypeOperators TUPLE_DOMAIN_TYPE_OPERATORS = new TypeOperators();

    public static Block nativeValueToBlock(Type type, Object object)
    {
        if (object != null && !Primitives.wrap(type.getJavaType()).isInstance(object)) {
            throw new IllegalArgumentException(format("Object '%s' does not match type %s", object, type.getJavaType()));
        }
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        writeNativeValue(type, blockBuilder, object);
        return blockBuilder.build();
    }

    static Object blockToNativeValue(Type type, Block block)
    {
        return readNativeValue(type, block, 0);
    }

    static RuntimeException handleThrowable(Throwable throwable)
    {
        if (throwable instanceof Error) {
            throw (Error) throwable;
        }
        if (throwable instanceof RuntimeException) {
            throw (RuntimeException) throwable;
        }
        return new RuntimeException(throwable);
    }
}
