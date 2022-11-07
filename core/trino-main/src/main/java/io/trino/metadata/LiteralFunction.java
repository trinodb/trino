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
package io.trino.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;
import io.trino.operator.scalar.ChoicesSpecializedSqlScalarFunction;
import io.trino.operator.scalar.SpecializedSqlScalarFunction;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.block.BlockSerdeUtil.READ_BLOCK;
import static io.trino.block.BlockSerdeUtil.READ_BLOCK_VALUE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.util.Objects.requireNonNull;

public class LiteralFunction
        extends SqlScalarFunction
{
    public static final String LITERAL_FUNCTION_NAME = "$literal$";

    private final BlockEncodingSerde blockEncodingSerde;

    public LiteralFunction(BlockEncodingSerde blockEncodingSerde)
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .name(LITERAL_FUNCTION_NAME)
                        .typeVariable("F")
                        .typeVariable("T")
                        .returnType(new TypeSignature("T"))
                        .argumentType(new TypeSignature("F"))
                        .build())
                .hidden()
                .description("literal")
                .build());
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        Type parameterType = boundSignature.getArgumentTypes().get(0);
        Type type = boundSignature.getReturnType();

        MethodHandle methodHandle = null;
        if (parameterType.getJavaType() == type.getJavaType()) {
            methodHandle = MethodHandles.identity(parameterType.getJavaType());
        }

        if (parameterType.getJavaType() == Slice.class) {
            if (type.getJavaType() == Block.class) {
                methodHandle = READ_BLOCK.bindTo(blockEncodingSerde);
            }
            else if (type.getJavaType() != Slice.class) {
                methodHandle = READ_BLOCK_VALUE.bindTo(blockEncodingSerde).bindTo(type);
            }
        }

        checkArgument(
                methodHandle != null,
                "Expected type %s to use (or can be converted into) Java type %s, but Java type is %s",
                type,
                parameterType.getJavaType(),
                type.getJavaType());

        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL),
                methodHandle);
    }

    public static Type typeForMagicLiteral(Type type)
    {
        Class<?> clazz = type.getJavaType();
        clazz = Primitives.unwrap(clazz);

        if (clazz == long.class) {
            return BIGINT;
        }
        if (clazz == double.class) {
            return DOUBLE;
        }
        if (!clazz.isPrimitive()) {
            if (type instanceof VarcharType) {
                return type;
            }
            return VARBINARY;
        }
        if (clazz == boolean.class) {
            return BOOLEAN;
        }
        throw new IllegalArgumentException("Unhandled Java type: " + clazz.getName());
    }
}
