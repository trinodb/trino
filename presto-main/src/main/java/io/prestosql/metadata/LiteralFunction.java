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
package io.prestosql.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;
import io.prestosql.operator.scalar.ScalarFunctionImplementation;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.VarcharType;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.block.BlockSerdeUtil.READ_BLOCK;
import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;

public class LiteralFunction
        extends SqlScalarFunction
{
    static final String LITERAL_FUNCTION_NAME = "$literal$";
    private static final Set<Class<?>> SUPPORTED_LITERAL_TYPES = ImmutableSet.of(long.class, double.class, Slice.class, boolean.class);

    public LiteralFunction()
    {
        super(new Signature(LITERAL_FUNCTION_NAME, SCALAR, new TypeSignature("R"), new TypeSignature("T")));
    }

    @Override
    public boolean isHidden()
    {
        return true;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "literal";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, Metadata metadata)
    {
        Type parameterType = boundVariables.getTypeVariable("T");
        Type type = boundVariables.getTypeVariable("R");

        MethodHandle methodHandle = null;
        if (parameterType.getJavaType() == type.getJavaType()) {
            methodHandle = MethodHandles.identity(parameterType.getJavaType());
        }

        if (parameterType.getJavaType() == Slice.class) {
            if (type.getJavaType() == Block.class) {
                methodHandle = READ_BLOCK.bindTo(metadata.getBlockEncodingSerde());
            }
        }

        checkArgument(methodHandle != null,
                "Expected type %s to use (or can be converted into) Java type %s, but Java type is %s",
                type,
                parameterType.getJavaType(),
                type.getJavaType());

        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle,
                isDeterministic());
    }

    public static boolean isSupportedLiteralType(Type type)
    {
        return SUPPORTED_LITERAL_TYPES.contains(type.getJavaType());
    }

    public static Signature getLiteralFunctionSignature(Type type)
    {
        TypeSignature argumentType = typeForLiteralFunctionArgument(type).getTypeSignature();

        return new Signature(
                LITERAL_FUNCTION_NAME + type.getTypeId().getId(),
                SCALAR,
                type.getTypeSignature(),
                argumentType);
    }

    public static Type typeForLiteralFunctionArgument(Type type)
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
            else {
                return VARBINARY;
            }
        }
        if (clazz == boolean.class) {
            return BOOLEAN;
        }
        throw new IllegalArgumentException("Unhandled Java type: " + clazz.getName());
    }
}
