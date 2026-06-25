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
package io.trino.spi.type;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.ValueBlock;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class FunctionType
        implements Type
{
    public static final String NAME = "function";

    private final TypeDescriptor signature;
    private final Type returnType;
    private final List<Type> argumentTypes;

    public FunctionType(List<Type> argumentTypes, Type returnType)
    {
        this.signature = new TypeDescriptor(NAME, typeParameters(argumentTypes, returnType));
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.argumentTypes = List.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
    }

    private static List<TypeParameter> typeParameters(List<Type> argumentTypes, Type returnType)
    {
        requireNonNull(returnType, "returnType is null");
        requireNonNull(argumentTypes, "argumentTypes is null");
        List<TypeParameter> typeParameters = new ArrayList<>(argumentTypes.size() + 1);
        argumentTypes.stream()
                .map(Type::getTypeDescriptor)
                .map(TypeParameter::typeParameter)
                .forEach(typeParameters::add);
        typeParameters.add(TypeParameter.typeParameter(returnType.getTypeDescriptor()));
        return List.copyOf(typeParameters);
    }

    public Type getReturnType()
    {
        return returnType;
    }

    public List<Type> getArgumentTypes()
    {
        return argumentTypes;
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return Stream.concat(argumentTypes.stream(), Stream.of(returnType)).toList();
    }

    @Override
    public TypeDescriptor getTypeDescriptor()
    {
        return signature;
    }

    @Override
    public String getDisplayName()
    {
        return Stream.concat(argumentTypes.stream(), Stream.of(returnType))
                .map(Type::getDisplayName)
                .collect(joining(",", "function(", ")"));
    }

    @Override
    public Class<?> getJavaType()
    {
        throw new UnsupportedOperationException(getDisplayName() + " type does not have a Java type");
    }

    @Override
    public Class<? extends ValueBlock> getValueBlockType()
    {
        throw new UnsupportedOperationException(getDisplayName() + " type does not have a ValueBlock type");
    }

    @Override
    public boolean isComparable()
    {
        return false;
    }

    @Override
    public boolean isOrderable()
    {
        return false;
    }

    @Override
    public boolean getBoolean(Block block, int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void writeBoolean(BlockBuilder blockBuilder, boolean value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public long getLong(Block block, int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public double getDouble(Block block, int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void writeDouble(BlockBuilder blockBuilder, double value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public Object getObject(Block block, int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public Object getObjectValue(Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getFlatFixedSize()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isFlatVariableWidth()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getFlatVariableWidthSize(Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getFlatVariableWidthLength(byte[] fixedSizeSlice, int fixedSizeOffset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FunctionType that = (FunctionType) o;
        return Objects.equals(returnType, that.returnType) && Objects.equals(argumentTypes, that.argumentTypes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(returnType, argumentTypes);
    }

    @Override
    public String toString()
    {
        return argumentTypes.stream()
                .map(Type::toString)
                .collect(joining(", ", "function(", ") -> " + returnType));
    }
}
