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
package io.trino.plugin.geospatial;

import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.IsNull;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.type.AbstractVariableWidthType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TypeOperatorDeclaration;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignature;

import static io.trino.geospatial.serde.GeometrySerde.deserialize;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.IDENTICAL;
import static io.trino.spi.function.OperatorType.XX_HASH_64;

public class GeometryType
        extends AbstractVariableWidthType
{
    public static final GeometryType GEOMETRY = new GeometryType();

    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION =
            TypeOperatorDeclaration.builder(Slice.class)
                    .addOperators(DEFAULT_READ_OPERATORS)
                    .addOperators(DEFAULT_COMPARABLE_OPERATORS)
                    .build();

    private GeometryType()
    {
        super(new TypeSignature(StandardTypes.GEOMETRY), Slice.class);
    }

    protected GeometryType(TypeSignature signature)
    {
        super(signature, Slice.class);
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        return TYPE_OPERATOR_DECLARATION;
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        VariableWidthBlock valueBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();
        int valuePosition = block.getUnderlyingValuePosition(position);
        return valueBlock.getSlice(valuePosition);
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        if (value == null) {
            blockBuilder.appendNull();
            return;
        }
        writeSlice(blockBuilder, value, 0, value.length());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        if (value == null) {
            blockBuilder.appendNull();
            return;
        }
        ((VariableWidthBlockBuilder) blockBuilder).writeEntry(value, offset, length);
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        try {
            return deserialize(getSlice(block, position)).asText();
        }
        catch (Exception e) {
            return "<invalid geometry>";
        }
    }

    @ScalarOperator(HASH_CODE)
    private static long hashCodeOperator(Slice value)
    {
        return value.hashCode();
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(Slice value)
    {
        return XxHash64.hash(value);
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(Slice left, Slice right)
    {
        return left.equals(right);
    }

    @ScalarOperator(IDENTICAL)
    private static boolean identical(Slice left, @IsNull boolean leftNull, Slice right, @IsNull boolean rightNull)
    {
        if (leftNull || rightNull) {
            return leftNull == rightNull;
        }
        return left.equals(right);
    }
}
