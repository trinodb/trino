package io.trino.arrow.writer;

import io.trino.spi.block.Block;
import io.trino.spi.type.CharType;
import org.apache.arrow.vector.VarCharVector;

public class CharColumnWriter extends PrimitiveColumnWriter<VarCharVector>
{
    private final CharType type;

    public CharColumnWriter(VarCharVector vector, CharType type)
    {
        super(vector);
        this.type = type;
    }

    @Override
    protected void writeNull(int position)
    {
        vector.setNull(position);
    }

    @Override
    protected void writeValue(Block block, int position)
    {
        vector.set(position, type.getSlice(block, position).getBytes());
    }
}
