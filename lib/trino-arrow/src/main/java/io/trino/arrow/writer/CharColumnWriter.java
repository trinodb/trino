package io.trino.arrow.writer;

import io.trino.spi.block.Block;
import io.trino.spi.type.CharType;
import org.apache.arrow.vector.VarCharVector;

public class CharColumnWriter extends VariableWidthColumnWriter<VarCharVector>
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
        vector.setSafe(position, type.getSlice(block, position).getBytes());
    }
}
