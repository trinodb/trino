package io.trino.arrow.writer;

import io.trino.spi.block.Block;
import io.trino.spi.type.VarbinaryType;
import org.apache.arrow.vector.VarBinaryVector;

public class VarbinaryColumnWriter extends PrimitiveColumnWriter<VarBinaryVector>
{
    private final VarbinaryType type = VarbinaryType.VARBINARY;

    public VarbinaryColumnWriter(VarBinaryVector vector)
    {
        super(vector);
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
