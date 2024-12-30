package io.trino.arrow.writer;

import io.trino.spi.block.Block;
import io.trino.spi.type.BooleanType;
import org.apache.arrow.vector.BitVector;

public class BooleanColumnWriter extends FixedWidthColumnWriter<BitVector>
{
    private final BooleanType type = BooleanType.BOOLEAN;

    public BooleanColumnWriter(BitVector vector) {
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
        vector.set(position, type.getBoolean(block, position) ? 1 : 0);
    }
}
