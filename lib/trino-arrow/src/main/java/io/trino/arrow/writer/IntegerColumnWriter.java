package io.trino.arrow.writer;

import io.trino.spi.block.Block;
import io.trino.spi.type.IntegerType;
import org.apache.arrow.vector.IntVector;

public class IntegerColumnWriter extends FixedWidthColumnWriter<IntVector>
{
    private final IntegerType type = IntegerType.INTEGER;

    public IntegerColumnWriter(IntVector vector) {
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
        vector.set(position, type.getInt(block, position));
    }
}
