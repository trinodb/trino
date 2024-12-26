package io.trino.arrow.writer;

import io.trino.spi.block.Block;
import io.trino.spi.type.DecimalType;
import org.apache.arrow.vector.DecimalVector;

public class DecimalColumnWriter extends PrimitiveColumnWriter<DecimalVector>
{
    private final DecimalType type;

    public DecimalColumnWriter(DecimalVector vector, DecimalType type)
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
        vector.set(position, type.getLong(block, position));
    }
}
