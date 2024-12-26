package io.trino.arrow.writer;

import io.trino.spi.block.Block;
import io.trino.spi.type.DoubleType;
import org.apache.arrow.vector.Float8Vector;

public class DoubleColumnWriter extends PrimitiveColumnWriter<Float8Vector>
{
    private final DoubleType type = DoubleType.DOUBLE;

    public DoubleColumnWriter(Float8Vector vector)
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
        vector.set(position, type.getDouble(block, position));
    }
}
