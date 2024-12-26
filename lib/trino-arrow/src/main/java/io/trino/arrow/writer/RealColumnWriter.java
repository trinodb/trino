package io.trino.arrow.writer;

import io.trino.spi.block.Block;
import io.trino.spi.type.RealType;
import org.apache.arrow.vector.Float4Vector;

public class RealColumnWriter extends PrimitiveColumnWriter<Float4Vector>
{
    private final RealType type = RealType.REAL;

    public RealColumnWriter(Float4Vector vector)
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
        vector.set(position, type.getFloat(block, position));
    }
}
