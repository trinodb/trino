package io.trino.arrow.writer;

import io.trino.spi.block.Block;
import io.trino.spi.type.TinyintType;
import org.apache.arrow.vector.TinyIntVector;

public class TinyIntColumnWriter extends PrimitiveColumnWriter<TinyIntVector>
{
    private final TinyintType type = TinyintType.TINYINT;

    public TinyIntColumnWriter(TinyIntVector vector)
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
        vector.set(position, type.getByte(block, position));
    }
}
