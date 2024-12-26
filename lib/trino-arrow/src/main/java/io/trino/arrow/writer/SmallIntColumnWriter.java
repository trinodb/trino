package io.trino.arrow.writer;

import io.trino.spi.block.Block;
import io.trino.spi.type.SmallintType;
import org.apache.arrow.vector.SmallIntVector;

public class SmallIntColumnWriter extends PrimitiveColumnWriter<SmallIntVector>
{
    private final SmallintType type = SmallintType.SMALLINT;

    public SmallIntColumnWriter(SmallIntVector vector)
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
        vector.set(position, type.getShort(block, position));
    }
}
