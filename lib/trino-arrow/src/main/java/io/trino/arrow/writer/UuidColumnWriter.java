package io.trino.arrow.writer;

import io.trino.spi.block.Block;
import io.trino.spi.type.UuidType;
import org.apache.arrow.vector.FixedSizeBinaryVector;

public class UuidColumnWriter extends FixedWidthColumnWriter<FixedSizeBinaryVector>
{
    private final UuidType type = UuidType.UUID;

    public UuidColumnWriter(FixedSizeBinaryVector vector) {
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
