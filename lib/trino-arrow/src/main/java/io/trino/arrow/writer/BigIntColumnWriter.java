package io.trino.arrow.writer;

import io.trino.spi.block.Block;
import io.trino.spi.type.BigintType;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.types.Types;

public class BigIntColumnWriter extends FixedWidthColumnWriter<BigIntVector>
{
    private final BigintType type = BigintType.BIGINT;

    public BigIntColumnWriter(BigIntVector vector) {
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
        vector.set(position, type.getLong(block, position));
    }
}
