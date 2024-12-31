package io.trino.arrow.writer;

import io.trino.spi.block.Block;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.VarCharVector;

public class VarcharColumnWriter extends VariableWidthColumnWriter<VarCharVector>
{
    private final VarcharType type = VarcharType.VARCHAR;

    public VarcharColumnWriter(VarCharVector vector) {
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
        vector.setSafe(position, type.getSlice(block, position).getBytes());
    }
}
