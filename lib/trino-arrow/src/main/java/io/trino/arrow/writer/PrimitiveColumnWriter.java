package io.trino.arrow.writer;

import io.trino.arrow.ArrowColumnWriter;
import io.trino.spi.block.Block;
import org.apache.arrow.vector.ValueVector;

public abstract class PrimitiveColumnWriter<V extends ValueVector> implements ArrowColumnWriter
{
    protected final V vector;

    protected PrimitiveColumnWriter(V vector)
    {
        this.vector = vector;
    }

    @Override
    public void write(Block block)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                writeNull( position);
            }
            else {
                writeValue(block, position);
            }
        }
        vector.setValueCount(block.getPositionCount());
    }

    protected abstract void writeNull(int position);
    protected abstract void writeValue(Block block, int position);

}
