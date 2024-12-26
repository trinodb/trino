package io.trino.arrow.writer;

import io.trino.arrow.ArrowColumnWriter;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import org.apache.arrow.vector.complex.ListVector;

import static java.util.Objects.requireNonNull;

public class ArrayColumnWriter implements ArrowColumnWriter
{
    private final ListVector vector;
    private final ArrowColumnWriter elementWriter;

    public ArrayColumnWriter(ListVector vector, ArrowColumnWriter elementWriter)
    {
        this.vector = requireNonNull(vector, "vector is null");
        this.elementWriter = requireNonNull(elementWriter, "elementWriter is null");
    }

    @Override
    public void write(Block block)
    {
        if (block instanceof ArrayBlock arrayBlock) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (block.isNull(position)) {
                    vector.setNull(position);
                } else {
                    Block elementBlock = arrayBlock.getArray(position);
                    vector.startNewValue(position);
                    elementWriter.write(elementBlock);
                    vector.endValue(position, elementBlock.getPositionCount());
                }
            }
            vector.setValueCount(block.getPositionCount());
        }else{
            throw new UnsupportedOperationException("ArrayBlock is expected");
        }
    }

}
