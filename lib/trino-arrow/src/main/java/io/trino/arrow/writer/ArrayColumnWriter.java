package io.trino.arrow.writer;

import io.trino.arrow.ArrowColumnWriter;
import io.trino.arrow.ArrowWriters;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.ListVector;

import static java.util.Objects.requireNonNull;

public class ArrayColumnWriter implements ArrowColumnWriter
{
    private final ListVector vector;
    private final ArrayType type;

    public ArrayColumnWriter(ListVector vector, ArrayType type)
    {
        this.vector = requireNonNull(vector, "vector is null");
        this.type = type;

    }

    @Override
    public void write(Block block)
    {
        vector.setInitialCapacity(block.getPositionCount());
        vector.allocateNew();
        if (block instanceof ArrayBlock arrayBlock) {
            Block dataBlock = arrayBlock.getElementsBlock();
            ArrowColumnWriter elementWriter = ArrowWriters.createWriter(vector.getDataVector(), type.getElementType());
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (block.isNull(position)) {
                    vector.setNull(position);
                } else {
                    Block elementBlock = arrayBlock.getArray(position);
                    vector.startNewValue(position);
                    vector.endValue(position, elementBlock.getPositionCount());
                }

            }
            elementWriter.write(dataBlock);
            vector.setValueCount(block.getPositionCount());
        }else{
            throw new UnsupportedOperationException("ArrayBlock is expected");
        }
    }
}
