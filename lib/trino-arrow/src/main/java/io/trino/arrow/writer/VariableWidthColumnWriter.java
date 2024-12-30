package io.trino.arrow.writer;

import io.trino.spi.block.Block;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.VariableWidthVector;

public abstract class VariableWidthColumnWriter<V extends VariableWidthVector> extends PrimitiveColumnWriter<V> {

    protected VariableWidthColumnWriter(V vector) {
        super(vector);
    }


    @Override
    //allocate space for the vector based on the block size.
    //subclasses should use setSafe since we cant guarantee that the block size is greater than the vector size.
    protected void initialize(Block block) {
        vector.allocateNew(block.getRetainedSizeInBytes(), block.getPositionCount());
    }

}
