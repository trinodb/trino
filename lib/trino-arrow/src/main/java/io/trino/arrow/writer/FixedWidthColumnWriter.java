package io.trino.arrow.writer;

import io.trino.spi.block.Block;
import org.apache.arrow.vector.FixedWidthVector;

public abstract class FixedWidthColumnWriter<V extends FixedWidthVector> extends PrimitiveColumnWriter<V> {

    protected FixedWidthColumnWriter(V vector) {
        super(vector);
    }

    @Override
    protected void initialize(Block block) {
        vector.allocateNew(block.getPositionCount());
    }
}
