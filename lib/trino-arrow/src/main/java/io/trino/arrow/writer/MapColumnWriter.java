package io.trino.arrow.writer;

import io.trino.arrow.ArrowColumnWriter;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.MapBlock;
import org.apache.arrow.vector.complex.MapVector;

import static java.util.Objects.requireNonNull;

public class MapColumnWriter implements ArrowColumnWriter
{
    private final MapVector vector;
    private final ArrowColumnWriter keyWriter;
    private final ArrowColumnWriter valueWriter;

    public MapColumnWriter(MapVector vector, ArrowColumnWriter keyWriter, ArrowColumnWriter valueWriter)
    {
        this.vector = requireNonNull(vector, "vector is null");
        this.keyWriter = requireNonNull(keyWriter, "keyWriter is null");
        this.valueWriter = requireNonNull(valueWriter, "valueWriter is null");
    }

    @Override
    public void write(Block block)
    {
        ColumnarMap mapBlock = ColumnarMap.toColumnarMap(block);
        //ColumnarMap mapBlock = ColumnarMap.toColumnarMap(mb);
        Block keyBlock = mapBlock.getKeysBlock();
        Block valueBlock = mapBlock.getValuesBlock();
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                vector.setNull(position);
            }
            else {
                vector.startNewValue(position);
                int entries = mapBlock.getEntryCount(position);
                int offset = mapBlock.getOffset(position);
                Block keyRegion = keyBlock.getRegion(offset, entries);
                keyWriter.write(keyRegion);
                Block valueRegion = valueBlock.getRegion(offset, entries);
                valueWriter.write(valueRegion);
                vector.endValue(position, entries);
            }
        }
        vector.setValueCount(block.getPositionCount());


    }
}
