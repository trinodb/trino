package io.trino.arrow.writer;

import io.trino.arrow.ArrowColumnWriter;
import io.trino.arrow.ArrowWriters;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.MapBlock;
import io.trino.spi.type.MapType;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;

import static java.util.Objects.requireNonNull;

public class MapColumnWriter implements ArrowColumnWriter
{
    private final MapVector vector;
    private final ArrowColumnWriter keyWriter;
    private final ArrowColumnWriter valueWriter;
    private final MapType type;

    public MapColumnWriter(MapVector vector, MapType type)
    {
        this.vector = requireNonNull(vector, "vector is null");
        this.type = requireNonNull(type, "type is null");
        if(vector.getDataVector() instanceof StructVector structVector){
            this.keyWriter = ArrowWriters.createWriter(structVector.getChildByOrdinal(0), type.getKeyType());
            this.valueWriter = ArrowWriters.createWriter(structVector.getChildByOrdinal(1), type.getValueType());
        }else{
            throw new UnsupportedOperationException("Unsupported data vector : " + vector.getDataVector().getClass());
        }
    }



    @Override
    public void write(Block block)
    {

        ColumnarMap mapBlock = ColumnarMap.toColumnarMap(block);
        //ColumnarMap mapBlock = ColumnarMap.toColumnarMap(mb);

        Block keyBlock = mapBlock.getKeysBlock();
        Block valueBlock = mapBlock.getValuesBlock();
        vector.setInitialTotalCapacity(mapBlock.getPositionCount(), mapBlock.getValuesBlock().getPositionCount());
        vector.allocateNew();
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                vector.setNull(position);
            }
            else {
                vector.startNewValue(position);
                int entries = mapBlock.getEntryCount(position);
                vector.endValue(position, entries);
            }
        }
        keyWriter.write(keyBlock);
        valueWriter.write(valueBlock);
        vector.setValueCount(block.getPositionCount());
    }
}
