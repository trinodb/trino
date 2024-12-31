package io.trino.arrow.writer;

import io.trino.arrow.ArrowColumnWriter;
import io.trino.arrow.ArrowWriters;

import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.StructVector;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class StructColumnWriter implements ArrowColumnWriter
{
    private final RowType type;
    private final StructVector vector;

    public StructColumnWriter(StructVector vector, Type t)
    {
        this.type = (RowType) t;
        this.vector = vector;

    }

    @Override
    public void write(Block block)
    {
        vector.setInitialCapacity(block.getPositionCount());
        vector.allocateNew();
        List<Block> fields = RowBlock.getRowFieldsFromBlock(block);
        List<FieldVector> children = vector.getChildrenFromFields();
        for (int i = 0; i < children.size(); i++) {
            Type childType = type.getFields().get(i).getType();
            Block childBlock = fields.get(i);
            ArrowColumnWriter columnWriter = ArrowWriters.createWriter(children.get(i), childType);
            columnWriter.write(childBlock);
        }
    }
}
