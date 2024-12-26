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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class StructColumnWriter implements ArrowColumnWriter
{
    private final StructVector vector;
    private final RowType type;

    public StructColumnWriter(StructVector vector, Type t)
    {
        this.type = (RowType) requireNonNull(t, "type is null");
        this.vector = requireNonNull(vector, "vector is null");
    }
    @Override
    public void write(Block block)
    {
        List<Block> fields = RowBlock.getNullSuppressedRowFieldsFromBlock(block);
        List<FieldVector> children = vector.getChildrenFromFields();
        for (int i = 0; i < children.size(); i++) {
            Type childType = type.getFields().get(i).getType();
            ArrowColumnWriter columnWriter = ArrowWriters.createWriter(children.get(i), childType);
            columnWriter.write(fields.get(i));
        }
    }

}
