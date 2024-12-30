package io.trino.arrow;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ArrowWriter
        implements AutoCloseable
{
    private final BufferAllocator allocator;
    private final VectorSchemaRoot root;
    private final Map<OutputColumn, Field> columnToField;
    private final OutputStream outputStream;

    public ArrowWriter(List<OutputColumn> columns, OutputStream output)
            throws IOException
    {
        this.allocator = new RootAllocator();
        this.outputStream = output;

        // Convert OutputColumns to Arrow Fields
        this.columnToField = columns.stream()
                .map(column -> Map.entry(column, ArrowTypeConverter.toArrowField(column.columnName(), column.type())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));



        // Create Arrow Schema
        Schema schema = new Schema(columnToField.values());

        // Create VectorSchemaRoot
        this.root = VectorSchemaRoot.create(schema, allocator);
    }

    public void write(Page page)
            throws IOException
    {
        //TODO chunk to some max size?
        try(ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(outputStream))) {
            writer.start();
            for(OutputColumn outputColumn : columnToField.keySet()) {
                Field field = columnToField.get(outputColumn);
                FieldVector vector = root.getVector(field);
                vector.allocateNew();
                Block block = page.getBlock(outputColumn.sourcePageChannel());
                ArrowColumnWriter columnWriter = ArrowWriters.createWriter(vector, outputColumn.type());
                columnWriter.write(block);
            }
            root.setRowCount(page.getPositionCount());
            writer.writeBatch();
            writer.end();
        }



    }

    @Override
    public void close()
            throws IOException
    {
        if (outputStream != null) {
            outputStream.close();
        }
        if (root != null) {
            root.close();
        }
        if (allocator != null) {
            allocator.close();
        }
    }
}
