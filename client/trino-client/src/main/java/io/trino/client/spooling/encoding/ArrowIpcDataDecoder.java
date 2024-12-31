package io.trino.client.spooling.encoding;

import io.trino.client.Column;
import io.trino.client.QueryDataDecoder;
import io.trino.client.ResultRows;
import io.trino.client.spooling.DataAttributes;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.util.Text;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;


public class ArrowIpcDataDecoder implements QueryDataDecoder {
    private static BufferAllocator rootAllocator = new RootAllocator();
    private static String ENCODING = "arrow-ipc";
    private final List<Column> columns;
    public ArrowIpcDataDecoder(List<Column> columns) {
        this.columns = columns;
    }

    @Override
    public ResultRows decode(InputStream input, DataAttributes segmentAttributes) throws IOException {
        ArrowStreamReader streamReader = new ArrowStreamReader(input, rootAllocator);
        streamReader.getVectorSchemaRoot();
        return () -> new ArrowRowIterator(streamReader, columns);
    }


    public static class ArrowRowIterator implements Iterator<List<Object>> {

        private final ArrowReader reader;
        private final List<Column> columns;
        private int currentRow = 0;
        public ArrowRowIterator(ArrowReader reader, List<Column> columns) {
            this.reader = reader;
            this.columns = columns;
        }

        private boolean advance(){
            try {
                if (reader.loadNextBatch()){
                    currentRow = 0;
                    return true;
                }else{
                    return false;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public boolean hasNext() {
            try {
                return currentRow < reader.getVectorSchemaRoot().getRowCount() || advance();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public List<Object> next() {
            if(hasNext()){
                try {
                    List<Object> row = getRow(reader.getVectorSchemaRoot());
                    currentRow++;
                    return row;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            }else{
                throw new NoSuchElementException("No more rows");
            }
        }

        public List<Object> getRow(VectorSchemaRoot vectorSchemaRoot){
            return columns.stream()
                    .map(c -> {
                        FieldVector vector = vectorSchemaRoot.getVector(c.getName());
                        Object value = vector.getObject(currentRow);
                        if(value == null){
                            return null;
                        }

                        switch(vector.getMinorType()){
                            case INT:
                            case TINYINT:
                            case SMALLINT:
                            case BIT:
                            case FLOAT2:
                            case FLOAT4:
                            case FLOAT8:
                            case VARBINARY:
                            case BIGINT: {
                                return value;
                            }
                            default: return value.toString();
                        }
                    })
                    .collect(Collectors.toList());


        }
    }

    public static class Factory
            implements QueryDataDecoder.Factory
    {
        @Override
        public QueryDataDecoder create(List<Column> columns, DataAttributes queryAttributes)
        {
            return new ArrowIpcDataDecoder(columns);
        }

        @Override
        public String encoding()
        {
            return ENCODING;
        }
    }

    @Override
    public String encoding() {
        return ENCODING;
    }
}
