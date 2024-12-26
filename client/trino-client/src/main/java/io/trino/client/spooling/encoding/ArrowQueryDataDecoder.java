/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.client.spooling.encoding;

import com.google.common.collect.ImmutableList;
import io.trino.client.CloseableIterator;
import io.trino.client.Column;
import io.trino.client.QueryDataDecoder;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.encoding.ArrowDecodingUtils.VectorTypeDecoder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static io.trino.client.spooling.encoding.ArrowDecodingUtils.createVectorTypeDecoders;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class ArrowQueryDataDecoder
        implements QueryDataDecoder
{
    private static final String ENCODING = "arrow";

    private static final BufferAllocator ROOT_ALLOCATOR = new RootAllocator();
    private final List<Column> columns;

    public ArrowQueryDataDecoder(List<Column> columns)
    {
        this.columns = ImmutableList.copyOf(columns);
    }

    @Override
    public CloseableIterator<List<Object>> decode(InputStream input, DataAttributes segmentAttributes)
            throws IOException
    {
        BufferAllocator allocator = ROOT_ALLOCATOR.newChildAllocator(randomUUID().toString(), Integer.MAX_VALUE, Integer.MAX_VALUE);
        ArrowStreamReader streamReader = new ArrowStreamReader(input, allocator, new AirliftCompressionCodecFactory());
        return new ArrowRowIterator(allocator, streamReader, columns);
    }

    public static class ArrowRowIterator
            implements CloseableIterator<List<Object>>
    {
        private final BufferAllocator allocator;
        private final ArrowReader reader;
        private final List<Column> columns;

        private VectorTypeDecoder[] currentVectorDecoders;
        private int currentRow;
        private int currentMaxRows;

        public ArrowRowIterator(BufferAllocator allocator, ArrowReader reader, List<Column> columns)
                throws IOException
        {
            this.allocator = requireNonNull(allocator, "allocator is null");
            this.reader = requireNonNull(reader, "reader is null");
            this.columns = requireNonNull(columns, "columns is null");
            this.currentMaxRows = reader.getVectorSchemaRoot().getRowCount();
        }

        private boolean advance()
        {
            clearVectors();
            try {
                if (reader.loadNextBatch()) {
                    currentRow = 0;
                    currentMaxRows = reader.getVectorSchemaRoot().getRowCount();
                    currentVectorDecoders = createVectorTypeDecoders(columns, allocator, reader.getVectorSchemaRoot().getFieldVectors());
                    return true;
                }
                return false;
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public boolean hasNext()
        {
            return currentRow < currentMaxRows || advance();
        }

        @Override
        public List<Object> next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            ArrayList<Object> row = new ArrayList<>();
            for (VectorTypeDecoder vectorDecoder : currentVectorDecoders) {
                row.add(vectorDecoder.decode(currentRow));
            }
            currentRow++;
            return row;
        }

        @Override
        public String toString()
        {
            return "ArrowRowIterator{reader=" + reader + '}';
        }

        @Override
        public void close()
                throws IOException
        {
            clearVectors();
            reader.close();
            allocator.close();
        }

        private void clearVectors()
        {
            if (currentVectorDecoders != null) {
                for (VectorTypeDecoder vectorDecoder : currentVectorDecoders) {
                    try {
                        vectorDecoder.close();
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            }
        }
    }

    public static class Factory
            implements QueryDataDecoder.Factory
    {
        @Override
        public QueryDataDecoder create(List<Column> columns, DataAttributes queryAttributes)
        {
            return new ArrowQueryDataDecoder(columns);
        }

        @Override
        public String encoding()
        {
            return ENCODING;
        }
    }

    private static class ZstdArrowQueryDataDecoder
            extends ArrowQueryDataDecoder
    {
        public ZstdArrowQueryDataDecoder(List<Column> columns)
        {
            super(columns);
        }

        @Override
        public String encoding()
        {
            return super.encoding() + "+zstd";
        }
    }

    // Arrow knows internally how to decode Zstd, so we don't need to do anything special here
    public static class ZstdFactory
            extends Factory
    {
        @Override
        public QueryDataDecoder create(List<Column> columns, DataAttributes queryAttributes)
        {
            return new ZstdArrowQueryDataDecoder(columns);
        }

        @Override
        public String encoding()
        {
            return super.encoding() + "+zstd";
        }
    }

    @Override
    public String encoding()
    {
        return ENCODING;
    }
}
